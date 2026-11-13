package fabric

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"math/rand/v2"
	"path/filepath"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Sebby1770/kvstore/kvgw/netstack/endpoint"
	"github.com/Sebby1770/kvstore/kvgw/netstack/ktp"
	"github.com/Sebby1770/kvstore/kvgw/netstack/simnet"
)

func TestRegionLocalOps(t *testing.T) {
	r := NewRegion(100) // rounds to 104
	if r.Size() != 104 {
		t.Fatalf("size %d", r.Size())
	}
	if err := r.WriteAt([]byte("hello"), 100); !errors.Is(err, ErrBounds) {
		t.Fatalf("write past end: %v", err)
	}
	if _, err := r.Load64(4); !errors.Is(err, ErrAlign) {
		t.Fatalf("misaligned load: %v", err)
	}
	if _, err := r.Load64(104); !errors.Is(err, ErrBounds) {
		t.Fatalf("load past end: %v", err)
	}
	r.Store64(8, 5)
	if prev, ok, _ := r.CompareAndSwap64(8, 4, 9); ok || prev != 5 {
		t.Fatalf("CAS with wrong old: %d %v", prev, ok)
	}
	if prev, ok, _ := r.CompareAndSwap64(8, 5, 9); !ok || prev != 5 {
		t.Fatalf("CAS: %d %v", prev, ok)
	}
	if prev, _ := r.FetchAdd64(8, 3); prev != 9 {
		t.Fatalf("fetch-add prev %d", prev)
	}
	if v, _ := r.Load64(8); v != 12 {
		t.Fatalf("value %d", v)
	}
	// In-place view: Bytes aliases the region.
	b, _ := r.Bytes(16, 4)
	copy(b, "abcd")
	got := make([]byte, 4)
	r.ReadAt(got, 16)
	if string(got) != "abcd" {
		t.Fatalf("Bytes is not a view: %q", got)
	}
}

// A linked list laid out in a region: nodes at 8, 32, 56 linked by their
// first word, keys in their second.
func listRegion() *Region {
	r := NewRegion(128)
	nodes := []uint64{8, 32, 56}
	for i, off := range nodes {
		next := uint64(0)
		if i+1 < len(nodes) {
			next = nodes[i+1]
		}
		r.Store64(off, next)
		r.Store64(off+8, uint64(100+i))
	}
	return r
}

func TestChase(t *testing.T) {
	r := listRegion()
	res, err := r.Chase(8, ChaseSpec{LinkOff: 0, KeyOff: 8, Key: 102, Match: true, MaxHops: 10, ReadLen: 16})
	if err != nil || !res.Found || res.Off != 56 || res.Hops != 2 {
		t.Fatalf("chase to key 102: %+v %v", res, err)
	}
	if k := le.Uint64(res.Data[8:]); k != 102 {
		t.Fatalf("returned node key %d", k)
	}
	if res, _ := r.Chase(8, ChaseSpec{KeyOff: 8, Key: 7, Match: true, MaxHops: 10}); res.Found || res.Off != 0 {
		t.Fatalf("missing key found: %+v", res)
	}
	if res, _ := r.Chase(8, ChaseSpec{MaxHops: 1}); res.Off != 32 || res.Hops != 1 {
		t.Fatalf("bounded walk: %+v", res)
	}
	// A cycle cannot run forever: MaxHops bounds it.
	r.Store64(56, 8)
	if res, _ := r.Chase(8, ChaseSpec{KeyOff: 8, Key: 999, Match: true, MaxHops: 50}); res.Hops != 50 {
		t.Fatalf("cycle walk stopped after %d hops", res.Hops)
	}
}

// Two mappings of one file are one memory: stores through either are seen
// through the other, and atomics from both stay exact -- the shared-fabric
// case, with no messages at all.
func TestMapFileShared(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fabric.mem")
	a, err := MapFile(path, 1<<16)
	if err != nil {
		t.Skipf("no shared mappings here: %v", err)
	}
	defer a.Close()
	b, err := MapFile(path, 1<<16)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	a.WriteAt([]byte("over the fabric"), 64)
	got := make([]byte, 15)
	b.ReadAt(got, 64)
	if string(got) != "over the fabric" {
		t.Fatalf("second mapping saw %q", got)
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		r := a
		if i%2 == 1 {
			r = b
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10000; j++ {
				r.FetchAdd64(0, 1)
			}
		}()
	}
	wg.Wait()
	if v, _ := b.Load64(0); v != 80000 {
		t.Fatalf("counter %d, want 80000", v)
	}
}

// ---- remote verbs over simnet ----

func key(seed byte) ed25519.PrivateKey {
	s := make([]byte, ed25519.SeedSize)
	s[0] = seed
	return ed25519.NewKeyFromSeed(s)
}

type cluster struct {
	net   *simnet.Network
	eps   []*endpoint.Endpoint
	nodes []*Node
}

func newCluster(t *testing.T, n int, link simnet.Link) *cluster {
	t.Helper()
	c := &cluster{net: simnet.New(int64(n) * 7)}
	c.net.SetDefaultLink(link)
	for i := 0; i < n; i++ {
		addr := fmt.Sprintf("n%d", i)
		pc, err := c.net.Listen(addr)
		if err != nil {
			t.Fatal(err)
		}
		ep, err := endpoint.New(pc, &ktp.Config{Identity: key(byte(i + 1)), ClusterKey: []byte("fabric"),
			HandshakeTimeout: time.Second}, endpoint.Options{Resolve: simnet.Resolve, AcceptDialed: true})
		if err != nil {
			t.Fatal(err)
		}
		c.eps = append(c.eps, ep)
		c.nodes = append(c.nodes, New(ep, Options{Advertise: addr}))
	}
	return c
}

func (c *cluster) close() {
	for _, ep := range c.eps {
		ep.Close()
	}
}

func TestRemoteVerbs(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newCluster(t, 2, simnet.Link{Latency: 2 * time.Millisecond})
		defer c.close()
		owner, user := c.nodes[0], c.nodes[1]
		reg := listRegion()
		rr := owner.Register(reg, PermAll)
		ctx := context.Background()

		if err := user.Write(ctx, rr, 80, []byte("remote write")); err != nil {
			t.Fatal(err)
		}
		got, err := user.Read(ctx, rr, 80, 12)
		if err != nil || string(got) != "remote write" {
			t.Fatalf("read back %q %v", got, err)
		}
		if prev, ok, err := user.CAS(ctx, rr, 16, 100, 500); err != nil || !ok || prev != 100 {
			t.Fatalf("CAS: %d %v %v", prev, ok, err)
		}
		if prev, err := user.FetchAdd(ctx, rr, 16, 5); err != nil || prev != 500 {
			t.Fatalf("fetch-add: %d %v", prev, err)
		}
		if v, _ := reg.Load64(16); v != 505 {
			t.Fatalf("owner memory holds %d", v)
		}
		res, err := user.Chase(ctx, rr, 8, ChaseSpec{KeyOff: 8, Key: 102, Match: true, MaxHops: 8, ReadLen: 16})
		if err != nil || !res.Found || res.Off != 56 || res.Hops != 2 {
			t.Fatalf("remote chase: %+v %v", res, err)
		}
		// Capabilities: a wrong key, a revoked region, a missing permission,
		// and out-of-bounds access are all refused by the owner.
		bad := rr
		bad.Key ^= 0xff
		if _, err := user.Read(ctx, bad, 0, 8); !errors.Is(err, ErrKey) {
			t.Fatalf("wrong rkey: %v", err)
		}
		ro := owner.Register(NewRegion(64), PermRead)
		if err := user.Write(ctx, ro, 0, []byte("x")); !errors.Is(err, ErrPerm) {
			t.Fatalf("write to read-only region: %v", err)
		}
		if _, err := user.Read(ctx, rr, 120, 16); !errors.Is(err, ErrBounds) {
			t.Fatalf("read past end: %v", err)
		}
		owner.Deregister(rr.Region)
		if _, err := user.Read(ctx, rr, 0, 8); !errors.Is(err, ErrKey) {
			t.Fatalf("revoked region: %v", err)
		}
		if st := owner.Stats(); st.Served == 0 || st.Denied < 4 {
			t.Fatalf("owner stats %+v", st)
		}
	})
}

func TestLargeTransfersSplit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newCluster(t, 2, simnet.Link{Latency: time.Millisecond, Loss: 0.05})
		defer c.close()
		rr := c.nodes[0].Register(NewRegion(64<<10), PermAll)
		data := make([]byte, 20000)
		rng := rand.New(rand.NewPCG(1, 2))
		for i := range data {
			data[i] = byte(rng.Uint32())
		}
		ctx := context.Background()
		if err := c.nodes[1].Write(ctx, rr, 1000, data); err != nil {
			t.Fatal(err)
		}
		got, err := c.nodes[1].Read(ctx, rr, 1000, len(data))
		if err != nil || !bytes.Equal(got, data) {
			t.Fatalf("20KB round trip over a lossy link failed: %v", err)
		}
	})
}

// FETCH_ADD from many requesters over a lossy, duplicating link is exact:
// lost requests are retried, lost replies are answered from the owner's
// response cache rather than re-executed.
func TestFetchAddExactlyOnceUnderLoss(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newCluster(t, 5, simnet.Link{Latency: 3 * time.Millisecond, Jitter: 2 * time.Millisecond, Loss: 0.15, Duplicate: 0.1})
		defer c.close()
		reg := NewRegion(64)
		rr := c.nodes[0].Register(reg, PermAll)
		var wg sync.WaitGroup
		const per = 50
		for i := 1; i < 5; i++ {
			wg.Add(1)
			go func(n *Node) {
				defer wg.Done()
				for j := 0; j < per; j++ {
					if _, err := n.FetchAdd(context.Background(), rr, 0, 1); err != nil {
						t.Errorf("fetch-add: %v", err)
						return
					}
				}
			}(c.nodes[i])
		}
		wg.Wait()
		if v, _ := reg.Load64(0); v != 4*per {
			t.Fatalf("counter = %d, want %d", v, 4*per)
		}
		var retries uint64
		for _, n := range c.nodes[1:] {
			retries += n.Stats().Retries
		}
		if retries == 0 || c.nodes[0].Stats().DedupHits == 0 {
			t.Fatalf("loss never exercised retries/dedup (retries %d, owner %+v)", retries, c.nodes[0].Stats())
		}
	})
}

// The index is used concurrently by several nodes over a lossy network; the
// owner registers the region and runs nothing else.
func TestIndexConcurrentRemoteUse(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newCluster(t, 4, simnet.Link{Latency: 2 * time.Millisecond, Loss: 0.05})
		defer c.close()
		reg := NewRegion(256 << 10)
		if err := FormatIndex(reg, 16); err != nil { // few buckets: long chains, real CAS races
			t.Fatal(err)
		}
		rr := c.nodes[0].Register(reg, PermAll)
		ctx := context.Background()
		var wg sync.WaitGroup
		for w := 1; w < 4; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				x, err := OpenIndex(ctx, c.nodes[w], rr)
				if err != nil {
					t.Error(err)
					return
				}
				for i := 0; i < 60; i++ {
					k := fmt.Sprintf("w%d-k%d", w, i)
					if err := x.Put(ctx, []byte(k), []byte("v1-"+k)); err != nil {
						t.Errorf("put %s: %v", k, err)
						return
					}
					if i%3 == 0 { // overwrite: the newer version must win
						x.Put(ctx, []byte(k), []byte("v2-"+k))
					}
				}
			}(w)
		}
		wg.Wait()
		x, err := OpenIndex(ctx, c.nodes[2], rr)
		if err != nil {
			t.Fatal(err)
		}
		for w := 1; w < 4; w++ {
			for i := 0; i < 60; i++ {
				k := fmt.Sprintf("w%d-k%d", w, i)
				want := "v1-" + k
				if i%3 == 0 {
					want = "v2-" + k
				}
				got, err := x.Get(ctx, []byte(k))
				if err != nil || string(got) != want {
					t.Fatalf("get %s = %q, %v; want %q", k, got, err, want)
				}
			}
		}
		if err := x.Delete(ctx, []byte("w1-k5")); err != nil {
			t.Fatal(err)
		}
		if _, err := x.Get(ctx, []byte("w1-k5")); !errors.Is(err, ErrNotFound) {
			t.Fatalf("deleted key: %v", err)
		}
		if _, err := x.Get(ctx, []byte("never")); !errors.Is(err, ErrNotFound) {
			t.Fatalf("missing key: %v", err)
		}
		// The owner's own view of the same memory agrees, with no verbs at
		// all: a node with no endpoint reads the region directly.
		self := New(nil, Options{})
		lx, err := OpenIndex(ctx, self, self.Register(reg, PermRead))
		if err != nil {
			t.Fatal(err)
		}
		if got, err := lx.Get(ctx, []byte("w3-k9")); err != nil || string(got) != "v2-w3-k9" {
			t.Fatalf("local view: %q %v", got, err)
		}
	})
}

func TestIndexFull(t *testing.T) {
	n := New(nil, Options{})
	reg := NewRegion(512)
	FormatIndex(reg, 4)
	rr := n.Register(reg, PermAll)
	x, err := OpenIndex(context.Background(), n, rr)
	if err != nil {
		t.Fatal(err)
	}
	var full bool
	for i := 0; i < 100 && !full; i++ {
		err := x.Put(context.Background(), []byte(fmt.Sprint(i)), bytes.Repeat([]byte("v"), 40))
		full = errors.Is(err, ErrIndexFull)
		if err != nil && !full {
			t.Fatal(err)
		}
	}
	if !full {
		t.Fatal("index never reported full")
	}
	if got, err := x.Get(context.Background(), []byte("0")); err != nil || len(got) != 40 {
		t.Fatalf("entries before the full one are intact: %q %v", got, err)
	}
}

// Hostile datagrams never panic the owner's packet path.
func FuzzServe(f *testing.F) {
	f.Add(make([]byte, reqHeader))
	q := newRequest(OpChase, Remote{Region: 1, Key: 3}, 8, 0)
	le.PutUint32(q[24:], 5)
	f.Add([]byte(q))
	f.Fuzz(func(t *testing.T, b []byte) {
		n := New(nil, Options{})
		reg := listRegion()
		n.regions[1] = &regEntry{r: reg, key: 3, perm: PermAll}
		if len(b) > 1 {
			b[0] = TagRequest
		}
		if len(b) >= 8 {
			le.PutUint16(b[2:], 1)
			le.PutUint32(b[4:], 3)
		}
		n.serve(nil, b)
		n.complete(nil, b)
	})
}
