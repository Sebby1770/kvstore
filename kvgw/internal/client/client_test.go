package client

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"io"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Sebby1770/kvstore/kvgw/internal/kv"
	"github.com/Sebby1770/kvstore/kvgw/internal/proto"
	"github.com/Sebby1770/kvstore/kvgw/netstack/endpoint"
	"github.com/Sebby1770/kvstore/kvgw/netstack/ktp"
)

// fakeNode serves the kvnode request protocol from memory, over any stream:
// a TCP connection or a KTP stream. (The real Rust kvnode is exercised by
// the interop suite in kvgw/interop.)
type fakeNode struct {
	mu      sync.Mutex
	rows    map[string]kv.Entry
	delay   atomic.Int64 // nanoseconds to wait before each reply
	streams atomic.Int64 // streams (or connections) served
}

func newFakeNode() *fakeNode { return &fakeNode{rows: map[string]kv.Entry{}} }

func (f *fakeNode) serve(rw io.ReadWriter) {
	f.streams.Add(1)
	r, w := bufio.NewReader(rw), bufio.NewWriter(rw)
	key := func() string {
		n, _ := proto.ReadU32(r)
		b, _ := proto.ReadBytes(r, n)
		return string(b)
	}
	for {
		op, err := proto.ReadU8(r)
		if err != nil {
			return
		}
		var reply bytes.Buffer
		f.mu.Lock()
		switch op {
		case proto.OpPing, proto.OpCompact:
			proto.WriteU8(&reply, proto.StatusOK)
		case proto.OpStat:
			proto.WriteU8(&reply, proto.StatusOK)
			proto.WriteU32(&reply, uint32(len(f.rows)))
			proto.WriteU32(&reply, 1024)
			proto.WriteU64(&reply, 0)
			proto.WriteU32(&reply, 0)
		case proto.OpGet:
			e, ok := f.rows[key()]
			switch {
			case !ok:
				proto.WriteU8(&reply, proto.StatusNotFound)
			case e.Tombstone:
				proto.WriteU8(&reply, proto.StatusDeleted)
				proto.WriteU64(&reply, e.Version)
				proto.WriteU64(&reply, e.ExpiresAt)
			default:
				proto.WriteU8(&reply, proto.StatusOK)
				proto.WriteU64(&reply, e.Version)
				proto.WriteU64(&reply, e.ExpiresAt)
				proto.WriteU32(&reply, uint32(len(e.Value)))
				proto.WriteBytes(&reply, e.Value)
			}
		case proto.OpPut, proto.OpPutX:
			e := kv.Entry{Key: []byte(key())}
			e.Version, _ = proto.ReadU64(r)
			if op == proto.OpPutX {
				e.ExpiresAt, _ = proto.ReadU64(r)
			}
			n, _ := proto.ReadU32(r)
			e.Value, _ = proto.ReadBytes(r, n)
			f.rows[string(e.Key)] = e
			proto.WriteU8(&reply, proto.StatusOK)
		case proto.OpDel:
			k := key()
			v, _ := proto.ReadU64(r)
			if _, ok := f.rows[k]; !ok {
				proto.WriteU8(&reply, proto.StatusNotFound)
				break
			}
			f.rows[k] = kv.Entry{Key: []byte(k), Version: v, Tombstone: true}
			proto.WriteU8(&reply, proto.StatusOK)
		case proto.OpScan, proto.OpScanAll:
			proto.WriteU8(&reply, proto.StatusOK)
			keys := make([]string, 0, len(f.rows))
			for k := range f.rows {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for _, k := range keys {
				e := f.rows[k]
				if e.Tombstone && op == proto.OpScan {
					continue
				}
				proto.WriteU32(&reply, uint32(len(k)))
				proto.WriteBytes(&reply, []byte(k))
				proto.WriteU64(&reply, e.Version)
				proto.WriteU64(&reply, e.ExpiresAt)
				if op == proto.OpScanAll {
					tomb := byte(0)
					if e.Tombstone {
						tomb = 1
					}
					proto.WriteU8(&reply, tomb)
				}
				proto.WriteU32(&reply, uint32(len(e.Value)))
				proto.WriteBytes(&reply, e.Value)
			}
			proto.WriteU32(&reply, proto.ScanTerminator)
		default:
			proto.WriteU8(&reply, proto.StatusError)
		}
		f.mu.Unlock()
		if d := f.delay.Load(); d > 0 {
			time.Sleep(time.Duration(d))
		}
		w.Write(reply.Bytes())
		if w.Flush() != nil {
			return
		}
	}
}

func serveTCP(t *testing.T, f *fakeNode) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				f.serve(c)
			}()
		}
	}()
	return ln.Addr().String()
}

func ktpConfig(t *testing.T, key string) *ktp.Config {
	t.Helper()
	_, id, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return &ktp.Config{Identity: id, ClusterKey: []byte(key), HandshakeTimeout: 2 * time.Second, IdleTimeout: 10 * time.Second}
}

// serveKTP runs f on every stream opened to a Go KTP endpoint on pc.
func serveKTP(t *testing.T, pc net.PacketConn, key string, f *fakeNode) *endpoint.Endpoint {
	t.Helper()
	ep, err := endpoint.New(pc, ktpConfig(t, key), endpoint.Options{})
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			c, err := ep.Accept(context.Background())
			if err != nil {
				return
			}
			go func() {
				for {
					s, err := c.AcceptStream(context.Background())
					if err != nil {
						return
					}
					go func() {
						f.serve(s)
						s.Close()
					}()
				}
			}()
		}
	}()
	return ep
}

func listenUDP(t *testing.T, addr string) net.PacketConn {
	t.Helper()
	pc, err := net.ListenPacket("udp", addr)
	if err != nil {
		t.Fatal(err)
	}
	return pc
}

func clientEndpoint(t *testing.T, key string) *endpoint.Endpoint {
	t.Helper()
	ep, err := endpoint.Listen("127.0.0.1:0", ktpConfig(t, key), endpoint.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ep.Close() })
	return ep
}

func exercise(t *testing.T, c *Client) {
	t.Helper()
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(c.Ping())
	must(c.Put([]byte("a"), []byte("alpha"), 10))
	must(c.PutX([]byte("b"), []byte("beta"), 11, 1_900_000_000))
	r, err := c.Get([]byte("a"))
	must(err)
	if r.State != kv.Present || string(r.Value) != "alpha" || r.Version != 10 {
		t.Fatalf("get a: %+v", r)
	}
	if r, err = c.Get([]byte("missing")); err != nil || r.State != kv.Missing {
		t.Fatalf("get missing: %+v %v", r, err)
	}
	if ok, err := c.Del([]byte("a"), 12); err != nil || !ok {
		t.Fatalf("del: %v %v", ok, err)
	}
	if r, err = c.Get([]byte("a")); err != nil || r.State != kv.Deleted || r.Version != 12 {
		t.Fatalf("get deleted: %+v %v", r, err)
	}
	live, err := c.Scan()
	must(err)
	all, err := c.ScanAll()
	must(err)
	if len(live) != 1 || string(live[0].Key) != "b" || live[0].ExpiresAt != 1_900_000_000 || len(all) != 2 || !all[0].Tombstone {
		t.Fatalf("scan %+v, scan all %+v", live, all)
	}
	st, err := c.Stat()
	must(err)
	if st.Count != 2 || st.Capacity != 1024 {
		t.Fatalf("stat %+v", st)
	}
	must(c.Compact())
}

func TestEveryOpOverTCPAndKTP(t *testing.T) {
	t.Run("tcp", func(t *testing.T) {
		c, err := Open(serveTCP(t, newFakeNode()), nil, 2*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		exercise(t, c)
	})
	t.Run("ktp", func(t *testing.T) {
		f := newFakeNode()
		srv := serveKTP(t, listenUDP(t, "127.0.0.1:0"), "k", f)
		defer srv.Close()
		addr := "ktp://" + srv.LocalAddr().String()
		c, err := Open(addr, clientEndpoint(t, "k"), 2*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		if c.Addr() != addr {
			t.Fatalf("Addr() = %q, want the configured %q", c.Addr(), addr)
		}
		exercise(t, c)
		if n := f.streams.Load(); n != 1 {
			t.Fatalf("%d streams for one client's sequential calls, want 1", n)
		}
	})
}

func TestKTPClientsShareOneConnection(t *testing.T) {
	f := newFakeNode()
	srv := serveKTP(t, listenUDP(t, "127.0.0.1:0"), "k", f)
	defer srv.Close()
	ep := clientEndpoint(t, "k")
	addr := "ktp://" + srv.LocalAddr().String()
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := NewKTP(addr, ep, 2*time.Second)
			defer c.Close()
			for j := 0; j < 50; j++ {
				k := []byte{byte(i), byte(j)}
				if err := c.Put(k, k, uint64(j+1)); err != nil {
					errs <- err
					return
				}
				if r, err := c.Get(k); err != nil || !bytes.Equal(r.Value, k) {
					errs <- errors.Join(err, errors.New("wrong value"))
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if n := len(ep.Conns()); n != 1 {
		t.Fatalf("%d connections to one node, want 1", n)
	}
	if n := f.streams.Load(); n != 8 {
		t.Fatalf("%d streams for 8 clients, want 8", n)
	}
}

// A storage node that crashes and comes back on the same port: the client's
// next call reaches the new process (whose stateless reset kills the stale
// connection) without the caller seeing an error.
func TestKTPNodeRestart(t *testing.T) {
	pc := listenUDP(t, "127.0.0.1:0")
	addr := pc.LocalAddr().String()
	old := serveKTP(t, pc, "k", newFakeNode())
	c := NewKTP("ktp://"+addr, clientEndpoint(t, "k"), 2*time.Second)
	defer c.Close()
	if err := c.Ping(); err != nil {
		t.Fatal(err)
	}
	pc.Close() // crash: the process vanishes without closing anything
	defer old.Close()

	fresh := newFakeNode()
	srv := serveKTP(t, listenUDP(t, addr), "k", fresh)
	defer srv.Close()
	start := time.Now()
	if err := c.Put([]byte("k"), []byte("v"), 1); err != nil {
		t.Fatalf("call after restart: %v", err)
	}
	if took := time.Since(start); took > time.Second {
		t.Fatalf("recovery took %v; the stateless reset should make it immediate", took)
	}
	fresh.mu.Lock()
	defer fresh.mu.Unlock()
	if fresh.rows["k"].Version != 1 {
		t.Fatal("the write did not reach the restarted node")
	}
}

func TestKTPWrongClusterKeyFailsFast(t *testing.T) {
	srv := serveKTP(t, listenUDP(t, "127.0.0.1:0"), "right", newFakeNode())
	defer srv.Close()
	c := NewKTP("ktp://"+srv.LocalAddr().String(), clientEndpoint(t, "wrong"), 3*time.Second)
	start := time.Now()
	err := c.Ping()
	if !errors.Is(err, ktp.ErrClusterKeyMismatch) {
		t.Fatalf("ping with the wrong cluster key: %v", err)
	}
	if took := time.Since(start); took > time.Second {
		t.Fatalf("took %v to fail; the handshake's confirm tag should reject at once", took)
	}
}

// A timeout abandons the stream: the late reply must never be read as the
// answer to the next request, and the timeout is not retried (which would
// double it).
func TestKTPTimeoutDiscardsTheStream(t *testing.T) {
	f := newFakeNode()
	srv := serveKTP(t, listenUDP(t, "127.0.0.1:0"), "k", f)
	defer srv.Close()
	c := NewKTP("ktp://"+srv.LocalAddr().String(), clientEndpoint(t, "k"), 300*time.Millisecond)
	defer c.Close()
	if err := c.Put([]byte("x"), []byte("1"), 1); err != nil {
		t.Fatal(err)
	}
	f.delay.Store(int64(500 * time.Millisecond))
	start := time.Now()
	err := c.Ping()
	var ne net.Error
	if !errors.As(err, &ne) || !ne.Timeout() {
		t.Fatalf("slow node: %v", err)
	}
	if took := time.Since(start); took > 450*time.Millisecond {
		t.Fatalf("timed out after %v: the timeout was retried", took)
	}
	f.delay.Store(0)
	time.Sleep(300 * time.Millisecond) // the late PING reply is now in flight
	r, err := c.Get([]byte("x"))
	if err != nil || string(r.Value) != "1" {
		t.Fatalf("get after a timeout: %+v %v (a stale reply leaked into the next request?)", r, err)
	}
}

func TestOpenChecksAddresses(t *testing.T) {
	for _, tc := range []struct {
		addr string
		ok   bool
	}{
		{"127.0.0.1:7070", true},
		{"tcp://127.0.0.1:7070", true},
		{"ktp://127.0.0.1:7170", false}, // no endpoint
		{"http://127.0.0.1:7070", false},
	} {
		c, err := Open(tc.addr, nil, time.Second)
		if c == nil || c.Addr() != tc.addr {
			t.Fatalf("%s: client %v", tc.addr, c)
		}
		if (err == nil) != tc.ok {
			t.Fatalf("%s: err %v", tc.addr, err)
		}
		if !tc.ok && !errors.Is(c.Ping(), err) {
			t.Fatalf("%s: calls on an unusable address must fail with the Open error", tc.addr)
		}
	}
}
