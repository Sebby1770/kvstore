// Package interop drives the real Rust kvnode (its own KTP implementation,
// kvnode/src/ktp) with the Go KTP stack and the Go storage client, over real
// UDP and through a lossy relay. It needs the kvnode binary:
//
//	make interop          (or: KVNODE_BIN=bin/kvnode go test ./interop)
//
// Without KVNODE_BIN the tests skip.
package interop

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	mrand "math/rand/v2"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Sebby1770/kvstore/kvgw/internal/client"
	"github.com/Sebby1770/kvstore/kvgw/internal/kv"
	"github.com/Sebby1770/kvstore/kvgw/netstack/endpoint"
	"github.com/Sebby1770/kvstore/kvgw/netstack/ktp"
)

const clusterKey = "interop-cluster-key"

// kvnode is one running storage-node process.
type kvnode struct {
	t     *testing.T
	cmd   *exec.Cmd
	addr  string // the KTP UDP address
	mu    sync.Mutex
	lines []string
	exit  chan struct{}
}

var ktpLine = regexp.MustCompile(`kvnode: ktp on (\S+) \(identity ([0-9a-f]{64})`)

// startKVNode runs kvnode --ktp on addr ("127.0.0.1:0" for any port).
func startKVNode(t *testing.T, addr, db string, extra ...string) *kvnode {
	t.Helper()
	bin := os.Getenv("KVNODE_BIN")
	if bin == "" {
		t.Skip("KVNODE_BIN not set (run `make interop`)")
	}
	args := append([]string{"--ktp", addr, "--db", db, "--capacity", "4096", "--cluster-key", clusterKey}, extra...)
	cmd := exec.Command(bin, args...)
	stderr, err := cmd.StderrPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	n := &kvnode{t: t, cmd: cmd, exit: make(chan struct{})}
	ready := make(chan string, 1)
	go func() {
		sc := bufio.NewScanner(stderr)
		for sc.Scan() {
			line := sc.Text()
			n.mu.Lock()
			n.lines = append(n.lines, line)
			n.mu.Unlock()
			if m := ktpLine.FindStringSubmatch(line); m != nil {
				ready <- m[1]
			}
		}
		cmd.Wait()
		close(n.exit)
	}()
	select {
	case n.addr = <-ready:
	case <-n.exit:
		t.Fatalf("kvnode exited at startup:\n%s", n.log())
	case <-time.After(10 * time.Second):
		t.Fatalf("kvnode did not start:\n%s", n.log())
	}
	t.Cleanup(n.kill)
	return n
}

func (n *kvnode) log() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return strings.Join(n.lines, "\n")
}

// kill is kill -9: no CONNECTION_CLOSE, no goodbyes.
func (n *kvnode) kill() {
	n.cmd.Process.Kill()
	<-n.exit
}

func (n *kvnode) logged(substr string) bool {
	return strings.Contains(n.log(), substr)
}

type endpointOpts struct {
	key      string
	identity ed25519.PrivateKey
}

func newEndpoint(t *testing.T, o endpointOpts) *endpoint.Endpoint {
	t.Helper()
	id := o.identity
	if id == nil {
		var err error
		if _, id, err = ed25519.GenerateKey(rand.Reader); err != nil {
			t.Fatal(err)
		}
	}
	cfg := &ktp.Config{Identity: id, ClusterKey: []byte(o.key), Congestion: "cubic",
		KeepAlive: 5 * time.Second, IdleTimeout: 30 * time.Second, HandshakeTimeout: 3 * time.Second}
	ep, err := endpoint.Listen("127.0.0.1:0", cfg, endpoint.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ep.Close() })
	return ep
}

// relay forwards UDP between one client and kvnode, dropping each datagram
// with probability loss (seeded, so failures reproduce).
type relay struct {
	pc        net.PacketConn // client side
	up        *net.UDPConn   // kvnode side
	loss      float64
	mu        sync.Mutex
	rng       *mrand.Rand
	client    atomic.Pointer[net.UDPAddr]
	forwarded atomic.Int64
	dropped   atomic.Int64
}

func newRelay(t *testing.T, target string, loss float64, seed uint64) *relay {
	t.Helper()
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ta, err := net.ResolveUDPAddr("udp", target)
	if err != nil {
		t.Fatal(err)
	}
	up, err := net.DialUDP("udp", nil, ta)
	if err != nil {
		t.Fatal(err)
	}
	r := &relay{pc: pc, up: up, loss: loss, rng: mrand.New(mrand.NewPCG(seed, 0x7265))}
	t.Cleanup(func() { pc.Close(); up.Close() })
	go func() { // client -> kvnode
		buf := make([]byte, 65536)
		for {
			n, from, err := pc.ReadFrom(buf)
			if err != nil {
				if errors.Is(err, net.ErrClosed) {
					return
				}
				continue
			}
			r.client.Store(from.(*net.UDPAddr))
			if !r.drop() {
				up.Write(buf[:n])
			}
		}
	}()
	go func() { // kvnode -> client
		buf := make([]byte, 65536)
		for {
			n, err := up.Read(buf)
			if err != nil {
				if errors.Is(err, net.ErrClosed) {
					return
				}
				continue // e.g. ECONNREFUSED while kvnode restarts
			}
			if c := r.client.Load(); c != nil && !r.drop() {
				pc.WriteTo(buf[:n], c)
			}
		}
	}()
	return r
}

func (r *relay) addr() string { return r.pc.LocalAddr().String() }

func (r *relay) drop() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.rng.Float64() < r.loss {
		r.dropped.Add(1)
		return true
	}
	r.forwarded.Add(1)
	return false
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func TestEveryOpAgainstTheRustNode(t *testing.T) {
	node := startKVNode(t, "127.0.0.1:0", filepath.Join(t.TempDir(), "kv.db"))
	c := client.NewKTP("ktp://"+node.addr, newEndpoint(t, endpointOpts{key: clusterKey}), 3*time.Second)
	defer c.Close()

	must(t, c.Ping())
	must(t, c.Put([]byte("a"), []byte("alpha"), 10))
	must(t, c.PutX([]byte("b"), []byte("beta"), 11, uint64(time.Now().Add(time.Hour).Unix())))
	r, err := c.Get([]byte("a"))
	must(t, err)
	if r.State != kv.Present || string(r.Value) != "alpha" || r.Version != 10 {
		t.Fatalf("get: %+v", r)
	}
	if r, err := c.Get([]byte("nope")); err != nil || r.State != kv.Missing {
		t.Fatalf("get missing: %+v %v", r, err)
	}
	// Last-writer-wins lives in the engine: an older version loses.
	must(t, c.Put([]byte("a"), []byte("stale"), 5))
	if r, _ := c.Get([]byte("a")); string(r.Value) != "alpha" {
		t.Fatalf("an older write replaced a newer one: %+v", r)
	}
	if ok, err := c.Del([]byte("a"), 12); err != nil || !ok {
		t.Fatalf("del: %v %v", ok, err)
	}
	if r, err := c.Get([]byte("a")); err != nil || r.State != kv.Deleted {
		t.Fatalf("get after del: %+v %v", r, err)
	}
	live, err := c.Scan()
	must(t, err)
	all, err := c.ScanAll()
	must(t, err)
	if len(live) != 1 || string(live[0].Key) != "b" || len(all) != 2 {
		t.Fatalf("scan %d rows, scan all %d rows", len(live), len(all))
	}
	st, err := c.Stat()
	must(t, err)
	if st.Capacity != 4096 || st.MaxVersion != 12 {
		t.Fatalf("stat %+v", st)
	}
	must(t, c.Compact())

	// The protocol's limits hold over KTP too: a 64-byte key and a 512-byte
	// value fit; one byte more is refused (and the stream with it, so the
	// next call gets a fresh one).
	key, val := bytes.Repeat([]byte("k"), 64), bytes.Repeat([]byte("v"), 512)
	must(t, c.Put(key, val, 20))
	if r, err := c.Get(key); err != nil || !bytes.Equal(r.Value, val) {
		t.Fatalf("max-size row: %v", err)
	}
	if err := c.Put(key, append(val, 'x'), 21); err == nil {
		t.Fatal("an oversized value was accepted")
	}
	must(t, c.Ping())

	if !node.logged("connected (peer") {
		t.Fatalf("kvnode did not log the authenticated connection:\n%s", node.log())
	}
}

// 10% loss in each direction: every write lands, every read is right, and a
// scan of ~600 KB (hundreds of packets, many retransmitted) arrives intact.
func TestLossyPathWithTheRustNode(t *testing.T) {
	node := startKVNode(t, "127.0.0.1:0", filepath.Join(t.TempDir(), "kv.db"))
	rl := newRelay(t, node.addr, 0.10, 42)
	ep := newEndpoint(t, endpointOpts{key: clusterKey})
	c := client.NewKTP("ktp://"+rl.addr(), ep, 10*time.Second)
	defer c.Close()

	val := func(i int) []byte { return bytes.Repeat([]byte{byte('a' + i%26)}, 400) }
	const rows = 1500
	start := time.Now()
	for i := 0; i < rows; i++ {
		must(t, c.Put([]byte(fmt.Sprintf("key-%05d", i)), val(i), uint64(i+1)))
	}
	for i := 0; i < rows; i += 37 {
		r, err := c.Get([]byte(fmt.Sprintf("key-%05d", i)))
		if err != nil || !bytes.Equal(r.Value, val(i)) {
			t.Fatalf("get %d: %v", i, err)
		}
	}
	writes := time.Since(start)
	start = time.Now()
	all, err := c.ScanAll()
	must(t, err)
	if len(all) != rows {
		t.Fatalf("scan returned %d rows, want %d", len(all), rows)
	}
	for _, e := range all {
		var i int
		fmt.Sscanf(string(e.Key), "key-%05d", &i)
		if !bytes.Equal(e.Value, val(i)) || e.Version != uint64(i+1) {
			t.Fatalf("row %s corrupted", e.Key)
		}
	}
	st := ep.Conns()[0].Stats
	t.Logf("%d writes + reads in %v, a %d KB scan in %v; relay dropped %d of %d datagrams; client: %d lost, %d PTOs, srtt %v",
		rows, writes.Round(time.Millisecond), len(all)*420/1024, time.Since(start).Round(time.Millisecond),
		rl.dropped.Load(), rl.dropped.Load()+rl.forwarded.Load(), st.PacketsLost, st.PTOCount, st.SRTT)
	if rl.dropped.Load() == 0 {
		t.Fatal("the relay dropped nothing; the test proved nothing")
	}
}

func TestWrongClusterKeyIsRefusedFast(t *testing.T) {
	node := startKVNode(t, "127.0.0.1:0", filepath.Join(t.TempDir(), "kv.db"))
	c := client.NewKTP("ktp://"+node.addr, newEndpoint(t, endpointOpts{key: "not-the-key"}), 3*time.Second)
	start := time.Now()
	err := c.Ping()
	if !errors.Is(err, ktp.ErrClusterKeyMismatch) {
		t.Fatalf("ping with the wrong key: %v", err)
	}
	if took := time.Since(start); took > time.Second {
		t.Fatalf("rejection took %v", took)
	}
}

// kill -9 the storage node and start it again on the same port and data:
// the next call heals through the new process's stateless reset, and the
// data is still there.
func TestNodeRestartHealsThroughStatelessReset(t *testing.T) {
	db := filepath.Join(t.TempDir(), "kv.db")
	node := startKVNode(t, "127.0.0.1:0", db)
	c := client.NewKTP("ktp://"+node.addr, newEndpoint(t, endpointOpts{key: clusterKey}), 3*time.Second)
	defer c.Close()
	must(t, c.Put([]byte("survivor"), []byte("still here"), 7))
	node.kill()
	restarted := startKVNode(t, node.addr, db)
	start := time.Now()
	r, err := c.Get([]byte("survivor"))
	if err != nil || string(r.Value) != "still here" {
		t.Fatalf("get after restart: %+v %v\n%s", r, err, restarted.log())
	}
	if took := time.Since(start); took > time.Second {
		t.Fatalf("recovery took %v", took)
	}
}

func TestManyClientsOnOneConnection(t *testing.T) {
	node := startKVNode(t, "127.0.0.1:0", filepath.Join(t.TempDir(), "kv.db"))
	ep := newEndpoint(t, endpointOpts{key: clusterKey})
	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := client.NewKTP("ktp://"+node.addr, ep, 5*time.Second)
			defer c.Close()
			for i := 0; i < 100; i++ {
				k := []byte(fmt.Sprintf("g%02d-%03d", g, i))
				if err := c.Put(k, k, uint64(i+1)); err != nil {
					errs <- err
					return
				}
				if r, err := c.Get(k); err != nil || !bytes.Equal(r.Value, k) {
					errs <- fmt.Errorf("get %s: %+v %v", k, r, err)
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
		t.Fatalf("%d connections, want 1", n)
	}
	c := client.NewKTP("ktp://"+node.addr, ep, 3*time.Second)
	defer c.Close()
	st, err := c.Stat()
	if err != nil || st.Count != 1600 {
		t.Fatalf("stat after 16x100 writes: %+v %v", st, err)
	}
}

// kvnode --allow-peer pins coordinator identities on top of the cluster key.
func TestAllowListPinsCoordinators(t *testing.T) {
	_, allowed, err := ed25519.GenerateKey(rand.Reader)
	must(t, err)
	pub := hex.EncodeToString(allowed.Public().(ed25519.PublicKey))
	node := startKVNode(t, "127.0.0.1:0", filepath.Join(t.TempDir(), "kv.db"), "--allow-peer", pub)

	ok := client.NewKTP("ktp://"+node.addr, newEndpoint(t, endpointOpts{key: clusterKey, identity: allowed}), 3*time.Second)
	defer ok.Close()
	must(t, ok.Ping())

	stranger := client.NewKTP("ktp://"+node.addr, newEndpoint(t, endpointOpts{key: clusterKey}), 3*time.Second)
	defer stranger.Close()
	err = stranger.Ping()
	var ce *ktp.CloseError
	if !errors.As(err, &ce) || ce.Code != ktp.CodePeerRejected {
		t.Fatalf("unlisted coordinator: %v", err)
	}
}

// The identity file is created on first start and reused afterwards, so a
// node's fingerprint is stable across restarts.
func TestIdentityPersists(t *testing.T) {
	dir := t.TempDir()
	idFile := filepath.Join(dir, "kvnode.key")
	first := startKVNode(t, "127.0.0.1:0", filepath.Join(dir, "kv.db"), "--identity", idFile)
	id1 := ktpLine.FindStringSubmatch(first.log())[2]
	first.kill()
	info, err := os.Stat(idFile)
	must(t, err)
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("identity file mode %v, want 0600", info.Mode().Perm())
	}
	second := startKVNode(t, "127.0.0.1:0", filepath.Join(dir, "kv.db"), "--identity", idFile)
	if id2 := ktpLine.FindStringSubmatch(second.log())[2]; id1 != id2 {
		t.Fatalf("identity changed across restarts: %s then %s", id1, id2)
	}
	// And the coordinator sees exactly that key.
	ep := newEndpoint(t, endpointOpts{key: clusterKey})
	conn, err := ep.Conn(context.Background(), second.addr)
	must(t, err)
	if got := hex.EncodeToString(conn.PeerIdentity()); got != id1 {
		t.Fatalf("handshake proved identity %s, kvnode announced %s", got, id1)
	}
}
