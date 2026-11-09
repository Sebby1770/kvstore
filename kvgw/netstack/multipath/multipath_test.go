package multipath

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"io"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Sebby1770/kvstore/kvgw/netstack/endpoint"
	"github.com/Sebby1770/kvstore/kvgw/netstack/ktp"
	"github.com/Sebby1770/kvstore/kvgw/netstack/segroute"
	"github.com/Sebby1770/kvstore/kvgw/netstack/simnet"
)

func key(seed byte) ed25519.PrivateKey {
	s := make([]byte, ed25519.SeedSize)
	s[0] = seed
	return ed25519.NewKeyFromSeed(s)
}

// Three access networks between a phone and a server.
var (
	fiber     = simnet.Link{Latency: 5 * time.Millisecond, Bandwidth: 1536 << 10, QueueBytes: 128 << 10}
	cellular  = simnet.Link{Latency: 25 * time.Millisecond, Jitter: 5 * time.Millisecond, Loss: 0.005, Bandwidth: 1 << 20, QueueBytes: 128 << 10}
	satellite = simnet.Link{Latency: 120 * time.Millisecond, Bandwidth: 1 << 20, QueueBytes: 256 << 10}
)

type rig struct {
	net      *simnet.Network
	server   *endpoint.Endpoint
	client   map[string]*endpoint.Endpoint
	listener *Listener
}

func newRig(t *testing.T, links map[string]simnet.Link, clientSeed byte) *rig {
	r := &rig{net: simnet.New(42), client: map[string]*endpoint.Endpoint{}}
	mk := func(addr string, seed byte) *endpoint.Endpoint {
		pc, err := r.net.Listen(addr)
		if err != nil {
			t.Fatal(err)
		}
		ep, err := endpoint.New(pc, &ktp.Config{Identity: key(seed), ClusterKey: []byte("mp"), Congestion: "bbr",
			HandshakeTimeout: 2 * time.Second, KeepAlive: time.Second}, endpoint.Options{Resolve: simnet.Resolve})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { ep.Close() })
		return ep
	}
	r.server = mk("server", 1)
	for name, l := range links {
		addr := "phone." + name
		r.client[name] = mk(addr, clientSeed)
		r.net.SetLink(addr, "server", l)
		r.net.SetLink("server", addr, l)
	}
	return r
}

func (r *rig) paths(names ...string) []Path {
	var ps []Path
	for _, n := range names {
		ps = append(ps, Path{Name: n, Endpoint: r.client[n], Addr: "server"})
	}
	return ps
}

// transfer sends size bytes client -> server and returns how long until the
// server read them all, checking every byte.
func transfer(t *testing.T, r *rig, cfg Config, size int, during func(*Session), names ...string) (time.Duration, *Session) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	l := Listen(cfg, r.server)
	defer l.Close()
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i*31 + i>>9)
	}
	got := make(chan []byte, 1)
	go func() {
		s, err := l.Accept(ctx)
		if err != nil {
			got <- nil
			return
		}
		b, _ := io.ReadAll(s)
		got <- b
	}()
	start := time.Now()
	s, err := Dial(ctx, cfg, r.paths(names...)...)
	if err != nil {
		t.Fatal(err)
	}
	if during != nil {
		go during(s)
	}
	go func() { s.Write(data); s.CloseWrite() }()
	b := <-got
	took := time.Since(start)
	if !bytes.Equal(b, data) {
		t.Fatalf("received %d of %d bytes, or corrupted", len(b), len(data))
	}
	return took, s
}

// Bandwidth adds up across heterogeneous paths.
func TestAggregatesHeterogeneousPaths(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// Long enough for the satellite path's congestion controller to leave
		// startup (its RTT is ~0.25-0.5s): short transfers barely use it, as
		// with real MPTCP.
		const size = 16 << 20
		single, _ := transfer(t, newRig(t, map[string]simnet.Link{"fiber": fiber}, 9), Config{}, size, nil, "fiber")
		r := newRig(t, map[string]simnet.Link{"fiber": fiber, "cell": cellular, "sat": satellite}, 9)
		multi, s := transfer(t, r, Config{Scheduler: ECF}, size, nil, "fiber", "cell", "sat")
		st := s.Stats()
		t.Logf("16 MiB: fiber alone %v, fiber+cell+sat %v (reinjected %d KiB, %d suspicions)", single, multi, st.Reinjected>>10, st.Suspicions)
		for _, sf := range st.Subflows {
			t.Logf("  %-5s %7d KiB  srtt %v", sf.Name, sf.Bytes>>10, sf.SRTT)
		}
		if len(st.Subflows) != 3 {
			t.Fatalf("%d subflows joined", len(st.Subflows))
		}
		if float64(multi) > 0.65*float64(single) {
			t.Fatalf("multipath (%v) not meaningfully faster than the best single path (%v)", multi, single)
		}
		for _, sf := range st.Subflows {
			if sf.Bytes == 0 {
				t.Fatalf("path %s carried nothing", sf.Name)
			}
		}
		s.Close()
	})
}

// The fastest path goes dark mid-transfer: its in-flight data is reinjected
// on the others and the stream completes intact.
func TestSurvivesPathFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := newRig(t, map[string]simnet.Link{"fiber": fiber, "cell": cellular}, 9)
		cut := func(s *Session) {
			time.Sleep(400 * time.Millisecond)
			r.net.Block("phone.fiber", "server")
			r.net.Block("server", "phone.fiber")
		}
		_, s := transfer(t, r, Config{}, 3<<20, cut, "fiber", "cell")
		st := s.Stats()
		if st.Suspicions == 0 || st.Reinjected == 0 {
			t.Fatalf("no reinjection after the path died: %+v", st)
		}
		s.Close()
	})
}

// Redundant scheduling duplicates every segment; the receiver discards the
// copies and the stream is still exact.
func TestRedundantScheduler(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		lossy := simnet.Link{Latency: 10 * time.Millisecond, Loss: 0.05}
		r := newRig(t, map[string]simnet.Link{"a": lossy, "b": lossy}, 9)
		ctx := context.Background()
		l := Listen(Config{Scheduler: Redundant}, r.server)
		defer l.Close()
		done := make(chan *Session, 1)
		go func() {
			s, _ := l.Accept(ctx)
			io.ReadAll(s)
			done <- s
		}()
		s, err := Dial(ctx, Config{Scheduler: Redundant}, r.paths("a", "b")...)
		if err != nil {
			t.Fatal(err)
		}
		msg := bytes.Repeat([]byte("x"), 200<<10)
		s.Write(msg)
		s.CloseWrite()
		srv := <-done
		// Every segment goes to every path with room at that moment, so the
		// receiver sees copies (not necessarily two of everything).
		if st := srv.Stats(); st.DupReceived == 0 || st.BytesReceived-st.DupReceived != uint64(len(msg)) {
			t.Fatalf("redundant copies not seen at the receiver: %+v", st)
		}
		s.Close()
	})
}

// A join with another peer's identity is refused even with the right token.
func TestJoinRequiresSameIdentity(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r := newRig(t, map[string]simnet.Link{"a": {Latency: time.Millisecond}}, 9)
		imposterPC, _ := r.net.Listen("imposter")
		imposter, _ := endpoint.New(imposterPC, &ktp.Config{Identity: key(77), ClusterKey: []byte("mp"), HandshakeTimeout: time.Second},
			endpoint.Options{Resolve: simnet.Resolve})
		defer imposter.Close()
		ctx := context.Background()
		l := Listen(Config{}, r.server)
		defer l.Close()
		go l.Accept(ctx)
		s, err := Dial(ctx, Config{}, r.paths("a")...)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := openSubflow(ctx, Path{Name: "evil", Endpoint: imposter, Addr: "server"}, s.token, helloJoin); err == nil {
			t.Fatal("a different identity joined the session")
		}
		s.Close()
	})
}

// One subflow direct, one source-routed through a relay: a multipath
// session over disjoint paths.
func TestSegmentRoutedSubflow(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		n := simnet.New(7)
		n.SetDefaultLink(simnet.Link{Latency: 4 * time.Millisecond, Bandwidth: 1 << 20})
		relayPC, _ := n.Listen("relay")
		relay := segroute.NewRelay(relayPC, simnet.Resolve)
		defer relay.Close()
		cfg := func(s byte) *ktp.Config {
			return &ktp.Config{Identity: key(s), ClusterKey: []byte("mp"), HandshakeTimeout: time.Second}
		}
		spc, _ := n.Listen("server")
		srv, _ := endpoint.New(spc, cfg(1), endpoint.Options{Resolve: simnet.Resolve})
		defer srv.Close()
		dpc, _ := n.Listen("phone.direct")
		direct, _ := endpoint.New(dpc, cfg(9), endpoint.Options{Resolve: simnet.Resolve})
		defer direct.Close()
		rpc, _ := n.Listen("phone.routed")
		wrapped := segroute.Wrap(rpc, simnet.Resolve)
		wrapped.SetRoute("server", "relay")
		routed, _ := endpoint.New(wrapped, cfg(9), endpoint.Options{Resolve: simnet.Resolve})
		defer routed.Close()

		ctx := context.Background()
		l := Listen(Config{}, srv)
		defer l.Close()
		got := make(chan []byte, 1)
		go func() {
			s, _ := l.Accept(ctx)
			b, _ := io.ReadAll(s)
			got <- b
		}()
		s, err := Dial(ctx, Config{Scheduler: RoundRobin},
			Path{Name: "direct", Endpoint: direct, Addr: "server"}, Path{Name: "routed", Endpoint: routed, Addr: "server"})
		if err != nil {
			t.Fatal(err)
		}
		data := bytes.Repeat([]byte("segment routed "), 40000)
		s.Write(data)
		s.CloseWrite()
		if b := <-got; !bytes.Equal(b, data) {
			t.Fatalf("got %d of %d bytes", len(b), len(data))
		}
		st := s.Stats()
		if len(st.Subflows) != 2 || st.Subflows[0].Bytes == 0 || st.Subflows[1].Bytes == 0 {
			t.Fatalf("both paths should carry data: %+v", st.Subflows)
		}
		if rs := relay.Stats(); rs.Forwarded == 0 || rs.Returned == 0 {
			t.Fatalf("relay idle: %+v", rs)
		}
		s.Close()
	})
}
