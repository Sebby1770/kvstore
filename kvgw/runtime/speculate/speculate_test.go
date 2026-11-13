package speculate

import (
	"context"
	"crypto/ed25519"
	"fmt"
	"math/rand/v2"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Sebby1770/kvstore/kvgw/netstack/endpoint"
	"github.com/Sebby1770/kvstore/kvgw/netstack/ktp"
	"github.com/Sebby1770/kvstore/kvgw/netstack/rpc"
	"github.com/Sebby1770/kvstore/kvgw/netstack/simnet"
)

func key(seed byte) ed25519.PrivateKey {
	s := make([]byte, ed25519.SeedSize)
	s[0] = seed
	return ed25519.NewKeyFromSeed(s)
}

type world struct {
	net       *simnet.Network
	client    *rpc.Client
	cep       *endpoint.Endpoint
	regs      map[string]*Registry
	cancelled atomic.Int64
	completed atomic.Int64
	servers   []string
}

// newWorld starts n servers whose "work" handler takes delay(server, i)
// for its i-th call, honouring cancellation.
func newWorld(t *testing.T, n int, delay func(server string, call int) time.Duration) *world {
	w := &world{net: simnet.New(9), regs: map[string]*Registry{}}
	w.net.SetDefaultLink(simnet.Link{Latency: time.Millisecond})
	mk := func(addr string, seed byte) *endpoint.Endpoint {
		pc, err := w.net.Listen(addr)
		if err != nil {
			t.Fatal(err)
		}
		ep, err := endpoint.New(pc, &ktp.Config{Identity: key(seed), ClusterKey: []byte("spec"), HandshakeTimeout: time.Second},
			endpoint.Options{Resolve: simnet.Resolve, AcceptDialed: true})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { ep.Close() })
		return ep
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("s%d", i)
		w.servers = append(w.servers, name)
		ep := mk(name, byte(i+1))
		reg := NewRegistry(nil)
		w.regs[name] = reg
		ep.HandleRaw(Tag, reg.HandleRaw)
		srv := rpc.NewServer()
		srv.Use(reg.Interceptor())
		var calls atomic.Int64
		srv.Register("work", func(ctx context.Context, req []byte) ([]byte, error) {
			select {
			case <-time.After(delay(name, int(calls.Add(1)))):
				w.completed.Add(1)
				return []byte(name), nil
			case <-ctx.Done():
				w.cancelled.Add(1)
				return nil, ctx.Err()
			}
		})
		go srv.Serve(ctx, ep)
	}
	w.cep = mk("client", 99)
	w.client = rpc.NewClient(w.cep, rpc.ClientOptions{})
	return w
}

func (w *world) task(ctx context.Context, addr string) ([]byte, error) {
	return w.client.Call(ctx, addr, "work", nil, rpc.Idempotent())
}

func p99(ds []time.Duration) time.Duration {
	s := append([]time.Duration(nil), ds...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	return s[len(s)*99/100]
}

// Every server is usually fast but now and then stalls (a GC pause, a cold
// cache, a congested path) for 200ms. Calling one server directly inherits
// its tail; speculation hides it.
func TestHedgingMasksTailLatency(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rng := rand.New(rand.NewPCG(3, 3))
		var mu sync.Mutex
		w := newWorld(t, 4, func(string, int) time.Duration {
			mu.Lock()
			defer mu.Unlock()
			if rng.IntN(10) == 0 {
				return 200 * time.Millisecond
			}
			return 4*time.Millisecond + time.Duration(rng.IntN(2000))*time.Microsecond
		})
		ctx := context.Background()
		var direct []time.Duration
		for i := 0; i < 200; i++ {
			t0 := time.Now()
			if _, err := w.task(ctx, w.servers[i%4]); err != nil {
				t.Fatal(err)
			}
			direct = append(direct, time.Since(t0))
		}
		// With a 10% independent stall rate, two copies still both stall 1%
		// of the time -- exactly the p99 -- so allow a third.
		ex := NewExecutor(Config{MaxCopies: 3, BudgetRatio: 0.3}, nil, w.cep.SendRaw, nil)
		var hedged []time.Duration
		for i := 0; i < 200; i++ {
			out, err := ex.Do(ctx, w.servers, w.task)
			if err != nil {
				t.Fatal(err)
			}
			hedged = append(hedged, out.Latency)
		}
		time.Sleep(time.Second)
		st := ex.Stats()
		t.Logf("p99 direct %v, speculative %v; %d hedges (%d won), %d denied", p99(direct), p99(hedged), st.Hedges, st.HedgeWins, st.Denied)
		if p99(direct) < 150*time.Millisecond {
			t.Fatalf("the direct baseline has no tail (p99 %v)", p99(direct))
		}
		if p99(hedged) > 40*time.Millisecond {
			t.Fatalf("speculation did not mask the tail: p99 %v", p99(hedged))
		}
		if float64(st.Hedges) > 0.3*200+10 {
			t.Fatalf("%d hedges exceeds the budget", st.Hedges)
		}
		if st.HedgeWins == 0 || st.Cancels == 0 {
			t.Fatalf("hedges never won or losers never cancelled: %+v", st)
		}
		if w.cancelled.Load() == 0 {
			t.Fatal("no losing handler observed its cancellation")
		}
	})
}

// A copy whose cancel signal overtakes its request is refused on arrival.
func TestCancelBeforeArrivalIsRefused(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := newWorld(t, 1, func(string, int) time.Duration { return time.Millisecond })
		w.regs["s0"].Cancel("deadbeef")
		ctx := rpc.WithMetadata(context.Background(), MetaID, "deadbeef")
		_, err := w.task(ctx, "s0")
		if rpc.CodeOf(err) != rpc.Canceled {
			t.Fatalf("got %v, want canceled", err)
		}
		if st := w.regs["s0"].Stats(); st.Refused != 1 {
			t.Fatalf("registry %+v", st)
		}
	})
}

// A failing candidate is replaced at once (failover), whatever the budget.
func TestFailover(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := newWorld(t, 2, func(string, int) time.Duration { return 2 * time.Millisecond })
		ex := NewExecutor(Config{BudgetRatio: 0.0001, BudgetBurst: 0.0001}, nil, nil, nil)
		out, err := ex.Do(context.Background(), []string{"nowhere", "s1"}, func(ctx context.Context, addr string) ([]byte, error) {
			if addr == "nowhere" {
				return nil, rpc.Errorf(rpc.Unavailable, "down")
			}
			return w.task(ctx, addr)
		})
		if err != nil || out.Winner != "s1" {
			t.Fatalf("failover: %+v %v", out, err)
		}
		if st := ex.Stats(); st.Failovers != 1 {
			t.Fatalf("stats %+v", st)
		}
	})
}

// Digests reach every node epidemically, and ranking avoids loaded nodes.
func TestGossipConvergesAndSteers(t *testing.T) {
	const n = 16
	var nodes []*Gossip
	var addrs []string
	regs := map[string]*Registry{}
	for i := 0; i < n; i++ {
		a := fmt.Sprintf("n%d", i)
		g := NewGossip(a, a, uint64(i))
		g.SetClock(func() int64 { return 1 })
		g.Update(uint64(i*60), 0, time.Duration(i+1)*time.Millisecond, 10*time.Millisecond)
		nodes = append(nodes, g)
		addrs = append(addrs, a)
		regs[a] = NewRegistry(g)
	}
	rounds := 0
	complete := func() bool {
		for _, g := range nodes {
			if len(g.All()) != n {
				return false
			}
		}
		return true
	}
	for ; rounds < 50 && !complete(); rounds++ {
		for i, g := range nodes {
			for _, o := range g.Round(addrs) {
				regs[o.To].HandleRaw(addrs[i], o.Payload)
			}
		}
	}
	if !complete() {
		t.Fatalf("gossip did not converge in %d rounds", rounds)
	}
	if rounds > 12 {
		t.Fatalf("%d rounds for %d nodes: not epidemic", rounds, n)
	}
	// Newer digests replace older ones.
	nodes[3].Update(999, 7, time.Second, time.Second)
	for r := 0; r < 20; r++ {
		for i, g := range nodes {
			for _, o := range g.Round(addrs) {
				regs[o.To].HandleRaw(addrs[i], o.Payload)
			}
		}
	}
	if d, _ := nodes[9].Get("n3"); d.Load != 999 {
		t.Fatalf("n9 still has n3's old digest: %+v", d)
	}
	ex := NewExecutor(Config{}, nodes[0], nil, nil)
	if r := ex.Rank([]string{"n3", "n1", "n2"}); r[0] != "n1" || r[2] != "n3" {
		t.Fatalf("ranking %v: should prefer the idle, fast n1 and avoid the overloaded n3", r)
	}
	t.Logf("all %d digests everywhere after %d rounds", n, rounds)
}

func FuzzHandleRaw(f *testing.F) {
	f.Add([]byte{Tag, msgCancel, '{', '}'})
	f.Add(append([]byte{Tag, msgDigests}, `[{"n":"a","a":"a","s":1}]`...))
	f.Fuzz(func(t *testing.T, b []byte) {
		r := NewRegistry(NewGossip("me", "me", 1))
		r.HandleRaw("x", b)
	})
}

// Digests come from peers: a timestamp in the future must still expire, and
// a flood of distinct addresses cannot grow the table without bound.
func TestGossipRejectsHostileDigests(t *testing.T) {
	now := int64(1_000_000_000_000)
	g := NewGossip("me", "me", 1)
	g.SetClock(func() int64 { return now })
	g.merge([]Digest{{Node: "evil", Addr: "evil", Seq: 1, At: 1 << 62}})
	if d, _ := g.Get("evil"); d.At > now {
		t.Fatalf("future timestamp kept: %d", d.At)
	}
	now += int64(g.maxAge) + 1
	g.Round(nil) // expiry runs on rounds
	g.Round([]string{"x"})
	if _, ok := g.Get("evil"); ok {
		t.Fatal("a digest stamped in the future never expired")
	}
	var flood []Digest
	for i := 0; i < maxTable+500; i++ {
		flood = append(flood, Digest{Node: fmt.Sprint(i), Addr: fmt.Sprint("a", i), Seq: 1, At: now})
	}
	g.merge(flood)
	if n := len(g.All()); n > maxTable+1 {
		t.Fatalf("table grew to %d", n)
	}
	long := make([]byte, maxName+1)
	g.merge([]Digest{{Node: "n", Addr: string(long), Seq: 1}})
	if _, ok := g.Get(string(long)); ok {
		t.Fatal("oversized address accepted")
	}
}
