package control

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Sebby1770/kvstore/kvgw/runtime/raft"
)

// memNet connects Services in memory with a 1ms hop and a cut switch.
type memNet struct {
	mu   sync.Mutex
	svcs map[string]*Service
	cut  func(from, to string) bool
}

type memTransport struct {
	net  *memNet
	from string
}

func (t *memTransport) Send(msgs []raft.Message) {
	for _, m := range msgs {
		t.net.mu.Lock()
		dst, src := t.net.svcs[m.To], t.net.svcs[t.from]
		blocked := t.net.cut != nil && t.net.cut(t.from, m.To)
		t.net.mu.Unlock()
		if dst == nil || blocked {
			if m.Type == raft.MsgSnap && src != nil {
				go src.ReportSnapshot(m.To, false)
			}
			continue
		}
		wire, err := raft.UnmarshalMessage(m.Marshal())
		if err != nil {
			panic(err)
		}
		go func() {
			time.Sleep(time.Millisecond)
			dst.Step(wire)
			if wire.Type == raft.MsgSnap && src != nil {
				src.ReportSnapshot(wire.To, true)
			}
		}()
	}
}

type testCluster struct {
	t    *testing.T
	net  *memNet
	ids  []string
	dirs map[string]string
}

func newTestCluster(t *testing.T, n int, durable bool) *testCluster {
	c := &testCluster{t: t, net: &memNet{svcs: map[string]*Service{}}, dirs: map[string]string{}}
	for i := 1; i <= n; i++ {
		c.ids = append(c.ids, fmt.Sprintf("c%d", i))
	}
	for _, id := range c.ids {
		if durable {
			c.dirs[id] = t.TempDir()
		}
		c.start(id, c.ids)
	}
	return c
}

func (c *testCluster) start(id string, peers []string) *Service {
	c.t.Helper()
	s, err := NewService(ServiceConfig{ID: id, Peers: peers, DataDir: c.dirs[id],
		Transport: &memTransport{net: c.net, from: id}, SnapshotEvery: 20})
	if err != nil {
		c.t.Fatal(err)
	}
	c.net.mu.Lock()
	c.net.svcs[id] = s
	c.net.mu.Unlock()
	s.Start()
	return s
}

func (c *testCluster) svc(id string) *Service {
	c.net.mu.Lock()
	defer c.net.mu.Unlock()
	return c.net.svcs[id]
}

func (c *testCluster) stop(id string) {
	s := c.svc(id)
	c.net.mu.Lock()
	delete(c.net.svcs, id)
	c.net.mu.Unlock()
	s.Stop()
}

func (c *testCluster) stopAll() {
	for _, id := range c.ids {
		if c.svc(id) != nil {
			c.stop(id)
		}
	}
	// Let in-flight deliveries land (on stopped services) so the bubble
	// ends with no goroutines left.
	time.Sleep(20 * time.Millisecond)
}

// leader waits for a leader among the live services.
func (c *testCluster) leader() *Service {
	c.t.Helper()
	for i := 0; i < 200; i++ {
		for _, id := range c.ids {
			if s := c.svc(id); s != nil && s.IsLeader() {
				return s
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	c.t.Fatal("no leader elected")
	return nil
}

func (c *testCluster) follower() *Service {
	l := c.leader()
	for _, id := range c.ids {
		if s := c.svc(id); s != nil && s != l {
			return s
		}
	}
	return nil
}

// ctx is a context that ends after d (of the bubble's virtual time).
func ctx(d time.Duration) context.Context {
	c, cancel := context.WithCancel(context.Background())
	time.AfterFunc(d, cancel)
	return c
}

func TestServiceReplicatesAndForwards(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newTestCluster(t, 3, false)
		defer c.stopAll()
		f := c.follower()
		res, err := f.Apply(ctx(5*time.Second), AcquireLeaseCmd("lock", "worker-1", 30*time.Second, f.Now()))
		if err != nil {
			t.Fatalf("acquire via follower: %v", err)
		}
		time.Sleep(time.Second)
		for _, id := range c.ids {
			l, ok := c.svc(id).FSM().Lease("lock")
			if !ok || l.Token != res.Lease.Token || l.Holder != "worker-1" {
				t.Fatalf("%s: lease %+v ok=%v", id, l, ok)
			}
		}
	})
}

func TestServiceFailoverKeepsStateAndFencing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newTestCluster(t, 3, false)
		defer c.stopAll()
		l := c.leader()
		first, err := l.Apply(ctx(5*time.Second), AcquireLeaseCmd("lock", "a", 2*time.Second, l.Now()))
		if err != nil {
			t.Fatal(err)
		}
		old := l.cfg.ID
		c.stop(old)
		nl := c.leader()
		if nl.cfg.ID == old {
			t.Fatal("stopped leader still leads")
		}
		time.Sleep(3 * time.Second) // let a's lease lapse
		second, err := nl.Apply(ctx(5*time.Second), AcquireLeaseCmd("lock", "b", 2*time.Second, nl.Now()))
		if err != nil {
			t.Fatalf("acquire after failover: %v", err)
		}
		if second.Lease.Token <= first.Lease.Token {
			t.Fatalf("fencing token did not increase across failover: %d then %d", first.Lease.Token, second.Lease.Token)
		}
	})
}

func TestServiceReadBarrierIsLinearizable(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newTestCluster(t, 3, false)
		defer c.stopAll()
		l := c.leader()
		f := c.follower()
		for i := 0; i < 10; i++ {
			if _, err := l.Apply(ctx(5*time.Second), RegPutCmd(fmt.Sprintf("k%d", i), []byte("v"), l.Now())); err != nil {
				t.Fatal(err)
			}
			// Immediately after a write completes, a barrier read on a
			// follower must see it.
			if err := f.ReadBarrier(ctx(5 * time.Second)); err != nil {
				t.Fatal(err)
			}
			if _, ok := f.FSM().Get(fmt.Sprintf("k%d", i)); !ok {
				t.Fatalf("follower read after barrier missed write %d", i)
			}
		}
	})
}

func TestServiceRestartFromDisk(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newTestCluster(t, 3, true)
		l := c.leader()
		for i := 0; i < 45; i++ { // crosses SnapshotEvery: restart replays snapshot + log
			if _, err := l.Apply(ctx(5*time.Second), EnqueueJobCmd("q", fmt.Sprintf("j%d", i), nil, 1, 0, l.Now())); err != nil {
				t.Fatal(err)
			}
		}
		time.Sleep(time.Second)
		c.stopAll()
		for _, id := range c.ids {
			c.start(id, c.ids)
		}
		defer c.stopAll()
		nl := c.leader()
		if err := nl.ReadBarrier(ctx(5 * time.Second)); err != nil {
			t.Fatal(err)
		}
		if st := nl.FSM().Queue("q"); st.Pending != 45 {
			t.Fatalf("after restart queue has %d pending, want 45", st.Pending)
		}
		r, err := nl.Apply(ctx(5*time.Second), ClaimJobCmd("q", "w", time.Minute, nl.Now()))
		if err != nil || r.Job == nil || r.Job.ID != "j0" {
			t.Fatalf("claim after restart: %+v %v", r, err)
		}
	})
}

func TestServiceJoinCatchesUpBySnapshot(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newTestCluster(t, 3, false)
		defer c.stopAll()
		l := c.leader()
		for i := 0; i < 100; i++ {
			l.Apply(ctx(5*time.Second), RegPutCmd(fmt.Sprintf("k%03d", i), []byte("x"), l.Now()))
		}
		c.ids = append(c.ids, "c4")
		n4 := c.start("c4", nil) // empty peers: waits to be added
		if err := l.AddLearner(ctx(10*time.Second), "c4"); err != nil {
			t.Fatal(err)
		}
		if err := l.AddVoter(ctx(10*time.Second), "c4"); err != nil {
			t.Fatal(err)
		}
		time.Sleep(3 * time.Second)
		if got := len(n4.FSM().Keys("k")); got != 100 {
			t.Fatalf("joined replica has %d keys, want 100", got)
		}
		if st := l.Status(); len(st.Voters) != 4 {
			t.Fatalf("voters %v", st.Voters)
		}
		// The newcomer serves linearizable reads like everyone else.
		if err := n4.ReadBarrier(ctx(5 * time.Second)); err != nil {
			t.Fatal(err)
		}
	})
}

func TestServiceMinorityCannotCommit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newTestCluster(t, 3, false)
		defer c.stopAll()
		l := c.leader()
		lid := l.cfg.ID
		c.net.mu.Lock()
		c.net.cut = func(a, b string) bool { return a == lid || b == lid }
		c.net.mu.Unlock()
		_, err := l.Propose(ctx(3*time.Second), RegPutCmd("k", []byte("lost"), l.Now()))
		if !errors.Is(err, ErrNotApplied) && !errors.Is(err, ErrNoLeader) {
			t.Fatalf("isolated leader committed: %v", err)
		}
		// The majority elected someone else and keeps working.
		var other *Service
		for _, id := range c.ids {
			if id != lid && c.svc(id).IsLeader() {
				other = c.svc(id)
			}
		}
		if other == nil {
			t.Fatal("majority has no leader")
		}
		if _, err := other.Apply(ctx(5*time.Second), RegPutCmd("k", []byte("kept"), other.Now())); err != nil {
			t.Fatal(err)
		}
		c.net.mu.Lock()
		c.net.cut = nil
		c.net.mu.Unlock()
		time.Sleep(3 * time.Second)
		if e, _ := l.FSM().Get("k"); string(e.Value) != "kept" {
			t.Fatalf("old leader converged to %q", e.Value)
		}
	})
}

func TestServiceRingSubscription(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := newTestCluster(t, 3, false)
		defer c.stopAll()
		var mu sync.Mutex
		seen := map[string]uint64{}
		for _, id := range c.ids {
			id := id
			c.svc(id).OnRing(func(r RingConfig) {
				mu.Lock()
				seen[id] = r.Epoch
				mu.Unlock()
			})
		}
		l := c.leader()
		if _, err := l.Apply(ctx(5*time.Second), SetRingCmd([]string{"kv1:7070", "kv2:7070"}, 2, 1, 2, l.Now())); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Second)
		mu.Lock()
		defer mu.Unlock()
		for _, id := range c.ids {
			if seen[id] != 1 {
				t.Fatalf("%s saw epoch %d", id, seen[id])
			}
		}
	})
}
