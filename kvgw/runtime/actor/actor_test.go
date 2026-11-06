package actor

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

// ---- fakes ----

type memStore struct {
	mu   sync.Mutex
	data map[ID][]byte
	ver  map[ID]uint64
}

func newMemStore() *memStore { return &memStore{data: map[ID][]byte{}, ver: map[ID]uint64{}} }

func (s *memStore) Load(_ context.Context, id ID) ([]byte, uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]byte(nil), s.data[id]...), s.ver[id], nil
}

func (s *memStore) Save(_ context.Context, id ID, data []byte, expect uint64) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ver[id] != expect {
		return 0, ErrConflict
	}
	s.ver[id]++
	s.data[id] = append([]byte(nil), data...)
	return s.ver[id], nil
}

func (s *memStore) get(id ID) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return string(s.data[id])
}

// placement: per-node view, defaulting to a fixed owner.
type placement struct {
	mu    sync.Mutex
	owner map[ID]string
	def   string
}

func (p *placement) Owner(id ID) (string, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if o, ok := p.owner[id]; ok {
		return o, true
	}
	return p.def, p.def != ""
}

func (p *placement) set(id ID, node string) {
	p.mu.Lock()
	p.owner[id] = node
	p.mu.Unlock()
}

type network struct {
	mu    sync.Mutex
	hosts map[string]*Host
}

type netTransport struct{ n *network }

func (t netTransport) Deliver(ctx context.Context, node string, env Envelope) ([]byte, error) {
	t.n.mu.Lock()
	h := t.n.hosts[node]
	t.n.mu.Unlock()
	if h == nil {
		return nil, errors.New("unreachable")
	}
	return h.HandleRemote(ctx, env)
}

type world struct {
	net   *network
	store *memStore
	place map[string]*placement // each node's own view
	hosts map[string]*Host
}

func newWorld(nodes ...string) *world {
	w := &world{net: &network{hosts: map[string]*Host{}}, store: newMemStore(), place: map[string]*placement{}, hosts: map[string]*Host{}}
	for _, n := range nodes {
		p := &placement{owner: map[ID]string{}, def: nodes[0]}
		h := NewHost(HostConfig{Node: n, Placement: p, Transport: netTransport{w.net}, Store: w.store,
			IdleTimeout: 10 * time.Second, MailboxSize: 8})
		h.Register("counter", func(ID) Actor { return &counter{} })
		h.Register("crashy", func(ID) Actor { return &crashy{} })
		w.place[n], w.hosts[n] = p, h
		w.net.hosts[n] = h
	}
	return w
}

func (w *world) setOwner(id ID, node string) {
	for _, p := range w.place {
		p.set(id, node)
	}
}

func (w *world) close() {
	for _, h := range w.hosts {
		h.Close(context.Background())
	}
}

// counter: "inc" adds one and persists; "get" reads; "where" names the node;
// "dirty" changes state without persisting; "slow" holds the turn.
type counter struct {
	inflight atomic.Int32
}

var maxConcurrent atomic.Int32

func (c *counter) Receive(ctx *Context, msg []byte) ([]byte, error) {
	if n := c.inflight.Add(1); n > maxConcurrent.Load() {
		maxConcurrent.Store(n)
	}
	defer c.inflight.Add(-1)
	v, _ := strconv.Atoi(string(ctx.State()))
	switch string(msg) {
	case "inc":
		ctx.SetState([]byte(strconv.Itoa(v + 1)))
		if err := ctx.Persist(); err != nil {
			return nil, err
		}
		return []byte(strconv.Itoa(v + 1)), nil
	case "dirty":
		ctx.SetState([]byte(strconv.Itoa(v + 100)))
		return nil, nil
	case "where":
		return []byte(ctx.Node()), nil
	case "slow":
		time.Sleep(time.Second)
		return nil, nil
	case "tick-on":
		ctx.SetTimer("t", time.Second, []byte("tick"))
		return nil, nil
	case "tick":
		ctx.SetState([]byte(strconv.Itoa(v + 1)))
		return nil, nil
	}
	return []byte(strconv.Itoa(v)), nil
}

type crashy struct{}

func (crashy) Receive(ctx *Context, msg []byte) ([]byte, error) {
	if string(msg) == "boom" {
		ctx.SetState([]byte("corrupted"))
		panic("boom")
	}
	if string(msg) == "save" {
		ctx.SetState([]byte("good"))
		return nil, ctx.Persist()
	}
	return ctx.State(), nil
}

// ---- tests ----

func TestTurnBasedConcurrency(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := newWorld("a")
		defer w.close()
		maxConcurrent.Store(0)
		id := ID{"counter", "x"}
		var wg sync.WaitGroup
		for i := 0; i < 200; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if _, err := w.hosts["a"].Ask(context.Background(), id, []byte("inc")); err != nil {
					t.Error(err)
				}
			}()
		}
		wg.Wait()
		if maxConcurrent.Load() != 1 {
			t.Fatalf("an activation ran %d turns at once", maxConcurrent.Load())
		}
		if got := w.store.get(id); got != "200" {
			t.Fatalf("counter = %s, want 200 (lost updates)", got)
		}
		if st := w.hosts["a"].Stats(); st.Activations != 1 {
			t.Fatalf("%d activations for one actor on one host", st.Activations)
		}
	})
}

func TestRoutesToOwner(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := newWorld("a", "b", "c")
		defer w.close()
		id := ID{"counter", "y"}
		w.setOwner(id, "b")
		for _, from := range []string{"a", "b", "c"} {
			got, err := w.hosts[from].Ask(context.Background(), id, []byte("where"))
			if err != nil || string(got) != "b" {
				t.Fatalf("from %s: served by %q (%v)", from, got, err)
			}
		}
	})
}

func TestMigrationOnRebalance(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := newWorld("a", "b")
		defer w.close()
		id := ID{"counter", "m"}
		w.setOwner(id, "a")
		ctx := context.Background()
		w.hosts["a"].Ask(ctx, id, []byte("inc"))
		w.hosts["a"].Ask(ctx, id, []byte("dirty")) // unpersisted change: must survive the move
		w.setOwner(id, "b")
		if n := w.hosts["a"].Rebalance(); n != 1 {
			t.Fatalf("rebalanced %d", n)
		}
		time.Sleep(10 * time.Millisecond)
		got, err := w.hosts["a"].Ask(ctx, id, []byte("get"))
		if err != nil || string(got) != "101" {
			t.Fatalf("after migration: %q %v", got, err)
		}
		if where, _ := w.hosts["a"].Ask(ctx, id, []byte("where")); string(where) != "b" {
			t.Fatalf("served by %s", where)
		}
	})
}

// Two nodes both believe they own the actor (membership views disagree).
// CAS-fenced state means no acknowledged increment is ever lost.
func TestSplitBrainLosesNoAcknowledgedUpdate(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := newWorld("a", "b")
		defer w.close()
		id := ID{"counter", "split"}
		w.place["a"].set(id, "a")
		w.place["b"].set(id, "b")
		ctx := context.Background()
		acked, stale := 0, 0
		for i := 0; i < 50; i++ {
			node := []string{"a", "b"}[i%2]
			_, err := w.hosts[node].Ask(ctx, id, []byte("inc"))
			switch {
			case err == nil:
				acked++
			case errors.Is(err, ErrStaleActivation):
				stale++
			default:
				t.Fatal(err)
			}
		}
		if stale == 0 {
			t.Fatal("split brain never produced a conflict (test did not exercise fencing)")
		}
		if got := w.store.get(id); got != strconv.Itoa(acked) {
			t.Fatalf("stored counter %s, but %d increments were acknowledged", got, acked)
		}
	})
}

func TestHopLimit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := newWorld("a", "b")
		defer w.close()
		id := ID{"counter", "loop"}
		w.place["a"].set(id, "b")
		w.place["b"].set(id, "a")
		if _, err := w.hosts["a"].Ask(context.Background(), id, []byte("get")); !errors.Is(err, ErrTooManyHops) {
			t.Fatalf("got %v", err)
		}
	})
}

func TestMailboxBackpressure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := newWorld("a")
		defer w.close()
		id := ID{"counter", "busy"}
		ctx := context.Background()
		full := false
		for i := 0; i < 20 && !full; i++ {
			if err := w.hosts["a"].Tell(ctx, id, []byte("slow")); errors.Is(err, ErrMailboxFull) {
				full = true
			}
		}
		if !full {
			t.Fatal("bounded mailbox never pushed back")
		}
		// Ask waits for room instead of failing.
		actx, cancel := context.WithTimeout(ctx, time.Minute)
		defer cancel()
		if _, err := w.hosts["a"].Ask(actx, id, []byte("get")); err != nil {
			t.Fatalf("ask under backpressure: %v", err)
		}
	})
}

func TestIdleDeactivationPersistsAndTimers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := newWorld("a")
		defer w.close()
		ctx := context.Background()
		id := ID{"counter", "idle"}
		w.hosts["a"].Ask(ctx, id, []byte("dirty"))
		time.Sleep(11 * time.Second) // > IdleTimeout
		if st := w.hosts["a"].Stats(); st.Active != 0 {
			t.Fatalf("still %d active after idle timeout", st.Active)
		}
		if got := w.store.get(id); got != "100" {
			t.Fatalf("dirty state not persisted on deactivation: %q", got)
		}
		tid := ID{"counter", "timer"}
		w.hosts["a"].Ask(ctx, tid, []byte("tick-on"))
		time.Sleep(5500 * time.Millisecond)
		got, _ := w.hosts["a"].Ask(ctx, tid, []byte("get"))
		if string(got) != "5" {
			t.Fatalf("timer ticked %s times in 5.5s, want 5", got)
		}
	})
}

func TestPanicSupervision(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := newWorld("a")
		defer w.close()
		ctx := context.Background()
		id := ID{"crashy", "c"}
		w.hosts["a"].Ask(ctx, id, []byte("save"))
		if _, err := w.hosts["a"].Ask(ctx, id, []byte("boom")); !errors.Is(err, ErrActorPanicked) {
			t.Fatalf("panic surfaced as %v", err)
		}
		if _, err := w.hosts["a"].Ask(ctx, id, []byte("get")); !errors.Is(err, ErrBackoff) {
			t.Fatalf("immediate retry: %v (want backoff)", err)
		}
		time.Sleep(200 * time.Millisecond)
		got, err := w.hosts["a"].Ask(ctx, id, []byte("get"))
		if err != nil || string(got) != "good" {
			t.Fatalf("reactivated with %q %v; want the last persisted state", got, err)
		}
		if w.hosts["a"].Stats().Crashes != 1 {
			t.Fatal("crash not counted")
		}
	})
}

func TestCloseDrains(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w := newWorld("a")
		ctx := context.Background()
		w.hosts["a"].Ask(ctx, ID{"counter", "z"}, []byte("dirty"))
		w.hosts["a"].Close(ctx)
		if got := w.store.get(ID{"counter", "z"}); got != "100" {
			t.Fatalf("close did not persist: %q", got)
		}
		if _, err := w.hosts["a"].Ask(ctx, ID{"counter", "z"}, nil); !errors.Is(err, ErrClosed) {
			t.Fatalf("ask after close: %v", err)
		}
		if _, err := w.hosts["a"].Ask(ctx, ID{"nope", "z"}, nil); err == nil {
			t.Fatal("unknown type accepted")
		}
	})
}
