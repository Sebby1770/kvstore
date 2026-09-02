package cluster

import (
	"testing"

	"github.com/Sebby1770/kvstore/kvgw/internal/kv"
)

func TestCompactAllDropsTombstones(t *testing.T) {
	c, fakes := build(3, 3, 2, 2)
	if _, err := c.Put([]byte("keep"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Put([]byte("gone"), []byte("x")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := c.Del([]byte("gone")); err != nil {
		t.Fatal(err)
	}

	tombs := 0
	for _, f := range fakes {
		f.mu.Lock()
		for _, r := range f.data {
			if r.State == kv.Deleted {
				tombs++
			}
		}
		f.mu.Unlock()
	}
	if tombs == 0 {
		t.Fatal("expected tombstones before compact")
	}

	n, errs := c.CompactAll()
	if n != 3 || len(errs) != 0 {
		t.Fatalf("compacted=%d errs=%v", n, errs)
	}
	if c.Metrics().Compacts != 1 {
		t.Fatalf("Compacts=%d want 1", c.Metrics().Compacts)
	}
	for _, f := range fakes {
		if f.state("keep") != kv.Present {
			t.Fatalf("%s lost keep", f.addr)
		}
		if f.state("gone") != kv.Missing {
			t.Fatalf("%s still has gone: %v", f.addr, f.state("gone"))
		}
	}
	val, found, _, err := c.Get([]byte("keep"))
	if err != nil || !found || string(val) != "v" {
		t.Fatalf("get keep after compact: found=%v val=%q err=%v", found, val, err)
	}

	fakes[0].setDown(true)
	c.Heartbeat()
	n, errs = c.CompactAll()
	if n != 2 {
		t.Fatalf("live compact n=%d errs=%v", n, errs)
	}
	if c.Metrics().Compacts != 2 {
		t.Fatalf("Compacts=%d want 2", c.Metrics().Compacts)
	}
}

func TestHintsPendingOnHealth(t *testing.T) {
	c, fakes := build(3, 3, 2, 2)
	byAddr := map[string]*fakeNode{}
	for _, f := range fakes {
		byAddr[f.addr] = f
	}
	key := []byte("k")
	if _, err := c.Put(key, []byte("v1")); err != nil {
		t.Fatal(err)
	}
	pl := c.Route(key)
	victim := byAddr[pl[0]]
	victim.setDown(true)
	c.Heartbeat()
	if _, err := c.Put(key, []byte("v2")); err != nil {
		t.Fatal(err)
	}
	pending := c.HintsPending()
	if pending[pl[0]] < 1 {
		t.Fatalf("hints pending: %v", pending)
	}

	nodes, _ := c.Health()
	var saw bool
	for _, n := range nodes {
		if n.Addr != pl[0] {
			continue
		}
		saw = true
		if n.HintsPending < 1 {
			t.Fatalf("health hints_pending: %+v", n)
		}
		if n.Up {
			t.Fatalf("victim should be down: %+v", n)
		}
	}
	if !saw {
		t.Fatal("missing victim in health")
	}

	victim.setDown(false)
	c.Heartbeat()
	if n := c.HintsPending()[pl[0]]; n != 0 {
		t.Fatalf("pending after replay: %d", n)
	}
	nodes, allUp := c.Health()
	if !allUp {
		t.Fatal("cluster should recover")
	}
	for _, n := range nodes {
		if n.Addr == pl[0] && n.HintsPending != 0 {
			t.Fatalf("health still pending: %+v", n)
		}
		if n.Capacity == 0 {
			t.Fatalf("expected capacity on live node: %+v", n)
		}
		if n.Count > 0 && n.Load <= 0 {
			t.Fatalf("expected load on occupied node: %+v", n)
		}
	}
}
