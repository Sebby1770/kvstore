package cluster

import (
	"testing"

	"github.com/Sebby1770/kvstore/kvgw/internal/kv"
)

func TestHintedHandoff(t *testing.T) {
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
	if !c.IsDown(pl[0]) {
		t.Fatal("heartbeat should mark the down replica")
	}

	if _, err := c.Put(key, []byte("v2")); err != nil {
		t.Fatal(err)
	}
	if c.Metrics().HintsStored == 0 {
		t.Fatal("expected a hint to be stored for the down replica")
	}
	victim.mu.Lock()
	stale := string(victim.data["k"].Value)
	victim.mu.Unlock()
	if stale == "v2" {
		t.Fatal("down replica should have missed the write")
	}

	victim.setDown(false)
	c.Heartbeat() // ping succeeds, hints replay
	if c.IsDown(pl[0]) {
		t.Fatal("replica should be marked up")
	}
	if c.Metrics().HintsReplayed == 0 {
		t.Fatal("expected the hint to be replayed")
	}
	victim.mu.Lock()
	got := victim.data["k"]
	victim.mu.Unlock()
	if got.State != kv.Present || string(got.Value) != "v2" {
		t.Fatalf("hinted handoff did not replay: %+v", got)
	}
	val, found, _, err := c.Get(key)
	if err != nil || !found || string(val) != "v2" {
		t.Fatalf("get after replay: found=%v val=%q err=%v", found, val, err)
	}
}

func TestHeartbeatMarksDown(t *testing.T) {
	c, fakes := build(3, 3, 2, 2)
	fakes[0].setDown(true)
	c.Heartbeat()
	if !c.IsDown("n0") {
		t.Fatal("expected n0 marked down")
	}
	if c.IsDown("n1") || c.IsDown("n2") {
		t.Fatal("live nodes should not be down")
	}
	nodes, allUp := c.Health()
	if allUp {
		t.Fatal("cluster should report degraded")
	}
	var n0 NodeHealth
	for _, n := range nodes {
		if n.Addr == "n0" {
			n0 = n
		}
	}
	if !n0.Down || n0.Up {
		t.Fatalf("health for n0: %+v", n0)
	}
	if n0.LastOK != nil && n0.LastOK.IsZero() {
		t.Fatal("last_ok should be omitted or non-zero")
	}

	fakes[0].setDown(false)
	c.Heartbeat()
	if c.IsDown("n0") {
		t.Fatal("n0 should recover")
	}
	nodes, allUp = c.Health()
	if !allUp {
		t.Fatal("cluster should be healthy after recovery")
	}
	for _, n := range nodes {
		if n.Addr == "n0" && (n.Down || !n.Up || n.LastOK == nil) {
			t.Fatalf("recovered n0: %+v", n)
		}
	}
}
