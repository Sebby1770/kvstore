package cluster

import (
	"testing"

	"github.com/Sebby1770/kvstore/kvgw/internal/kv"
)

func TestDeletePrefix(t *testing.T) {
	c, _ := build(3, 3, 2, 2)
	if _, err := c.Put([]byte("a"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Put([]byte("aa"), []byte("2")); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Put([]byte("b"), []byte("3")); err != nil {
		t.Fatal(err)
	}

	n, err := c.DeletePrefix("a")
	if err != nil || n != 2 {
		t.Fatalf("DeletePrefix: n=%d err=%v", n, err)
	}
	if c.Metrics().PrefixDeletes != 1 {
		t.Fatalf("PrefixDeletes=%d want 1", c.Metrics().PrefixDeletes)
	}

	if _, found, _, err := c.Get([]byte("a")); err != nil || found {
		t.Fatalf("a should be gone: found=%v err=%v", found, err)
	}
	if _, found, _, err := c.Get([]byte("aa")); err != nil || found {
		t.Fatalf("aa should be gone: found=%v err=%v", found, err)
	}
	val, found, _, err := c.Get([]byte("b"))
	if err != nil || !found || string(val) != "3" {
		t.Fatalf("b should remain: found=%v val=%q err=%v", found, val, err)
	}

	n, err = c.DeletePrefix("z")
	if err != nil || n != 0 {
		t.Fatalf("empty match: n=%d err=%v", n, err)
	}
	if c.Metrics().PrefixDeletes != 2 {
		t.Fatalf("PrefixDeletes=%d want 2", c.Metrics().PrefixDeletes)
	}
}

func TestForceDownNotHealedByHeartbeat(t *testing.T) {
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

	c.ForceDown(pl[0], true)
	if !c.IsDown(pl[0]) {
		t.Fatal("ForceDown should mark the replica down for writes")
	}
	if err := victim.Ping(); err != nil {
		t.Fatal("force-downed node is still reachable")
	}

	c.Heartbeat()
	if !c.IsDown(pl[0]) {
		t.Fatal("heartbeat must not heal a force-downed node")
	}

	nodes, allUp := c.Health()
	if allUp {
		t.Fatal("cluster should report degraded while forced")
	}
	var n0 NodeHealth
	for _, n := range nodes {
		if n.Addr == pl[0] {
			n0 = n
		}
	}
	if !n0.Forced || !n0.Down {
		t.Fatalf("health after ForceDown+Heartbeat+Health: %+v", n0)
	}

	if _, err := c.Put(key, []byte("v2")); err != nil {
		t.Fatal(err)
	}
	if c.Metrics().HintsStored == 0 {
		t.Fatal("expected a hint for the force-downed replica")
	}
	victim.mu.Lock()
	stale := string(victim.data["k"].Value)
	victim.mu.Unlock()
	if stale == "v2" {
		t.Fatal("force-downed replica should have missed the write")
	}

	c.Heartbeat() // ping still succeeds; must not replay hints while forced
	victim.mu.Lock()
	still := string(victim.data["k"].Value)
	victim.mu.Unlock()
	if still == "v2" {
		t.Fatal("heartbeat must not replay hints onto a force-downed node")
	}

	c.ForceDown(pl[0], false)
	c.Heartbeat()
	if c.IsDown(pl[0]) {
		t.Fatal("ForceDown(false) then heartbeat should mark the node up")
	}
	if c.Metrics().HintsReplayed == 0 {
		t.Fatal("expected hint replay after ForceDown(false)")
	}
	victim.mu.Lock()
	got := victim.data["k"]
	victim.mu.Unlock()
	if got.State != kv.Present || string(got.Value) != "v2" {
		t.Fatalf("hinted handoff after heal: %+v", got)
	}
}

func TestLatencyEWMA(t *testing.T) {
	c, _ := build(3, 3, 2, 2)
	if _, err := c.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := c.Get([]byte("k")); err != nil {
		t.Fatal(err)
	}
	m := c.Metrics()
	if m.LatencyGetMs < 0 || m.LatencyPutMs < 0 {
		t.Fatalf("negative latency: %+v", m)
	}
	if m.LatencyPutMs == 0 && m.Puts != 1 {
		t.Fatalf("put latency not observed: %+v", m)
	}
}
