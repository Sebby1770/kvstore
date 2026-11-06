package cluster

import (
	"testing"

	"github.com/Sebby1770/kvstore/kvgw/internal/kv"
)

// A kvmesh node adopts rings committed by the control plane. Applies can be
// observed late or twice, so Reconfigure must be monotonic in the epoch.
func TestReconfigureIsEpochMonotonic(t *testing.T) {
	c, fakes := build(3, 3, 2, 2)
	if c.Epoch() != 0 {
		t.Fatalf("standalone epoch = %d, want 0", c.Epoch())
	}
	extra := newFake("n3")
	nodes := map[string]kv.Node{}
	for _, f := range fakes {
		nodes[f.addr] = f
	}
	nodes["n3"] = extra
	all := []string{"n0", "n1", "n2", "n3"}

	if !c.Reconfigure(2, all, nodes, 3, 1, 3) {
		t.Fatal("epoch 2 should apply")
	}
	if c.Epoch() != 2 || len(c.Addrs()) != 4 || c.R() != 1 || c.W() != 3 {
		t.Fatalf("after epoch 2: epoch=%d addrs=%v R=%d W=%d", c.Epoch(), c.Addrs(), c.R(), c.W())
	}
	// A stale (or repeated) epoch never rolls the ring back.
	if c.Reconfigure(1, all[:2], nodes, 1, 1, 1) || c.Reconfigure(2, all[:2], nodes, 1, 1, 1) {
		t.Fatal("stale epochs must be ignored")
	}
	if len(c.Addrs()) != 4 || c.RF() != 3 {
		t.Fatalf("stale epoch changed the ring: %v RF=%d", c.Addrs(), c.RF())
	}
}

// Hints for a replica that left the ring are dropped: replaying them later
// would write to a node that no longer owns the keys.
func TestReconfigureDropsStateOfRemovedNodes(t *testing.T) {
	c, fakes := build(3, 3, 2, 2)
	fakes[2].setDown(true)
	c.Heartbeat()
	if _, err := c.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if c.HintsPending()["n2"] == 0 {
		t.Fatal("precondition: expected a hint for the down replica")
	}
	nodes := map[string]kv.Node{"n0": fakes[0], "n1": fakes[1]}
	if !c.Reconfigure(1, []string{"n0", "n1"}, nodes, 2, 1, 2) {
		t.Fatal("reconfigure should apply")
	}
	if n := c.HintsPending()["n2"]; n != 0 {
		t.Fatalf("hints for removed node survived: %d", n)
	}
	if c.IsDown("n2") {
		t.Fatal("heartbeat state for removed node survived")
	}
}
