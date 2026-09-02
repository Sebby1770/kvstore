package cluster

import (
	"fmt"
	"testing"

	"github.com/Sebby1770/kvstore/kvgw/internal/kv"
)

func contains(s []string, x string) bool {
	for _, v := range s {
		if v == x {
			return true
		}
	}
	return false
}

func TestLiveMembership(t *testing.T) {
	c, _ := build(3, 3, 2, 2)
	keys := make([]string, 0, 8)
	for i := 0; i < 8; i++ {
		k := fmt.Sprintf("k%d", i)
		keys = append(keys, k)
		if _, err := c.Put([]byte(k), []byte("v"+k)); err != nil {
			t.Fatal(err)
		}
	}
	if c.RF() != 3 {
		t.Fatalf("RF = %d, want 3", c.RF())
	}

	// Join a fourth node at runtime.
	n3 := newFake("n3")
	if !c.AddNode("n3", n3) {
		t.Fatal("AddNode returned false")
	}
	if c.AddNode("n3", n3) {
		t.Fatal("duplicate AddNode should fail")
	}
	if len(c.Addrs()) != 4 {
		t.Fatalf("addrs after add = %v", c.Addrs())
	}

	// Reads stay correct across the resize (and heal the newcomer on the way).
	for _, k := range keys {
		val, found, _, err := c.Get([]byte(k))
		if err != nil || !found || string(val) != "v"+k {
			t.Fatalf("get %s: %q found=%v err=%v", k, val, found, err)
		}
	}

	// A sweep guarantees n3 holds every key it is now a replica for.
	c.RepairSweep()
	homed := 0
	for _, k := range keys {
		if contains(c.Route([]byte(k)), "n3") {
			homed++
			if n3.state(k) != kv.Present {
				t.Fatalf("n3 is a replica for %s but doesn't hold it after the sweep", k)
			}
		}
	}
	if homed == 0 {
		t.Fatal("expected the new node to become a replica for some keys")
	}

	// Leave: remove a node; every key must remain reachable from the survivors.
	if !c.RemoveNode("n0") {
		t.Fatal("RemoveNode returned false")
	}
	if len(c.Addrs()) != 3 {
		t.Fatalf("addrs after remove = %v", c.Addrs())
	}
	for _, k := range keys {
		val, found, _, err := c.Get([]byte(k))
		if err != nil || !found || string(val) != "v"+k {
			t.Fatalf("after remove, get %s: %q found=%v err=%v", k, val, found, err)
		}
	}

	// The cluster refuses to remove its last node.
	c.RemoveNode("n1")
	c.RemoveNode("n2")
	if len(c.Addrs()) != 1 {
		t.Fatalf("addrs = %v, want one survivor", c.Addrs())
	}
	if c.RemoveNode("n3") {
		t.Fatal("should refuse to remove the last node")
	}
}

func TestEffectiveQuorumTracksSize(t *testing.T) {
	c, _ := build(2, 3, 2, 2) // configured RF=3 but only 2 nodes
	if c.RF() != 2 || c.R() != 2 || c.W() != 2 {
		t.Fatalf("with 2 nodes: RF=%d R=%d W=%d", c.RF(), c.R(), c.W())
	}
	c.AddNode("n2", newFake("n2"))
	if c.RF() != 3 {
		t.Fatalf("after growing to 3 nodes, RF = %d, want 3", c.RF())
	}
}
