package crdt

import (
	"fmt"
	"math/rand"
	"testing"
)

func clonePN(c *PNCounter) *PNCounter {
	d, _ := ParsePNCounter(c.Bytes())
	return d
}

func TestPNCounterValue(t *testing.T) {
	c := NewPNCounter()
	c.Add("a", 5)
	c.Add("b", 3)
	c.Add("a", -2)
	if c.Value() != 6 {
		t.Fatalf("value = %d, want 6", c.Value())
	}
}

func TestPNCounterMergeIsIdempotent(t *testing.T) {
	c := NewPNCounter()
	c.Add("a", 4)
	c.Add("b", 7)
	before := c.Value()
	c.Merge(clonePN(c))
	c.Merge(clonePN(c))
	if c.Value() != before {
		t.Fatalf("idempotence broken: %d -> %d", before, c.Value())
	}
}

func TestPNCounterMergeIsCommutativeAndAssociative(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	mk := func() *PNCounter {
		c := NewPNCounter()
		for i := 0; i < 20; i++ {
			c.Add(fmt.Sprintf("a%d", rng.Intn(3)), int64(rng.Intn(11)-5))
		}
		return c
	}
	a, b, c := mk(), mk(), mk()

	ab := clonePN(a)
	ab.Merge(b)
	ba := clonePN(b)
	ba.Merge(a)
	if ab.Value() != ba.Value() {
		t.Fatalf("not commutative: %d vs %d", ab.Value(), ba.Value())
	}

	left := clonePN(a) // (a∪b)∪c
	left.Merge(b)
	left.Merge(c)
	right := clonePN(b) // a∪(b∪c)
	right.Merge(c)
	r2 := clonePN(a)
	r2.Merge(right)
	if left.Value() != r2.Value() {
		t.Fatalf("not associative: %d vs %d", left.Value(), r2.Value())
	}
}

// The headline property: increments scattered across replicas, gossiped in a
// random order, always converge to the true total — no lost updates.
func TestPNCounterConverges(t *testing.T) {
	for seed := int64(0); seed < 40; seed++ {
		rng := rand.New(rand.NewSource(seed))
		replicas := []*PNCounter{NewPNCounter(), NewPNCounter(), NewPNCounter()}
		total := int64(0)
		for i := 0; i < 60; i++ {
			r := rng.Intn(len(replicas))
			replicas[r].Add(fmt.Sprintf("r%d", r), 1)
			total++
		}
		// Gossip in random pairings until everything has been merged in.
		merged := NewPNCounter()
		order := rng.Perm(len(replicas))
		for _, idx := range order {
			merged.Merge(replicas[idx])
		}
		// merge again in another order — must be stable.
		for _, idx := range rng.Perm(len(replicas)) {
			merged.Merge(replicas[idx])
		}
		if merged.Value() != total {
			t.Fatalf("seed %d: converged to %d, want %d", seed, merged.Value(), total)
		}
	}
}

func TestORSetAddWins(t *testing.T) {
	// Replica A adds "x" with a fresh tag; replica B (which never saw that tag)
	// removes "x". After merge, the add wins.
	a := NewORSet()
	a.Add("x", "tagA1")
	b := NewORSet()
	b.Remove("x") // B has no tags for x, so this is a no-op observation
	a.Merge(b)
	b.Merge(a)
	if !a.Contains("x") || !b.Contains("x") {
		t.Fatalf("concurrent add should survive a remove that didn't observe it")
	}
}

func TestORSetObservedRemove(t *testing.T) {
	a := NewORSet()
	a.Add("x", "t1")
	b, _ := ParseORSet(a.Bytes()) // B observes the add
	b.Remove("x")                 // now B's remove observes t1
	a.Merge(b)
	if a.Contains("x") {
		t.Fatal("an observed remove should win")
	}
	// A re-adds with a new tag -> present again.
	a.Add("x", "t2")
	if !a.Contains("x") {
		t.Fatal("re-add after remove should be present")
	}
}

func TestORSetMergeIsCommutativeAndIdempotent(t *testing.T) {
	a := NewORSet()
	a.Add("x", "t1")
	a.Add("y", "t2")
	b := NewORSet()
	b.Add("y", "t3")
	b.Add("z", "t4")
	b.Remove("y")

	ab, _ := ParseORSet(a.Bytes())
	ab.Merge(b)
	ba, _ := ParseORSet(b.Bytes())
	ba.Merge(a)
	if fmt.Sprint(ab.Members()) != fmt.Sprint(ba.Members()) {
		t.Fatalf("not commutative: %v vs %v", ab.Members(), ba.Members())
	}
	before := fmt.Sprint(ab.Members())
	ab.Merge(b)
	ab.Merge(a)
	if fmt.Sprint(ab.Members()) != before {
		t.Fatalf("not idempotent: %s -> %v", before, ab.Members())
	}
	// y was added on both sides but removed on b after observing only t3;
	// a's t2 for y was never observed by the remove, so y survives.
	want := "[x y z]"
	if got := fmt.Sprint(ab.Members()); got != want {
		t.Fatalf("members = %s, want %s", got, want)
	}
}
