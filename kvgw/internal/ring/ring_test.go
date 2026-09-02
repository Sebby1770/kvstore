package ring

import (
	"fmt"
	"testing"
)

func keys(n int) [][]byte {
	out := make([][]byte, n)
	for i := range out {
		out[i] = []byte(fmt.Sprintf("key-%d", i))
	}
	return out
}

func TestPreflistShape(t *testing.T) {
	r := New([]string{"a", "b", "c", "d"})
	for _, k := range keys(200) {
		pl := r.Preflist(k, 3)
		if len(pl) != 3 {
			t.Fatalf("preflist len = %d, want 3", len(pl))
		}
		seen := map[string]bool{}
		for _, a := range pl {
			if seen[a] {
				t.Fatalf("duplicate %q in preflist %v", a, pl)
			}
			seen[a] = true
		}
	}
	// rf larger than the cluster is clamped, not padded.
	if got := r.Preflist([]byte("x"), 99); len(got) != 4 {
		t.Fatalf("over-large rf: len = %d, want 4", len(got))
	}
}

func TestDeterministic(t *testing.T) {
	a := New([]string{"a", "b", "c"})
	b := New([]string{"a", "b", "c"})
	for _, k := range keys(100) {
		if fmt.Sprint(a.Preflist(k, 2)) != fmt.Sprint(b.Preflist(k, 2)) {
			t.Fatalf("preflist not deterministic for %q", k)
		}
	}
}

func TestRoughlyEvenDistribution(t *testing.T) {
	addrs := []string{"a", "b", "c", "d", "e"}
	r := New(addrs)
	counts := map[string]int{}
	const n = 50000
	for _, k := range keys(n) {
		counts[r.Pick(k)]++
	}
	expected := n / len(addrs)
	for _, a := range addrs {
		got := counts[a]
		// Within 15% of even is plenty for a hash this simple.
		if got < expected*85/100 || got > expected*115/100 {
			t.Fatalf("node %q got %d keys, want ~%d", a, got, expected)
		}
	}
}

// The defining property of rendezvous hashing: adding a node only steals keys
// for itself; it never reshuffles keys among the pre-existing nodes. This is
// exactly what hash(key) mod N fails to guarantee.
func TestAddingNodeOnlyStealsKeys(t *testing.T) {
	old := New([]string{"a", "b", "c", "d"})
	nw := New([]string{"a", "b", "c", "d", "e"})

	moved := 0
	const n = 20000
	for _, k := range keys(n) {
		before := old.Pick(k)
		after := nw.Pick(k)
		if before == after {
			continue
		}
		moved++
		if after != "e" {
			t.Fatalf("key %q moved %s -> %s, but should only ever move to the new node", k, before, after)
		}
	}
	frac := float64(moved) / float64(n)
	// Should be about 1/5 of keys; assert it's in a sane band (and non-zero).
	if frac < 0.10 || frac > 0.30 {
		t.Fatalf("moved fraction = %.3f, want ~0.20", frac)
	}
}

// Removing a node only re-homes keys whose preflist included it.
func TestRemovingNodeOnlyAffectsItsKeys(t *testing.T) {
	full := New([]string{"a", "b", "c", "d"})
	less := New([]string{"a", "b", "c"})
	const rf = 2
	for _, k := range keys(20000) {
		before := full.Preflist(k, rf)
		hadD := false
		for _, a := range before {
			if a == "d" {
				hadD = true
			}
		}
		if hadD {
			continue // allowed to change
		}
		// "d" wasn't a replica for this key, so its preflist must be unchanged.
		if fmt.Sprint(before) != fmt.Sprint(less.Preflist(k, rf)) {
			t.Fatalf("key %q not on d changed preflist: %v -> %v", k, before, less.Preflist(k, rf))
		}
	}
}
