package cluster

import (
	"sync"
	"testing"

	"github.com/Sebby1770/kvstore/kvgw/internal/crdt"
)

func incCounter(states [][]byte, delta int64) ([]byte, error) {
	ctr := crdt.NewPNCounter()
	for _, s := range states {
		if o, err := crdt.ParsePNCounter(s); err == nil {
			ctr.Merge(o)
		}
	}
	if delta != 0 {
		ctr.Add("gw", delta)
	}
	return ctr.Bytes(), nil
}

func counterValue(t *testing.T, c *Cluster, key []byte) int64 {
	t.Helper()
	val, _, err := c.Mutate(key, func(s [][]byte) ([]byte, error) { return incCounter(s, 0) })
	if err != nil {
		t.Fatalf("read counter: %v", err)
	}
	ctr, _ := crdt.ParsePNCounter(val)
	return ctr.Value()
}

// Plain last-writer-wins would lose updates here (read-modify-write races); the
// per-key lock + CRDT merge in Mutate must count every increment.
func TestCRDTCounterNoLostUpdates(t *testing.T) {
	c, _ := build(3, 3, 2, 2)
	key := []byte("votes")
	const n = 50

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, err := c.Mutate(key, func(s [][]byte) ([]byte, error) { return incCounter(s, 1) })
			if err != nil {
				t.Errorf("mutate: %v", err)
			}
		}()
	}
	wg.Wait()

	if got := counterValue(t, c, key); got != n {
		t.Fatalf("counter = %d, want %d (lost updates)", got, n)
	}
}

// A counter must stay correct even when a replica misses some increments and is
// later folded back in by the read-all-then-merge.
func TestCRDTCounterConvergesAfterReplicaDown(t *testing.T) {
	c, fakes := build(4, 3, 2, 2) // one replica down still leaves a write + peek quorum
	byAddr := map[string]*fakeNode{}
	for _, f := range fakes {
		byAddr[f.addr] = f
	}
	key := []byte("hits")
	for i := 0; i < 3; i++ {
		c.Mutate(key, func(s [][]byte) ([]byte, error) { return incCounter(s, 1) })
	}
	victim := byAddr[c.Route(key)[0]]
	victim.setDown(true)
	for i := 0; i < 4; i++ {
		c.Mutate(key, func(s [][]byte) ([]byte, error) { return incCounter(s, 1) })
	}
	victim.setDown(false) // returns holding only the first 3 increments

	// The next read gathers all replicas (including the stale victim) and merges
	// — the total must still be 7, and the victim is repaired by the write-back.
	if got := counterValue(t, c, key); got != 7 {
		t.Fatalf("after churn, counter = %d, want 7", got)
	}
	if cv := byAddr[victim.addr]; cv.state("hits") != 1 { // present
		t.Fatalf("victim not healed: state=%v", cv.state("hits"))
	}
}
