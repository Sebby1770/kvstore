package cluster

import (
	"testing"

	"github.com/Sebby1770/kvstore/kvgw/internal/kv"
)

// valueOn reads a replica's stored value directly, bypassing the cluster, so a
// test can see divergence that a quorum read would mask.
func valueOn(t *testing.T, f *fakeNode, key string) (string, uint64, kv.State) {
	t.Helper()
	res, err := f.Get([]byte(key))
	if err != nil {
		t.Fatalf("replica %s unreachable: %v", f.addr, err)
	}
	return string(res.Value), res.Version, res.State
}

// A version must never be minted from an incomplete view of the replica set.
//
// The gateway assigns each write a version from a Lamport clock seeded by the
// highest version it can currently see. If that peek is allowed to proceed
// without a read quorum, a restarted gateway talking to a lagging replica can
// reissue a version that is already in use for a *different* value. Nothing
// afterwards can repair that: reads prefer strictly-greater versions, and
// read-repair treats an equal version as already up to date, so the replicas
// disagree forever and which value a client sees depends on who answers.
func TestWriteDoesNotReuseVersionAfterPartialPeek(t *testing.T) {
	const key = "k"
	c, fakes := build(3, 3, 2, 2)

	// Everyone agrees on the first value.
	if _, err := c.Put([]byte(key), []byte("first")); err != nil {
		t.Fatalf("initial put: %v", err)
	}

	// Replica 2 misses the next write, so it lags behind 0 and 1.
	fakes[2].setDown(true)
	if _, err := c.Put([]byte(key), []byte("second")); err != nil {
		t.Fatalf("second put should still reach a write quorum: %v", err)
	}

	// The gateway restarts (fresh clock) and can now only reach the replica
	// that lagged -- the two holding the newest value are unreachable.
	restarted, _ := rebuild(fakes, 3, 3, 2, 2)
	fakes[0].setDown(true)
	fakes[1].setDown(true)
	fakes[2].setDown(false)

	// This write cannot reach a write quorum, so the client is told it failed.
	if _, err := restarted.Put([]byte(key), []byte("third")); err == nil {
		t.Fatal("put reaching only one replica must not report success")
	}

	// Everything comes back. A read (with repair) must leave the cluster in
	// agreement -- and must never surface the value from the failed write.
	fakes[0].setDown(false)
	fakes[1].setDown(false)

	got, found, _, err := restarted.Get([]byte(key))
	if err != nil {
		t.Fatalf("get after recovery: %v", err)
	}
	if !found {
		t.Fatal("key vanished after recovery")
	}
	if string(got) == "third" {
		t.Fatalf("a write that failed its quorum became visible: %q", got)
	}

	// Read repair has now run; every replica must hold the same value.
	v0, ver0, st0 := valueOn(t, fakes[0], key)
	v1, ver1, st1 := valueOn(t, fakes[1], key)
	v2, ver2, st2 := valueOn(t, fakes[2], key)
	if v0 != v1 || v1 != v2 {
		t.Fatalf("replicas diverged: %q(v%d,%v) %q(v%d,%v) %q(v%d,%v)",
			v0, ver0, st0, v1, ver1, st1, v2, ver2, st2)
	}
	if ver0 != ver1 || ver1 != ver2 {
		t.Fatalf("replicas hold the same value at different versions: %d %d %d",
			ver0, ver1, ver2)
	}
}

// A write must also fail outright when no replica can be reached, rather than
// inventing version 1 on a fresh clock and overwriting real history later.
func TestWriteFailsWhenPeekSeesNoReplica(t *testing.T) {
	c, fakes := build(3, 3, 2, 2)
	if _, err := c.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("seed put: %v", err)
	}
	for _, f := range fakes {
		f.setDown(true)
	}
	if _, err := c.Put([]byte("k"), []byte("v2")); err == nil {
		t.Fatal("put with every replica down must fail")
	}
	if _, _, err := c.Del([]byte("k")); err == nil {
		t.Fatal("delete with every replica down must fail")
	}
}

// Equal versions carrying different values must still converge.
//
// A version collision should be impossible now that minting one requires a
// quorum peek, but "should be impossible" is not a convergence guarantee: a
// second gateway, a direct write to a node, or a replication path can still
// produce one. If that happens the cluster must heal itself rather than serve
// a different answer depending on which replicas respond.
func TestEqualVersionsWithDifferentValuesConverge(t *testing.T) {
	const key = "k"
	c, fakes := build(3, 3, 2, 2)

	// Seed a split-brain directly: same version, different values.
	for i, val := range []string{"alpha", "alpha", "beta"} {
		if err := fakes[i].Put([]byte(key), []byte(val), 5); err != nil {
			t.Fatalf("seed replica %d: %v", i, err)
		}
	}

	first, found, _, err := c.Get([]byte(key))
	if err != nil || !found {
		t.Fatalf("get: err=%v found=%v", err, found)
	}

	// Read repair has run; every replica must now agree.
	var vals []string
	for _, f := range fakes {
		res, gerr := f.Get([]byte(key))
		if gerr != nil {
			t.Fatalf("replica %s: %v", f.addr, gerr)
		}
		vals = append(vals, string(res.Value))
	}
	if vals[0] != vals[1] || vals[1] != vals[2] {
		t.Fatalf("replicas still disagree after a read: %q", vals)
	}

	// And the answer must not depend on which replicas happen to respond.
	for i := range fakes {
		for j := range fakes {
			fakes[j].setDown(j == i) // take one replica down at a time
		}
		got, ok, _, gerr := c.Get([]byte(key))
		if gerr != nil || !ok {
			t.Fatalf("get with replica %d down: err=%v found=%v", i, gerr, ok)
		}
		if string(got) != string(first) {
			t.Fatalf("answer changed with replica %d down: %q then %q",
				i, first, got)
		}
	}
}
