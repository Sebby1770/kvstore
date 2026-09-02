package cluster

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/Sebby1770/kvstore/kvgw/internal/kv"
)

// fakeNode is an in-memory storage node with the same last-writer-wins
// semantics as the C engine, plus a "down" switch to simulate failures.
type fakeNode struct {
	addr string
	mu   sync.Mutex
	data map[string]kv.ReadResult
	down bool
}

func newFake(addr string) *fakeNode {
	return &fakeNode{addr: addr, data: map[string]kv.ReadResult{}}
}

var errDown = errors.New("node down")

func (f *fakeNode) setDown(b bool) { f.mu.Lock(); f.down = b; f.mu.Unlock() }

func (f *fakeNode) Addr() string { return f.addr }

func (f *fakeNode) Ping() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.down {
		return errDown
	}
	return nil
}

func (f *fakeNode) Stat() (kv.Stat, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.down {
		return kv.Stat{}, errDown
	}
	st := kv.Stat{Capacity: 1024}
	for _, r := range f.data {
		if r.State == kv.Present {
			st.Count++
		} else if r.State == kv.Deleted {
			st.Tombstones++
		}
		if r.Version > st.MaxVersion {
			st.MaxVersion = r.Version
		}
	}
	return st, nil
}

func (f *fakeNode) Get(key []byte) (kv.ReadResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.down {
		return kv.ReadResult{}, errDown
	}
	if r, ok := f.data[string(key)]; ok {
		return r, nil
	}
	return kv.ReadResult{State: kv.Missing}, nil
}

func (f *fakeNode) Put(key, val []byte, version uint64) error {
	return f.PutX(key, val, version, 0)
}

func (f *fakeNode) PutX(key, val []byte, version, expiresAt uint64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.down {
		return errDown
	}
	if cur, ok := f.data[string(key)]; ok && cur.Version > version {
		return nil // LWW: stale write is a no-op
	}
	f.data[string(key)] = kv.ReadResult{
		State:     kv.Present,
		Value:     append([]byte(nil), val...),
		Version:   version,
		ExpiresAt: expiresAt,
	}
	return nil
}

func (f *fakeNode) Del(key []byte, version uint64) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.down {
		return false, errDown
	}
	cur, ok := f.data[string(key)]
	if ok && cur.Version > version {
		return cur.State == kv.Present, nil
	}
	existed := ok && cur.State == kv.Present
	f.data[string(key)] = kv.ReadResult{State: kv.Deleted, Version: version}
	return existed, nil
}

func (f *fakeNode) scanLocked(all bool) ([]kv.Entry, error) {
	if f.down {
		return nil, errDown
	}
	var out []kv.Entry
	for k, r := range f.data {
		if r.State == kv.Missing {
			continue
		}
		if !all && r.State != kv.Present {
			continue
		}
		out = append(out, kv.Entry{
			Key:       []byte(k),
			Value:     r.Value,
			Version:   r.Version,
			ExpiresAt: r.ExpiresAt,
			Tombstone: r.State == kv.Deleted,
		})
	}
	return out, nil
}

func (f *fakeNode) Scan() ([]kv.Entry, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.scanLocked(false)
}

func (f *fakeNode) ScanAll() ([]kv.Entry, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.scanLocked(true)
}

func (f *fakeNode) Compact() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.down {
		return errDown
	}
	for k, r := range f.data {
		if r.State == kv.Deleted {
			delete(f.data, k)
		}
	}
	return nil
}

func (f *fakeNode) version(key string) uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.data[key].Version
}

func (f *fakeNode) state(key string) kv.State {
	f.mu.Lock()
	defer f.mu.Unlock()
	if r, ok := f.data[key]; ok {
		return r.State
	}
	return kv.Missing
}

// build a cluster of n fake nodes with the given quorum settings.
func build(n, rf, r, w int) (*Cluster, []*fakeNode) {
	addrs := make([]string, n)
	fakes := make([]*fakeNode, n)
	nodes := map[string]kv.Node{}
	for i := 0; i < n; i++ {
		addr := fmt.Sprintf("n%d", i)
		addrs[i] = addr
		fakes[i] = newFake(addr)
		nodes[addr] = fakes[i]
	}
	return New(nodes, addrs, rf, r, w), fakes
}

func TestPutThenGet(t *testing.T) {
	c, _ := build(3, 3, 2, 2)
	if _, err := c.Put([]byte("hello"), []byte("world")); err != nil {
		t.Fatalf("put: %v", err)
	}
	val, found, meta, err := c.Get([]byte("hello"))
	if err != nil || !found {
		t.Fatalf("get: found=%v err=%v", found, err)
	}
	if string(val) != "world" {
		t.Fatalf("got %q, want world", val)
	}
	if meta.Version == 0 {
		t.Fatalf("expected a non-zero version")
	}
}

func TestReplicatedToRF(t *testing.T) {
	c, fakes := build(5, 3, 2, 2)
	if _, err := c.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	holders := 0
	for _, f := range fakes {
		if f.state("k") == kv.Present {
			holders++
		}
	}
	if holders != 3 {
		t.Fatalf("key stored on %d nodes, want RF=3", holders)
	}
}

func TestReadSurvivesOneReplicaDown(t *testing.T) {
	c, fakes := build(3, 3, 2, 2)
	if _, err := c.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	// Take down one replica; R=2 so the read should still succeed.
	c.Route([]byte("k"))
	fakes[0].setDown(true)
	val, found, _, err := c.Get([]byte("k"))
	if err != nil || !found || string(val) != "v" {
		t.Fatalf("read with one node down failed: found=%v err=%v val=%q", found, err, val)
	}
}

func TestWriteQuorumFailsWithTooManyDown(t *testing.T) {
	c, fakes := build(3, 3, 2, 2)
	fakes[0].setDown(true)
	fakes[1].setDown(true)
	_, err := c.Put([]byte("k"), []byte("v"))
	if !errors.Is(err, ErrQuorum) {
		t.Fatalf("expected ErrQuorum with 2/3 down, got %v", err)
	}
}

func TestReadRepairHealsStaleReplica(t *testing.T) {
	c, fakes := build(3, 3, 3, 1) // W=1 lets a write land with replicas down
	// First write reaches everyone.
	if _, err := c.Put([]byte("k"), []byte("v1")); err != nil {
		t.Fatal(err)
	}
	// Replica 2 goes away; a newer write lands on the other two.
	fakes[2].setDown(true)
	if _, err := c.Put([]byte("k"), []byte("v2")); err != nil {
		t.Fatal(err)
	}
	v2 := fakes[0].version("k")
	// Replica 2 comes back holding the stale v1.
	fakes[2].setDown(false)
	if fakes[2].version("k") >= v2 {
		t.Fatalf("precondition: replica 2 should be stale")
	}
	// A quorum read returns the fresh value and repairs the stale replica.
	val, found, meta, err := c.Get([]byte("k"))
	if err != nil || !found || string(val) != "v2" {
		t.Fatalf("read returned found=%v val=%q err=%v", found, val, err)
	}
	if fakes[2].version("k") != v2 || string(fakes[2].data["k"].Value) != "v2" {
		t.Fatalf("replica 2 not repaired: %+v", fakes[2].data["k"])
	}
	found2 := false
	for _, a := range meta.Repaired {
		if a == "n2" {
			found2 = true
		}
	}
	if !found2 {
		t.Fatalf("expected n2 in repaired list, got %v", meta.Repaired)
	}
}

func TestDeleteConverges(t *testing.T) {
	c, _ := build(3, 3, 2, 2)
	if _, err := c.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	existed, _, err := c.Del([]byte("k"))
	if err != nil || !existed {
		t.Fatalf("del: existed=%v err=%v", existed, err)
	}
	_, found, _, err := c.Get([]byte("k"))
	if err != nil || found {
		t.Fatalf("get after del: found=%v err=%v", found, err)
	}
	rows, _ := c.Scan()
	if len(rows) != 0 {
		t.Fatalf("scan after del returned %d rows, want 0", len(rows))
	}
}

func TestScanMergesByKey(t *testing.T) {
	c, _ := build(4, 3, 2, 2)
	for i := 0; i < 5; i++ {
		k := fmt.Sprintf("key%d", i)
		if _, err := c.Put([]byte(k), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	rows, errs := c.Scan()
	if len(errs) != 0 {
		t.Fatalf("scan errors: %v", errs)
	}
	if len(rows) != 5 {
		t.Fatalf("scan returned %d unique keys, want 5", len(rows))
	}
	for _, row := range rows {
		if row.Replicas != 3 {
			t.Fatalf("key %s reported %d replicas, want 3", row.Key, row.Replicas)
		}
	}
}

func TestAntiEntropyHealsWithoutAClientRead(t *testing.T) {
	c, fakes := build(4, 3, 2, 1) // W=1: a write lands even with a replica down
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

	// Victim misses the second write.
	victim.setDown(true)
	if _, err := c.Put(key, []byte("v2")); err != nil {
		t.Fatal(err)
	}
	freshVer := byAddr[pl[1]].version("k")
	victim.setDown(false)
	if string(victim.data["k"].Value) == "v2" {
		t.Fatal("precondition: victim should still be stale")
	}

	// No Get is issued — the sweep alone must heal the stale replica.
	repaired, errs := c.RepairSweep()
	if len(errs) != 0 {
		t.Fatalf("sweep errors: %v", errs)
	}
	if repaired == 0 {
		t.Fatal("expected the sweep to repair the stale replica")
	}
	if got := victim.data["k"]; got.State != kv.Present || string(got.Value) != "v2" || got.Version != freshVer {
		t.Fatalf("anti-entropy did not heal victim: %+v", got)
	}
}

func TestConsistencyFlagAndClamping(t *testing.T) {
	c, _ := build(3, 3, 2, 2)
	if !c.Consistent() {
		t.Fatal("R+W>RF should be consistent")
	}
	loose, _ := build(3, 3, 1, 1)
	if loose.Consistent() {
		t.Fatal("R+W<=RF should not be consistent")
	}
	// RF clamped to node count; quorums clamped to RF.
	clamped, _ := build(2, 9, 9, 9)
	if clamped.RF() != 2 || clamped.R() != 2 || clamped.W() != 2 {
		t.Fatalf("clamping wrong: RF=%d R=%d W=%d", clamped.RF(), clamped.R(), clamped.W())
	}
}
