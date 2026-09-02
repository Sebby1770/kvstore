// Package cluster is the quorum coordinator: the actual distributed-systems
// policy that turns a bag of dumb storage nodes into a fault-tolerant store.
//
// For each key the rendezvous ring yields a preference list of RF replicas.
//   - Writes go to all RF replicas and need W acks to succeed.
//   - Reads query all RF replicas, need R responses, and return the value with
//     the highest version. Stale replicas are repaired in the background of the
//     read (read-repair), so the cluster heals as it's used.
//   - A replica that misses a write gets a hint, replayed when it is reachable
//     again (hinted handoff). Heartbeats mark nodes down without removing them
//     from the ring, so preference lists stay stable.
//
// With R + W > RF every read overlaps every write on at least one replica, so a
// successful read always observes the latest successful write.
package cluster

import (
	"errors"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Sebby1770/kvstore/kvgw/internal/kv"
	"github.com/Sebby1770/kvstore/kvgw/internal/ring"
)

// ErrQuorum means not enough replicas responded to satisfy R (reads) or W
// (writes). The operation may have partially applied.
var ErrQuorum = errors.New("quorum not reached")

// ErrConflict means a compare-and-set observed a different version than the
// caller expected (or the key existed when expectedVersion was 0).
var ErrConflict = errors.New("version conflict")

// errDownSentinel is used internally when a write is skipped because the
// replica is already known-down (a hint is stored instead of waiting).
var errDownSentinel = errors.New("replica known down")

// membership is an immutable snapshot of the cluster's nodes and their ring.
// Changing membership swaps in a brand-new snapshot atomically, so readers never
// see a half-updated ring and never need a lock on the hot path.
type membership struct {
	ring  *ring.Ring
	nodes map[string]kv.Node
	addrs []string
}

type hbState struct {
	down   bool
	lastOK time.Time
}

type Cluster struct {
	members atomic.Value // *membership
	rfCfg   int          // configured RF/R/W; effective values clamp to live size
	rCfg    int
	wCfg    int
	clock   lamport
	locks   keyedMutex
	memMu   sync.Mutex // serialises membership changes

	// now, if set, is the wall clock used for TTL. Tests inject a fake.
	now func() time.Time

	hbMu sync.Mutex
	hb   map[string]*hbState

	hintsMu sync.Mutex
	hints   map[string][]hint

	// Operational counters (atomic).
	mGets, mPuts, mDels, mMutations    uint64
	mQuorumFails, mRepairs, mSweepReps uint64
	mHintsStored, mHintsReplayed       uint64
	mCompacts                          uint64
}

// Metrics is a snapshot of the coordinator's operational counters.
type Metrics struct {
	Gets           uint64 `json:"gets"`
	Puts           uint64 `json:"puts"`
	Dels           uint64 `json:"dels"`
	Mutations      uint64 `json:"mutations"`
	QuorumFailures uint64 `json:"quorum_failures"`
	ReadRepairs    uint64 `json:"read_repairs"`
	SweepRepairs   uint64 `json:"sweep_repairs"`
	HintsStored    uint64 `json:"hints_stored"`
	HintsReplayed  uint64 `json:"hints_replayed"`
	Compacts       uint64 `json:"compacts"`
}

func (c *Cluster) Metrics() Metrics {
	return Metrics{
		Gets:           atomic.LoadUint64(&c.mGets),
		Puts:           atomic.LoadUint64(&c.mPuts),
		Dels:           atomic.LoadUint64(&c.mDels),
		Mutations:      atomic.LoadUint64(&c.mMutations),
		QuorumFailures: atomic.LoadUint64(&c.mQuorumFails),
		ReadRepairs:    atomic.LoadUint64(&c.mRepairs),
		SweepRepairs:   atomic.LoadUint64(&c.mSweepReps),
		HintsStored:    atomic.LoadUint64(&c.mHintsStored),
		HintsReplayed:  atomic.LoadUint64(&c.mHintsReplayed),
		Compacts:       atomic.LoadUint64(&c.mCompacts),
	}
}

// keyedMutex hands out a distinct lock per key, so mutations to different keys
// stay concurrent while mutations to the same key serialize.
type keyedMutex struct {
	m sync.Map
}

func (k *keyedMutex) lock(key string) func() {
	v, _ := k.m.LoadOrStore(key, &sync.Mutex{})
	mu := v.(*sync.Mutex)
	mu.Lock()
	return mu.Unlock
}

// New builds a coordinator. rf/r/w are kept as configured; the *effective*
// values always clamp to the live node count, so the cluster keeps working as
// nodes join and leave.
func New(nodes map[string]kv.Node, addrs []string, rf, r, w int) *Cluster {
	c := &Cluster{
		rfCfg: rf, rCfg: r, wCfg: w,
		hb:    make(map[string]*hbState),
		hints: make(map[string][]hint),
	}
	c.setMembership(addrs, nodes)
	return c
}

// SetNow replaces the wall clock used for TTL. Pass nil to restore time.Now.
// Tests use this so expiry is deterministic.
func (c *Cluster) SetNow(now func() time.Time) { c.now = now }

// Now is the coordinator's wall clock (real time unless a test injected one).
func (c *Cluster) Now() time.Time {
	if c.now != nil {
		return c.now()
	}
	return time.Now()
}

func (c *Cluster) snap() *membership { return c.members.Load().(*membership) }

func (c *Cluster) setMembership(addrs []string, nodes map[string]kv.Node) {
	cp := make([]string, len(addrs))
	copy(cp, addrs)
	c.members.Store(&membership{ring: ring.New(cp), nodes: nodes, addrs: cp})
}

func effectiveRF(cfg, n int) int {
	if cfg > n {
		cfg = n
	}
	if cfg < 1 {
		return 1
	}
	return cfg
}

func clampQuorum(q, rf int) int {
	if q < 1 {
		return 1
	}
	if q > rf {
		return rf
	}
	return q
}

func (c *Cluster) RF() int          { return effectiveRF(c.rfCfg, len(c.snap().addrs)) }
func (c *Cluster) R() int           { return clampQuorum(c.rCfg, c.RF()) }
func (c *Cluster) W() int           { return clampQuorum(c.wCfg, c.RF()) }
func (c *Cluster) Addrs() []string  { return c.snap().addrs }
func (c *Cluster) Consistent() bool { return c.R()+c.W() > c.RF() } // R + W > RF

// Route returns the replica set (preference list) for a key.
func (c *Cluster) Route(key []byte) []string {
	m := c.snap()
	return m.ring.Preflist(key, effectiveRF(c.rfCfg, len(m.addrs)))
}

// AddNode brings a node into the cluster at runtime. Rendezvous hashing means
// only ~1/N of keys change replica set; the anti-entropy sweep (or the next
// read) re-replicates them onto the newcomer. Returns false if it already exists.
func (c *Cluster) AddNode(addr string, node kv.Node) bool {
	c.memMu.Lock()
	defer c.memMu.Unlock()
	m := c.snap()
	if _, ok := m.nodes[addr]; ok {
		return false
	}
	addrs := append(append([]string{}, m.addrs...), addr)
	nodes := make(map[string]kv.Node, len(m.nodes)+1)
	for k, v := range m.nodes {
		nodes[k] = v
	}
	nodes[addr] = node
	c.setMembership(addrs, nodes)
	return true
}

// RemoveNode drops a node from the cluster at runtime (won't remove the last
// one). Its keys re-home onto whoever the ring picks next.
func (c *Cluster) RemoveNode(addr string) bool {
	c.memMu.Lock()
	defer c.memMu.Unlock()
	m := c.snap()
	if _, ok := m.nodes[addr]; !ok || len(m.addrs) <= 1 {
		return false
	}
	addrs := make([]string, 0, len(m.addrs)-1)
	for _, a := range m.addrs {
		if a != addr {
			addrs = append(addrs, a)
		}
	}
	nodes := make(map[string]kv.Node, len(m.nodes)-1)
	for k, v := range m.nodes {
		if k != addr {
			nodes[k] = v
		}
	}
	c.setMembership(addrs, nodes)
	c.dropHints(addr)
	c.hbMu.Lock()
	delete(c.hb, addr)
	c.hbMu.Unlock()
	return true
}

func (c *Cluster) replicas(key []byte) ([]string, []kv.Node) {
	m := c.snap()
	addrs := m.ring.Preflist(key, effectiveRF(c.rfCfg, len(m.addrs)))
	out := make([]kv.Node, 0, len(addrs))
	for _, a := range addrs {
		if n, ok := m.nodes[a]; ok {
			out = append(out, n)
		}
	}
	return addrs, out
}

// --------------------------------------------------------------------------- //
// Heartbeat / failure detection. Down nodes stay on the ring (stable
// preference lists); writes skip them and store hints instead of waiting.
// --------------------------------------------------------------------------- //

func (c *Cluster) hbStateLocked(addr string) *hbState {
	st, ok := c.hb[addr]
	if !ok {
		st = &hbState{}
		c.hb[addr] = st
	}
	return st
}

func (c *Cluster) markDown(addr string) {
	c.hbMu.Lock()
	c.hbStateLocked(addr).down = true
	c.hbMu.Unlock()
}

func (c *Cluster) markUp(addr string) {
	c.hbMu.Lock()
	st := c.hbStateLocked(addr)
	st.down = false
	st.lastOK = c.Now()
	c.hbMu.Unlock()
}

// IsDown reports whether the last heartbeat/health probe marked addr unreachable.
func (c *Cluster) IsDown(addr string) bool {
	c.hbMu.Lock()
	defer c.hbMu.Unlock()
	st, ok := c.hb[addr]
	return ok && st.down
}

func (c *Cluster) lastOK(addr string) time.Time {
	c.hbMu.Lock()
	defer c.hbMu.Unlock()
	if st, ok := c.hb[addr]; ok {
		return st.lastOK
	}
	return time.Time{}
}

// Heartbeat pings every member, updates down/last-ok, and replays hints for
// nodes that answer.
func (c *Cluster) Heartbeat() {
	m := c.snap()
	var wg sync.WaitGroup
	for _, addr := range m.addrs {
		n := m.nodes[addr]
		wg.Add(1)
		go func(addr string, n kv.Node) {
			defer wg.Done()
			if n == nil {
				return
			}
			if err := n.Ping(); err != nil {
				c.markDown(addr)
				return
			}
			c.markUp(addr)
			c.replayHints(addr)
		}(addr, n)
	}
	wg.Wait()
}

// RunHeartbeat pings on interval until stop is closed. A zero interval returns.
func (c *Cluster) RunHeartbeat(interval time.Duration, stop <-chan struct{}) {
	if interval <= 0 {
		return
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	c.Heartbeat()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
			c.Heartbeat()
		}
	}
}

// --------------------------------------------------------------------------- //
// Lamport-ish version clock. Every observed version advances it, so each write
// gets a version strictly greater than anything the gateway has seen.
// --------------------------------------------------------------------------- //
type lamport struct {
	mu sync.Mutex
	v  uint64
}

func (l *lamport) witness(v uint64) {
	l.mu.Lock()
	if v > l.v {
		l.v = v
	}
	l.mu.Unlock()
}

func (l *lamport) tick(seen uint64) uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	if seen > l.v {
		l.v = seen
	}
	l.v++
	return l.v
}

// --------------------------------------------------------------------------- //
// Reads
// --------------------------------------------------------------------------- //
type nodeRead struct {
	addr string
	res  kv.ReadResult
	err  error
}

func gatherReads(addrs []string, nodes []kv.Node, key []byte) []nodeRead {
	out := make([]nodeRead, len(nodes))
	var wg sync.WaitGroup
	for i := range nodes {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			res, err := nodes[i].Get(key)
			out[i] = nodeRead{addr: addrs[i], res: res, err: err}
		}(i)
	}
	wg.Wait()
	return out
}

// ReadMeta describes how a read was served (for response headers / debugging).
type ReadMeta struct {
	Replicas   []string
	Responders []string
	Version    uint64
	Repaired   []string
	ExpiresAt  int64 // unix seconds; 0 if the value has no TTL
}

// Get performs a quorum read and returns the freshest value.
func (c *Cluster) Get(key []byte) (value []byte, found bool, meta ReadMeta, err error) {
	atomic.AddUint64(&c.mGets, 1)
	addrs, nodes := c.replicas(key)
	reads := gatherReads(addrs, nodes, key)

	meta.Replicas = addrs
	var best kv.ReadResult
	haveBest := false
	for _, rd := range reads {
		if rd.err != nil {
			continue
		}
		meta.Responders = append(meta.Responders, rd.addr)
		c.clock.witness(rd.res.Version)
		if !haveBest || rd.res.Version > best.Version {
			best = rd.res
			haveBest = true
		}
	}
	meta.Version = best.Version

	if len(meta.Responders) < c.R() {
		atomic.AddUint64(&c.mQuorumFails, 1)
		return nil, false, meta, ErrQuorum
	}

	var decoded []byte
	if haveBest && best.State == kv.Present {
		val, exp, expired := c.unwrapPresent(best)
		if expired {
			best.State = kv.Deleted
			best.Value = nil
		} else {
			decoded = val
			if exp > 0 {
				meta.ExpiresAt = exp
			}
		}
	}

	meta.Repaired = c.readRepair(key, reads, best)
	if n := len(meta.Repaired); n > 0 {
		atomic.AddUint64(&c.mRepairs, uint64(n))
	}

	if best.State == kv.Present {
		return decoded, true, meta, nil
	}
	return nil, false, meta, nil // Missing, Deleted, or expired -> not found
}

// readRepair pushes the winning value to any responding replica that is behind.
func (c *Cluster) readRepair(key []byte, reads []nodeRead, best kv.ReadResult) []string {
	if best.Version == 0 {
		return nil
	}
	var repaired []string
	for _, rd := range reads {
		if rd.err != nil {
			continue
		}
		if rd.res.Version >= best.Version && rd.res.State == best.State {
			continue // already current
		}
		node := c.snap().nodes[rd.addr]
		if node == nil {
			continue // node left the cluster
		}
		var rerr error
		switch best.State {
		case kv.Present:
			rerr = node.PutX(key, best.Value, best.Version, best.ExpiresAt)
		case kv.Deleted:
			_, rerr = node.Del(key, best.Version)
		default:
			continue
		}
		if rerr == nil {
			repaired = append(repaired, rd.addr)
		}
	}
	return repaired
}

// --------------------------------------------------------------------------- //
// Writes
// --------------------------------------------------------------------------- //
type WriteMeta struct {
	Replicas []string
	Acks     int
	Required int
	Version  uint64
}

// peekMaxVersion reads the replica set best-effort to learn the highest version
// in play, so the new write supersedes it.
func (c *Cluster) peekMaxVersion(addrs []string, nodes []kv.Node, key []byte) uint64 {
	var maxv uint64
	for _, rd := range gatherReads(addrs, nodes, key) {
		if rd.err == nil {
			c.clock.witness(rd.res.Version)
			if rd.res.Version > maxv {
				maxv = rd.res.Version
			}
		}
	}
	return maxv
}

// Put performs a quorum write (last-writer-wins by assigned version).
func (c *Cluster) Put(key, val []byte) (WriteMeta, error) {
	return c.putAt(key, val, 0)
}

// PutTTL stores val with a native engine expiry. ttl <= 0 is a normal immortal
// Put. Legacy TGTTL1/KVTTL1 envelopes are still decoded on read.
func (c *Cluster) PutTTL(key, val []byte, ttl time.Duration) (WriteMeta, error) {
	if ttl <= 0 {
		return c.Put(key, val)
	}
	return c.putAt(key, val, uint64(c.Now().Add(ttl).Unix()))
}

func (c *Cluster) putAt(key, val []byte, expiresAt uint64) (WriteMeta, error) {
	atomic.AddUint64(&c.mPuts, 1)
	addrs, nodes := c.replicas(key)
	version := c.clock.tick(c.peekMaxVersion(addrs, nodes, key))
	return c.broadcastPut(addrs, nodes, key, val, version, expiresAt)
}

func (c *Cluster) broadcastPut(addrs []string, nodes []kv.Node, key, val []byte, version, expiresAt uint64) (WriteMeta, error) {
	acks := 0
	var mu sync.Mutex
	var wg sync.WaitGroup
	for i := range nodes {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if err := putOrHint(c, addrs[i], nodes[i], key, val, version, expiresAt); err == nil {
				mu.Lock()
				acks++
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()

	meta := WriteMeta{Replicas: addrs, Acks: acks, Required: c.W(), Version: version}
	if acks < c.W() {
		atomic.AddUint64(&c.mQuorumFails, 1)
		return meta, ErrQuorum
	}
	return meta, nil
}

// Del tombstones a key across its replica set. `existed` is true if any replica
// reported the key was present before the delete.
func (c *Cluster) Del(key []byte) (existed bool, meta WriteMeta, err error) {
	atomic.AddUint64(&c.mDels, 1)
	addrs, nodes := c.replicas(key)
	version := c.clock.tick(c.peekMaxVersion(addrs, nodes, key))

	acks := 0
	anyExisted := false
	var mu sync.Mutex
	var wg sync.WaitGroup
	for i := range nodes {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			was, err := delOrHint(c, addrs[i], nodes[i], key, version)
			if err == nil {
				mu.Lock()
				acks++
				if was {
					anyExisted = true
				}
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()

	meta = WriteMeta{Replicas: addrs, Acks: acks, Required: c.W(), Version: version}
	if acks < c.W() {
		atomic.AddUint64(&c.mQuorumFails, 1)
		return anyExisted, meta, ErrQuorum
	}
	return anyExisted, meta, nil
}

// Mutate runs a read-merge-write transaction under a per-key lock — the basis
// for CRDT values. It gathers every reachable replica's current value, hands
// them to apply (which merges them, e.g. a CRDT join, and returns the new
// value), then writes the result back with a fresh version. The lock makes
// concurrent mutations on the same key safe for a single coordinator (no lost
// updates); reading *all* replicas first recovers state from any that diverged.
// apply should be associative/commutative/idempotent so retries and reordering
// stay correct.
func (c *Cluster) Mutate(key []byte, apply func(states [][]byte) ([]byte, error)) ([]byte, WriteMeta, error) {
	atomic.AddUint64(&c.mMutations, 1)
	unlock := c.locks.lock(string(key))
	defer unlock()

	addrs, nodes := c.replicas(key)
	reads := gatherReads(addrs, nodes, key)

	var states [][]byte
	responders := 0
	var maxSeen uint64
	for _, rd := range reads {
		if rd.err != nil {
			continue
		}
		responders++
		if rd.res.Version > maxSeen {
			maxSeen = rd.res.Version
		}
		if rd.res.State == kv.Present && !c.resultExpired(rd.res) {
			states = append(states, rd.res.Value)
		}
	}
	if responders < c.R() {
		atomic.AddUint64(&c.mQuorumFails, 1)
		return nil, WriteMeta{Replicas: addrs, Required: c.W()}, ErrQuorum
	}

	newVal, err := apply(states)
	if err != nil {
		return nil, WriteMeta{}, err
	}

	version := c.clock.tick(maxSeen)
	meta, err := c.broadcastPut(addrs, nodes, key, newVal, version, 0)
	return newVal, meta, err
}

// --------------------------------------------------------------------------- //
// Cluster-wide views
// --------------------------------------------------------------------------- //
type ScanRow struct {
	Key       string `json:"key"`
	Value     string `json:"value"`
	Version   uint64 `json:"version"`
	Replicas  int    `json:"replicas"`
	ExpiresAt *int64 `json:"expires_at,omitempty"`
}

type mergedRow struct {
	raw       []byte
	version   uint64
	expiresAt uint64
	replicas  int
	deleted   bool
}

// Scan fans out to every node and merges by key, keeping the newest version.
// Expired TTL envelopes / native expiries are omitted and values are decoded.
func (c *Cluster) Scan() ([]ScanRow, []string) {
	return c.ScanPrefix("")
}

// ScanPrefix is Scan filtered to keys that start with prefix (empty = all).
func (c *Cluster) ScanPrefix(prefix string) ([]ScanRow, []string) {
	merged, errs := c.mergeScan(false)
	rows := make([]ScanRow, 0, len(merged))
	for k, a := range merged {
		if prefix != "" && !strings.HasPrefix(k, prefix) {
			continue
		}
		if a.deleted {
			continue
		}
		fake := kv.ReadResult{State: kv.Present, Value: a.raw, Version: a.version, ExpiresAt: a.expiresAt}
		val, exp, expired := c.unwrapPresent(fake)
		if expired {
			continue
		}
		row := ScanRow{Key: k, Value: string(val), Version: a.version, Replicas: a.replicas}
		if exp > 0 {
			e := exp
			row.ExpiresAt = &e
		}
		rows = append(rows, row)
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Key < rows[j].Key })
	return rows, errs
}

func (c *Cluster) mergeScan(includeTombs bool) (map[string]*mergedRow, []string) {
	m := c.snap()
	addrs := m.addrs
	type res struct {
		entries []kv.Entry
		err     error
	}
	results := make([]res, len(addrs))
	var wg sync.WaitGroup
	for i, a := range addrs {
		wg.Add(1)
		go func(i int, a string) {
			defer wg.Done()
			var es []kv.Entry
			var err error
			if includeTombs {
				es, err = m.nodes[a].ScanAll()
			} else {
				es, err = m.nodes[a].Scan()
			}
			results[i] = res{entries: es, err: err}
		}(i, a)
	}
	wg.Wait()

	merged := map[string]*mergedRow{}
	var errs []string
	for i, r := range results {
		if r.err != nil {
			errs = append(errs, addrs[i]+": "+r.err.Error())
			continue
		}
		for _, e := range r.entries {
			k := string(e.Key)
			a, ok := merged[k]
			if !ok {
				merged[k] = &mergedRow{
					raw:       append([]byte(nil), e.Value...),
					version:   e.Version,
					expiresAt: e.ExpiresAt,
					replicas:  1,
					deleted:   e.Tombstone,
				}
				continue
			}
			a.replicas++
			if e.Version > a.version {
				a.version = e.Version
				a.raw = append([]byte(nil), e.Value...)
				a.expiresAt = e.ExpiresAt
				a.deleted = e.Tombstone
			}
		}
	}
	return merged, errs
}

// RepairSweep proactively re-replicates every key's freshest value (and
// tombstones, via ScanAll) to the replicas that are behind — healing without
// waiting for a client read.
func (c *Cluster) RepairSweep() (int, []string) {
	merged, errs := c.mergeScan(true)
	repaired := 0
	for k, row := range merged {
		key := []byte(k)
		if !row.deleted {
			fake := kv.ReadResult{State: kv.Present, Value: row.raw, Version: row.version, ExpiresAt: row.expiresAt}
			if _, _, expired := c.unwrapPresent(fake); expired {
				if _, _, err := c.Del(key); err == nil {
					repaired++
				}
				continue
			}
		}
		addrs, nodes := c.replicas(key)
		reads := gatherReads(addrs, nodes, key)

		best := kv.ReadResult{Version: row.version, ExpiresAt: row.expiresAt, Value: row.raw}
		if row.deleted {
			best.State = kv.Deleted
		} else {
			best.State = kv.Present
		}
		for _, rd := range reads {
			if rd.err == nil && rd.res.Version > best.Version {
				best = rd.res
			}
		}
		if best.State == kv.Present {
			if _, _, expired := c.unwrapPresent(best); expired {
				if _, _, err := c.Del(key); err == nil {
					repaired++
				}
				continue
			}
		}
		for _, rd := range reads {
			if rd.err != nil {
				continue
			}
			if rd.res.Version >= best.Version && rd.res.State == best.State {
				continue
			}
			node := c.snap().nodes[rd.addr]
			if node == nil {
				continue
			}
			var err error
			switch best.State {
			case kv.Present:
				err = node.PutX(key, best.Value, best.Version, best.ExpiresAt)
			case kv.Deleted:
				_, err = node.Del(key, best.Version)
			default:
				continue
			}
			if err == nil {
				repaired++
			} else if best.State == kv.Present {
				c.storeHint(rd.addr, hint{op: hintPut, key: key, value: best.Value, version: best.Version, expires: best.ExpiresAt})
			} else {
				c.storeHint(rd.addr, hint{op: hintDel, key: key, version: best.Version})
			}
		}
	}
	if repaired > 0 {
		atomic.AddUint64(&c.mSweepReps, uint64(repaired))
	}
	return repaired, errs
}

type NodeHealth struct {
	Addr         string     `json:"addr"`
	Up           bool       `json:"up"`
	Down         bool       `json:"down"`
	LastOK       *time.Time `json:"last_ok,omitempty"`
	Err          string     `json:"err,omitempty"`
	Count        uint32     `json:"count"`
	MaxVersion   uint64     `json:"max_version"`
	Tombstones   uint32     `json:"tombstones,omitempty"`
	Capacity     uint32     `json:"capacity,omitempty"`
	Load         float64    `json:"load,omitempty"`
	HintsPending int        `json:"hints_pending"`
}

// Health pings every node and pulls its stats. Also refreshes heartbeat state.
func (c *Cluster) Health() ([]NodeHealth, bool) {
	m := c.snap()
	addrs := m.addrs
	out := make([]NodeHealth, len(addrs))
	var wg sync.WaitGroup
	for i, a := range addrs {
		wg.Add(1)
		go func(i int, a string) {
			defer wg.Done()
			h := NodeHealth{Addr: a}
			if st, err := m.nodes[a].Stat(); err != nil {
				h.Err = err.Error()
				c.markDown(a)
				h.Down = true
			} else {
				c.markUp(a)
				h.Up = true
				h.Count = st.Count
				h.MaxVersion = st.MaxVersion
				h.Tombstones = st.Tombstones
				if st.Capacity > 0 {
					h.Capacity = st.Capacity
					h.Load = float64(st.Count) / float64(st.Capacity)
				}
			}
			if t := c.lastOK(a); !t.IsZero() {
				tcopy := t
				h.LastOK = &tcopy
			}
			h.Down = c.IsDown(a)
			out[i] = h
		}(i, a)
	}
	wg.Wait()

	pending := c.HintsPending()
	allUp := true
	for i, h := range out {
		out[i].HintsPending = pending[h.Addr]
		if !h.Up || h.Down {
			allUp = false
		}
	}
	return out, allUp
}

// HintsPending is the number of queued (not yet replayed) hints per node.
func (c *Cluster) HintsPending() map[string]int {
	c.hintsMu.Lock()
	defer c.hintsMu.Unlock()
	out := make(map[string]int, len(c.hints))
	for addr, q := range c.hints {
		if n := len(q); n > 0 {
			out[addr] = n
		}
	}
	return out
}

// CompactAll best-effort rehashes every live node at its current capacity
// so tombstones and expired keys are dropped. Down nodes are skipped.
func (c *Cluster) CompactAll() (int, []string) {
	atomic.AddUint64(&c.mCompacts, 1)
	m := c.snap()
	var (
		mu   sync.Mutex
		wg   sync.WaitGroup
		n    int
		errs []string
	)
	for _, addr := range m.addrs {
		if c.IsDown(addr) {
			continue
		}
		node := m.nodes[addr]
		if node == nil {
			continue
		}
		wg.Add(1)
		go func(addr string, node kv.Node) {
			defer wg.Done()
			if err := node.Compact(); err != nil {
				mu.Lock()
				errs = append(errs, addr+": "+err.Error())
				mu.Unlock()
				return
			}
			mu.Lock()
			n++
			mu.Unlock()
		}(addr, node)
	}
	wg.Wait()
	sort.Strings(errs)
	return n, errs
}
