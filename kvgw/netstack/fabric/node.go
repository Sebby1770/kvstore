package fabric

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Sebby1770/kvstore/kvgw/netstack/endpoint"
)

// Remote names a registered region somewhere on the fabric: the owner's
// address, the region ID and its remote key. Owners hand these out (over
// RPC, gossip, or configuration); holding one is the capability to use the
// region within its permissions.
type Remote struct {
	Addr   string `json:"addr"`
	Region uint16 `json:"region"`
	Key    uint32 `json:"key"`
	Size   uint64 `json:"size"`
}

// Ptr is the global pointer to off in this region.
func (r Remote) Ptr(off uint64) Ptr { return MakePtr(r.Region, off) }

// Options tunes a Node.
type Options struct {
	// Advertise is the address peers use for this node's regions.
	Advertise string
	// Timeout is the first retry interval for a verb (default 20ms); it
	// doubles per attempt up to 500ms.
	Timeout time.Duration
	// Attempts bounds sends per verb (default 8).
	Attempts int
	// DedupEntries bounds remembered responses to non-idempotent verbs
	// (default 4096).
	DedupEntries int
}

// Stats counts fabric activity.
type Stats struct {
	Regions   int    `json:"regions"`
	Served    uint64 `json:"verbs_served"` // executed for peers
	Issued    uint64 `json:"verbs_issued"` // sent to peers
	Local     uint64 `json:"verbs_local"`  // short-circuited to local memory
	Retries   uint64 `json:"retries"`
	DedupHits uint64 `json:"dedup_hits"` // retries answered from the response cache
	Denied    uint64 `json:"denied"`     // bad key, permission or bounds
}

type regEntry struct {
	r    *Region
	key  uint32
	perm Perm
}

type dedupKey struct {
	conn *endpoint.Conn
	id   uint64
}

// Node is one participant in the fabric: it owns registered regions and
// serves verbs on them, and it issues verbs against other nodes' regions.
type Node struct {
	ep   *endpoint.Endpoint
	opts Options

	mu      sync.RWMutex
	regions map[uint16]*regEntry
	nextID  uint16

	dmu   sync.Mutex
	dedup map[dedupKey]response
	dring []dedupKey
	dpos  int

	pmu     sync.Mutex
	pending map[uint64]chan response
	nextReq atomic.Uint64

	served, issued, local, retries, dedupHits, denied atomic.Uint64
}

// New attaches a fabric node to an endpoint. ep may be nil for a node that
// only works on local (or mapped) memory.
func New(ep *endpoint.Endpoint, opts Options) *Node {
	if opts.Timeout <= 0 {
		opts.Timeout = 20 * time.Millisecond
	}
	if opts.Attempts <= 0 {
		opts.Attempts = 8
	}
	if opts.DedupEntries <= 0 {
		opts.DedupEntries = 4096
	}
	n := &Node{ep: ep, opts: opts, regions: map[uint16]*regEntry{}, nextID: 1,
		dedup: map[dedupKey]response{}, dring: make([]dedupKey, opts.DedupEntries), pending: map[uint64]chan response{}}
	var seed [8]byte
	rand.Read(seed[:])
	n.nextReq.Store(binary.LittleEndian.Uint64(seed[:]) >> 1)
	if ep != nil {
		ep.HandleDatagrams(TagRequest, n.serve)
		ep.HandleDatagrams(TagResponse, n.complete)
	}
	return n
}

// Close detaches the node from its endpoint.
func (n *Node) Close() {
	if n.ep != nil {
		n.ep.HandleDatagrams(TagRequest, nil)
		n.ep.HandleDatagrams(TagResponse, nil)
	}
}

// Register makes r available to peers with the given permissions and
// returns the handle they need. The remote key is random, so a handle
// cannot be guessed from the region ID.
func (n *Node) Register(r *Region, perm Perm) Remote {
	var kb [4]byte
	rand.Read(kb[:])
	key := binary.LittleEndian.Uint32(kb[:]) | 1
	n.mu.Lock()
	defer n.mu.Unlock()
	id := n.nextID
	for n.regions[id] != nil || id == 0 {
		id++
	}
	n.nextID = id + 1
	n.regions[id] = &regEntry{r: r, key: key, perm: perm}
	return Remote{Addr: n.opts.Advertise, Region: id, Key: key, Size: r.Size()}
}

// Deregister revokes a region; verbs naming it fail with ErrKey.
func (n *Node) Deregister(id uint16) {
	n.mu.Lock()
	delete(n.regions, id)
	n.mu.Unlock()
}

// Stats snapshots the counters.
func (n *Node) Stats() Stats {
	n.mu.RLock()
	regions := len(n.regions)
	n.mu.RUnlock()
	return Stats{Regions: regions, Served: n.served.Load(), Issued: n.issued.Load(), Local: n.local.Load(),
		Retries: n.retries.Load(), DedupHits: n.dedupHits.Load(), Denied: n.denied.Load()}
}

func (n *Node) lookup(id uint16, key uint32) (*regEntry, bool) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	e := n.regions[id]
	if e == nil || e.key != key {
		return nil, false
	}
	return e, true
}

// serve executes a verb request in the endpoint's packet path.
func (n *Node) serve(c *endpoint.Conn, p []byte) []byte {
	if len(p) < reqHeader {
		return nil // too short to even address a reply
	}
	q := request(p)
	dk := dedupKey{c, q.id()}
	if !q.idempotent() {
		n.dmu.Lock()
		if r, ok := n.dedup[dk]; ok {
			n.dmu.Unlock()
			n.dedupHits.Add(1)
			return r
		}
		n.dmu.Unlock()
	}
	n.served.Add(1)
	var r response
	if e, ok := n.lookup(q.region(), q.rkey()); !ok {
		r = newResponse(q, stKey, 0)
	} else {
		r = execute(q, e.r, e.perm)
	}
	if r.status() != stOK {
		n.denied.Add(1)
	}
	if !q.idempotent() {
		n.dmu.Lock()
		if old := n.dring[n.dpos]; old.conn != nil {
			delete(n.dedup, old)
		}
		n.dring[n.dpos] = dk
		n.dpos = (n.dpos + 1) % len(n.dring)
		n.dedup[dk] = r
		n.dmu.Unlock()
	}
	return r
}

// complete hands a response to the verb waiting for it.
func (n *Node) complete(_ *endpoint.Conn, p []byte) []byte {
	if len(p) < respHeader {
		return nil
	}
	r := response(append([]byte(nil), p...))
	n.pmu.Lock()
	ch := n.pending[r.id()]
	n.pmu.Unlock()
	if ch != nil {
		select {
		case ch <- r:
		default: // a duplicate of a response already delivered
		}
	}
	return nil
}

// isLocal reports whether rr names one of this node's own regions.
func (n *Node) isLocal(rr Remote) bool {
	return rr.Addr == "" || rr.Addr == n.opts.Advertise
}

// do issues one verb and waits for its response, retrying on silence.
func (n *Node) do(ctx context.Context, rr Remote, q request) (response, error) {
	if n.isLocal(rr) {
		n.local.Add(1)
		e, ok := n.lookup(rr.Region, rr.Key)
		if !ok {
			return nil, ErrKey
		}
		r := execute(q, e.r, e.perm)
		return r, r.err()
	}
	if n.ep == nil {
		return nil, errors.New("fabric: node has no endpoint for remote verbs")
	}
	conn, err := n.ep.Conn(ctx, rr.Addr)
	if err != nil {
		return nil, err
	}
	id := n.nextReq.Add(1)
	q.setID(id)
	ch := make(chan response, 1)
	n.pmu.Lock()
	n.pending[id] = ch
	n.pmu.Unlock()
	defer func() {
		n.pmu.Lock()
		delete(n.pending, id)
		n.pmu.Unlock()
	}()
	wait := n.opts.Timeout
	t := time.NewTimer(wait)
	defer t.Stop()
	for attempt := 0; attempt < n.opts.Attempts; attempt++ {
		if attempt > 0 {
			n.retries.Add(1)
		}
		n.issued.Add(1)
		if err := conn.SendDatagram(q); err != nil {
			return nil, err
		}
		select {
		case r := <-ch:
			return r, r.err()
		case <-t.C:
			wait = min(wait*2, 500*time.Millisecond)
			t.Reset(wait)
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-conn.Done():
			return nil, conn.Err()
		}
	}
	return nil, ErrTimeout
}

// Read copies n bytes at off in rr. Transfers larger than MaxTransfer are
// split into verbs issued concurrently.
func (n *Node) Read(ctx context.Context, rr Remote, off uint64, size int) ([]byte, error) {
	out := make([]byte, size)
	err := n.split(size, func(lo, hi int) error {
		q := newRequest(OpRead, rr, off+uint64(lo), 0)
		le.PutUint32(q[24:], uint32(hi-lo))
		r, err := n.do(ctx, rr, q)
		if err != nil {
			return err
		}
		if len(r.data()) != hi-lo {
			return ErrMalformed
		}
		copy(out[lo:hi], r.data())
		return nil
	})
	return out, err
}

// Write copies data to off in rr.
func (n *Node) Write(ctx context.Context, rr Remote, off uint64, data []byte) error {
	return n.split(len(data), func(lo, hi int) error {
		q := newRequest(OpWrite, rr, off+uint64(lo), hi-lo)
		le.PutUint32(q[24:], uint32(hi-lo))
		copy(q.payload(), data[lo:hi])
		_, err := n.do(ctx, rr, q)
		return err
	})
}

func (n *Node) split(size int, f func(lo, hi int) error) error {
	if size <= MaxTransfer {
		return f(0, size)
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	var first error
	for lo := 0; lo < size; lo += MaxTransfer {
		hi := min(lo+MaxTransfer, size)
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := f(lo, hi); err != nil {
				mu.Lock()
				if first == nil {
					first = err
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	return first
}

// Load64 reads the word at off (8-byte aligned).
func (n *Node) Load64(ctx context.Context, rr Remote, off uint64) (uint64, error) {
	b, err := n.Read(ctx, rr, off, 8)
	if err != nil {
		return 0, err
	}
	return le.Uint64(b), nil
}

// CAS atomically replaces old with new at off; it returns the value found.
func (n *Node) CAS(ctx context.Context, rr Remote, off, old, new uint64) (prev uint64, swapped bool, err error) {
	q := newRequest(OpCAS, rr, off, 0)
	le.PutUint64(q[40:], old)
	le.PutUint64(q[48:], new)
	r, err := n.do(ctx, rr, q)
	if err != nil {
		return 0, false, err
	}
	return r.value(), r.flags()&1 != 0, nil
}

// FetchAdd atomically adds delta at off and returns the previous value.
// Retries are answered from the owner's response cache, so a lost reply
// never applies the addition twice.
func (n *Node) FetchAdd(ctx context.Context, rr Remote, off, delta uint64) (uint64, error) {
	q := newRequest(OpFetchAdd, rr, off, 0)
	le.PutUint64(q[40:], delta)
	r, err := n.do(ctx, rr, q)
	if err != nil {
		return 0, err
	}
	return r.value(), nil
}

// Chase walks a linked structure on the owner, starting at node offset
// start, and returns where it ended -- one round trip however many hops.
func (n *Node) Chase(ctx context.Context, rr Remote, start uint64, s ChaseSpec) (ChaseResult, error) {
	if s.ReadLen > MaxTransfer {
		return ChaseResult{}, ErrTooLarge
	}
	if s.MaxHops == 0 {
		s.MaxHops = 1
	}
	q := newRequest(OpChase, rr, start, 0)
	le.PutUint32(q[24:], s.MaxHops)
	le.PutUint32(q[28:], s.ReadLen)
	le.PutUint32(q[32:], s.LinkOff)
	le.PutUint32(q[36:], s.KeyOff)
	le.PutUint64(q[40:], s.Key)
	if s.Match {
		le.PutUint64(q[48:], 1)
	}
	r, err := n.do(ctx, rr, q)
	if err != nil {
		return ChaseResult{}, err
	}
	return ChaseResult{Off: r.value(), Hops: r.hops(), Found: r.flags()&2 != 0, Data: append([]byte(nil), r.data()...)}, nil
}
