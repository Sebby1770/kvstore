package speculate

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/Sebby1770/kvstore/kvgw/netstack/rpc"
)

// MetaID is the RPC metadata key carrying a speculative execution's ID.
const MetaID = "spec-id"

// Task runs one copy of the work on the node at addr. It must honour ctx
// (cancelled when another copy wins) and pass it to the RPC it makes, so the
// execution ID travels with the call.
type Task func(ctx context.Context, addr string) ([]byte, error)

// Config tunes an Executor.
type Config struct {
	MaxCopies     int           // copies at most, primary included (default 2)
	HedgeQuantile float64       // hedge after this quantile of the primary's latency (default 0.95)
	MinHedgeDelay time.Duration // never hedge sooner (default 2ms)
	BudgetRatio   float64       // hedges allowed per primary call (default 0.1)
	BudgetBurst   float64       // hedge budget burst (default 10)
}

func (c *Config) defaults() {
	if c.MaxCopies <= 0 {
		c.MaxCopies = 2
	}
	if c.HedgeQuantile <= 0 || c.HedgeQuantile >= 1 {
		c.HedgeQuantile = 0.95
	}
	if c.MinHedgeDelay <= 0 {
		c.MinHedgeDelay = 2 * time.Millisecond
	}
	if c.BudgetRatio <= 0 {
		c.BudgetRatio = 0.1
	}
	if c.BudgetBurst <= 0 {
		c.BudgetBurst = 10
	}
}

// Stats counts speculation.
type Stats struct {
	Calls     uint64 `json:"calls"`
	Hedges    uint64 `json:"hedges"`     // speculative copies launched
	Failovers uint64 `json:"failovers"`  // copies launched because one failed
	HedgeWins uint64 `json:"hedge_wins"` // calls a speculative copy won
	Cancels   uint64 `json:"cancels"`    // cancel signals sent
	Denied    uint64 `json:"denied"`     // hedges the budget refused
}

// Outcome describes a finished call.
type Outcome struct {
	Value   []byte
	Winner  string
	Copies  int
	Latency time.Duration
	ID      string
}

// Executor runs speculative calls.
type Executor struct {
	cfg    Config
	gossip *Gossip
	send   func(addr string, payload []byte) error
	load   func(addr string) (permille, queue uint64, ok bool)

	mu     sync.Mutex
	lat    map[string]*window
	tokens float64
	stats  Stats
}

// NewExecutor makes an executor. gossip (optional) supplies load digests;
// send (optional) delivers cancel datagrams; load (optional) reports a
// peer's live pressure (e.g. rpc.Client.Pressure).
func NewExecutor(cfg Config, gossip *Gossip, send func(addr string, payload []byte) error,
	load func(addr string) (uint64, uint64, bool)) *Executor {
	cfg.defaults()
	return &Executor{cfg: cfg, gossip: gossip, send: send, load: load, lat: map[string]*window{}, tokens: cfg.BudgetBurst}
}

// window is a peer's recent latencies.
type window struct {
	v   [64]time.Duration
	n   int
	pos int
}

func (w *window) add(d time.Duration) {
	w.v[w.pos] = d
	w.pos = (w.pos + 1) % len(w.v)
	if w.n < len(w.v) {
		w.n++
	}
}

func (w *window) quantile(q float64) time.Duration {
	if w.n == 0 {
		return 0
	}
	s := append([]time.Duration(nil), w.v[:w.n]...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	return s[min(int(q*float64(w.n)), w.n-1)]
}

// Quantile reports addr's observed latency quantile.
func (e *Executor) Quantile(addr string, q float64) (time.Duration, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	w := e.lat[addr]
	if w == nil || w.n == 0 {
		return 0, false
	}
	return w.quantile(q), true
}

func (e *Executor) observe(addr string, d time.Duration) {
	e.mu.Lock()
	defer e.mu.Unlock()
	w := e.lat[addr]
	if w == nil {
		w = &window{}
		e.lat[addr] = w
	}
	w.add(d)
}

// expected is the ranking score: predicted latency for a copy on addr.
func (e *Executor) expected(addr string) time.Duration {
	e.mu.Lock()
	var p50 time.Duration
	if w := e.lat[addr]; w != nil && w.n >= 5 {
		p50 = w.quantile(0.5)
	}
	e.mu.Unlock()
	var load, queue uint64
	if e.gossip != nil {
		if d, ok := e.gossip.Get(addr); ok {
			if p50 == 0 {
				p50 = d.P50
			}
			load, queue = d.Load, d.Queue
		}
	}
	if e.load != nil {
		if l, q, ok := e.load(addr); ok {
			load, queue = max(load, l), max(queue, q)
		}
	}
	if p50 == 0 {
		p50 = time.Millisecond // unknown: optimistic, so it gets explored
	}
	return time.Duration(float64(p50) * (1 + 2*float64(load)/1000) * (1 + 0.5*float64(queue)))
}

// Rank orders candidates by expected latency (stable for ties).
func (e *Executor) Rank(candidates []string) []string {
	out := append([]string(nil), candidates...)
	score := make(map[string]time.Duration, len(out))
	for _, c := range out {
		score[c] = e.expected(c)
	}
	sort.SliceStable(out, func(i, j int) bool { return score[out[i]] < score[out[j]] })
	return out
}

func (e *Executor) hedgeDelay(addr string) time.Duration {
	if q, ok := e.Quantile(addr, e.cfg.HedgeQuantile); ok {
		return max(q, e.cfg.MinHedgeDelay)
	}
	if e.gossip != nil {
		if d, ok := e.gossip.Get(addr); ok && d.P99 > 0 {
			return max(d.P99, e.cfg.MinHedgeDelay)
		}
	}
	return max(10*time.Millisecond, e.cfg.MinHedgeDelay)
}

func (e *Executor) takeBudget() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.tokens >= 1 {
		e.tokens--
		return true
	}
	e.stats.Denied++
	return false
}

// Stats snapshots the counters.
func (e *Executor) Stats() Stats {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.stats
}

func newID() string {
	var b [8]byte
	rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

type result struct {
	addr  string
	val   []byte
	err   error
	took  time.Duration
	index int
}

// ErrNoCandidates reports a call with nowhere to run.
var ErrNoCandidates = errors.New("speculate: no candidates")

// Do runs task speculatively across candidates and returns the first
// successful result.
func (e *Executor) Do(ctx context.Context, candidates []string, task Task) (Outcome, error) {
	if len(candidates) == 0 {
		return Outcome{}, ErrNoCandidates
	}
	order := e.Rank(candidates)
	id := newID()
	e.mu.Lock()
	e.stats.Calls++
	e.tokens = min(e.tokens+e.cfg.BudgetRatio, e.cfg.BudgetBurst)
	e.mu.Unlock()

	start := time.Now()
	results := make(chan result, len(order))
	cancels := make([]context.CancelFunc, 0, len(order))
	launched := []string{}
	launch := func() {
		i := len(launched)
		addr := order[i]
		cctx, cancel := context.WithCancel(rpc.WithMetadata(ctx, MetaID, id))
		cancels = append(cancels, cancel)
		launched = append(launched, addr)
		go func() {
			t0 := time.Now()
			v, err := task(cctx, addr)
			results <- result{addr: addr, val: v, err: err, took: time.Since(t0), index: i}
		}()
	}
	finish := func(winner string) {
		for _, c := range cancels {
			c()
		}
		var losers []string
		for _, a := range launched {
			if a != winner {
				losers = append(losers, a)
			}
		}
		e.broadcastCancel(id, losers)
	}

	launch()
	timer := time.NewTimer(e.hedgeDelay(order[0]))
	defer timer.Stop()
	pending := 1
	var lastErr error
	for {
		select {
		case r := <-results:
			pending--
			if r.err == nil {
				e.observe(r.addr, r.took)
				finish(r.addr)
				if r.index > 0 {
					e.mu.Lock()
					e.stats.HedgeWins++
					e.mu.Unlock()
				}
				return Outcome{Value: r.val, Winner: r.addr, Copies: len(launched), Latency: time.Since(start), ID: id}, nil
			}
			lastErr = r.err
			if ctx.Err() != nil {
				finish("")
				return Outcome{}, ctx.Err()
			}
			// A failed copy is replaced at once, budget or not: this is
			// failover, not speculation.
			if len(launched) < len(order) {
				e.mu.Lock()
				e.stats.Failovers++
				e.mu.Unlock()
				launch()
				pending++
			} else if pending == 0 {
				finish("")
				return Outcome{Copies: len(launched), ID: id}, lastErr
			}
		case <-timer.C:
			if len(launched) < min(e.cfg.MaxCopies, len(order)) {
				if e.takeBudget() {
					e.mu.Lock()
					e.stats.Hedges++
					e.mu.Unlock()
					launch()
					pending++
				}
				if len(launched) < len(order) {
					timer.Reset(e.hedgeDelay(order[len(launched)-1]))
				}
			}
		case <-ctx.Done():
			finish("")
			return Outcome{}, ctx.Err()
		}
	}
}

func (e *Executor) broadcastCancel(id string, addrs []string) {
	if e.send == nil || len(addrs) == 0 {
		return
	}
	b, _ := json.Marshal(cancelMsg{ID: id})
	payload := append([]byte{Tag, msgCancel}, b...)
	for _, a := range addrs {
		if e.send(a, payload) == nil {
			e.mu.Lock()
			e.stats.Cancels++
			e.mu.Unlock()
		}
	}
}

type cancelMsg struct {
	ID string `json:"id"`
}
