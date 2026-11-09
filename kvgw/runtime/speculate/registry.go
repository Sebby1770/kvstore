package speculate

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/Sebby1770/kvstore/kvgw/netstack/rpc"
)

// Registry is the server side: it tracks handlers running for speculative
// executions so a cancel signal can stop them, and remembers cancelled IDs
// so a copy that arrives after its cancel is refused instead of run.
type Registry struct {
	mu      sync.Mutex
	running map[string][]context.CancelFunc
	tomb    map[string]time.Time
	ttl     time.Duration
	gossip  *Gossip
	stats   RegistryStats
}

// RegistryStats counts server-side speculation.
type RegistryStats struct {
	Served    uint64 `json:"served"`
	Cancelled uint64 `json:"cancelled"` // running handlers stopped by a cancel signal
	Refused   uint64 `json:"refused"`   // copies that arrived already cancelled
}

// NewRegistry makes a registry; gossip (optional) receives digest
// datagrams handled by HandleRaw.
func NewRegistry(gossip *Gossip) *Registry {
	return &Registry{running: map[string][]context.CancelFunc{}, tomb: map[string]time.Time{}, ttl: 30 * time.Second, gossip: gossip}
}

// Interceptor makes speculative calls cancellable by execution ID.
func (r *Registry) Interceptor() rpc.Interceptor {
	return func(ctx context.Context, method string, req []byte, next rpc.Handler) ([]byte, error) {
		id := rpc.Metadata(ctx)[MetaID]
		if id == "" {
			return next(ctx, req)
		}
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		r.mu.Lock()
		if _, dead := r.tomb[id]; dead {
			r.stats.Refused++
			r.mu.Unlock()
			return nil, rpc.Errorf(rpc.Canceled, "speculative execution %s already won elsewhere", id)
		}
		r.running[id] = append(r.running[id], cancel)
		r.stats.Served++
		r.mu.Unlock()
		defer func() {
			r.mu.Lock()
			delete(r.running, id)
			r.mu.Unlock()
		}()
		return next(ctx, req)
	}
}

// Cancel stops every handler running for id and refuses later arrivals.
func (r *Registry) Cancel(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now()
	for k, t := range r.tomb {
		if now.Sub(t) > r.ttl {
			delete(r.tomb, k)
		}
	}
	r.tomb[id] = now
	for _, c := range r.running[id] {
		c()
		r.stats.Cancelled++
	}
	delete(r.running, id)
}

// HandleRaw processes a speculation datagram (cancel or gossip).
func (r *Registry) HandleRaw(from string, p []byte) {
	if len(p) < 2 || p[0] != Tag {
		return
	}
	switch p[1] {
	case msgCancel:
		var m cancelMsg
		if json.Unmarshal(p[2:], &m) == nil && m.ID != "" && len(m.ID) <= 64 {
			r.Cancel(m.ID)
		}
	case msgDigests:
		var ds []Digest
		if r.gossip != nil && json.Unmarshal(p[2:], &ds) == nil && len(ds) <= 64 {
			r.gossip.merge(ds)
		}
	}
}

// Stats snapshots the counters.
func (r *Registry) Stats() RegistryStats {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.stats
}
