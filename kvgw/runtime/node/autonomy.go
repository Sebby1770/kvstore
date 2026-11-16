package node

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Sebby1770/kvstore/kvgw/netstack/fabric"
	"github.com/Sebby1770/kvstore/kvgw/netstack/offload"
	"github.com/Sebby1770/kvstore/kvgw/netstack/rpc"
	"github.com/Sebby1770/kvstore/kvgw/runtime/causal"
	"github.com/Sebby1770/kvstore/kvgw/runtime/crdt"
	"github.com/Sebby1770/kvstore/kvgw/runtime/foresight"
	"github.com/Sebby1770/kvstore/kvgw/runtime/speculate"
	"github.com/Sebby1770/kvstore/kvgw/runtime/swim"
	"github.com/Sebby1770/kvstore/kvgw/runtime/vtime"
)

// autonomy wires the self-managing layers into the node:
//
//	data path   offload hook on the endpoint: heartbeat responder (always), rate limiter and
//	            circuit breaker (opt-in); fabric verbs served in the packet path
//	time        PTP-style discipline of the node clock to an elected grandmaster; the HLC
//	            reads the disciplined clock
//	state       CRDT store replicated by causal broadcast over raw datagrams
//	health      foresight: heartbeat probes + transport telemetry -> failure forecasts, which
//	            steer actor placement and make a leader hand over before it is cut off
//	latency     speculative execution with gossiped load digests (/spec/kv)
//	load        RPC worker pool with application backpressure (APP_PRESSURE)
type autonomy struct {
	n *Node

	hook    *offload.Hook
	hb      *offload.Heartbeat
	prober  *offload.Prober
	limiter *offload.RateLimiter
	breaker *offload.CircuitBreaker

	vclock   *vtime.Clock
	tmu      sync.Mutex
	gm       string // grandmaster address
	epoch    uint32
	master   *vtime.Master
	follower *vtime.Follower

	cmu          sync.Mutex
	causal       *causal.Node
	store        *crdt.Store
	bootstrapped bool

	fmu          sync.Mutex
	detector     *foresight.Detector
	lastTransfer time.Time

	gossip   *speculate.Gossip
	registry *speculate.Registry
	spec     *speculate.Executor

	fab      *fabric.Node
	index    *fabric.Index
	indexRR  fabric.Remote
	indexMem *fabric.Region
}

// Raw-datagram tags (SWIM keeps the low ones).
const (
	tagPTP    = vtime.TagPTP
	tagCausal = causal.Tag
	tagSpec   = speculate.Tag
)

func epochOf(name string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(name))
	return h.Sum32() | 1
}

// setupAutonomy runs after the endpoint and RPC exist and before SWIM
// starts (SWIM gossips the fabric index handle).
func (n *Node) setupAutonomy() error {
	a := &autonomy{n: n, vclock: n.vclock}
	n.auto = a
	ep := n.ep

	// Data path.
	a.hook = offload.NewHook()
	hb, err := offload.NewHeartbeat(n.cfg.ClusterKey)
	if err != nil {
		return err
	}
	a.hb = hb
	a.hook.AttachIngress(hb.Prog)
	if n.cfg.RateLimit > 0 {
		share := offload.Share(n.cfg.RateLimit, 1)
		if a.limiter, err = offload.NewRateLimiter(share, max(share/4, 16)); err != nil {
			return err
		}
		a.hook.AttachIngress(a.limiter.Prog)
	}
	if n.cfg.CircuitBreaker {
		if a.breaker, err = offload.NewCircuitBreaker(5, 2*time.Second); err != nil {
			return err
		}
		a.hook.AttachIngress(a.breaker.Ingress)
		a.hook.AttachEgress(a.breaker.Egress)
	}
	ep.SetFilter(a.hook)
	a.prober = offload.NewProber(hb, ep.WriteRaw)

	// Memory fabric: a hash index anyone in the mesh can read and write
	// with one-sided verbs, served from the packet path.
	a.fab = fabric.New(ep, fabric.Options{Advertise: n.cfg.Advertise})
	if n.cfg.FabricRegion >= 0 {
		size := n.cfg.FabricRegion
		if size == 0 {
			size = 4 << 20
		}
		a.indexMem = fabric.NewRegion(size)
		if err := fabric.FormatIndex(a.indexMem, 4096); err != nil {
			return err
		}
		a.indexRR = a.fab.Register(a.indexMem, fabric.PermAll)
		a.index, _ = fabric.OpenIndex(context.Background(), a.fab, a.indexRR)
	}

	// Replicated state.
	a.causal = causal.New(causal.Config{Self: n.cfg.Advertise})
	a.store = crdt.NewStore(n.cfg.Advertise, n.clock.Now)
	ep.HandleRaw(tagCausal, a.handleCausal)
	n.rpcSrv.Register("crdt.snapshot", a.handleSnapshot)

	// Time.
	ep.HandleRaw(tagPTP, a.handlePTP)

	// Health.
	a.detector = foresight.New(foresight.Config{Seed: uint64(epochOf(n.cfg.Name))})

	// Speculation.
	a.gossip = speculate.NewGossip(n.cfg.Name, n.cfg.Advertise, uint64(epochOf(n.cfg.Name)))
	a.registry = speculate.NewRegistry(a.gossip)
	ep.HandleRaw(tagSpec, a.registry.HandleRaw)
	n.rpcSrv.Use(a.registry.Interceptor())
	a.spec = speculate.NewExecutor(speculate.Config{MaxCopies: 3}, a.gossip, ep.SendRaw, func(addr string) (uint64, uint64, bool) {
		p, ok := n.rpcCli.Pressure(addr)
		return p.Load, p.Queue, ok
	})
	n.rpcSrv.Register("spec.kv.get", func(ctx context.Context, req []byte) ([]byte, error) {
		v, found, _, err := n.kv.Get(req)
		if err != nil {
			return nil, rpc.Errorf(rpc.Unavailable, "%v", err)
		}
		if !found {
			return nil, rpc.Errorf(rpc.NotFound, "no such key")
		}
		return v, nil
	})

	// Backpressure: a bounded handler pool that tells callers how loaded
	// it is.
	slots, queue := n.cfg.RPCConcurrency, n.cfg.RPCQueue
	if slots == 0 {
		slots = 512
	}
	if queue == 0 {
		queue = 4096
	}
	if slots > 0 {
		n.rpcSrv.SetLimits(rpc.Limits{MaxConcurrent: slots, MaxQueue: queue})
	}
	return nil
}

func (a *autonomy) fabricMeta() string {
	if a.index == nil {
		return ""
	}
	return fmt.Sprintf("%d:%d:%d", a.indexRR.Region, a.indexRR.Key, a.indexRR.Size)
}

func (a *autonomy) close() {
	a.prober.Close()
	a.fab.Close()
	a.n.rpcSrv.Close()
}

// alivePeers lists the other alive members.
func (a *autonomy) alivePeers() []swim.Member {
	var out []swim.Member
	for _, m := range a.n.members.alive() {
		if m.Name != a.n.cfg.Name {
			out = append(out, m)
		}
	}
	return out
}

// loop runs the periodic work. Each concern is cheap; one ticker keeps
// them in step.
func (a *autonomy) loop() {
	n := a.n
	defer n.wg.Done()
	t := time.NewTicker(n.cfg.ProbeInterval)
	defer t.Stop()
	fast := time.NewTicker(max(n.cfg.ProbeInterval/4, 10*time.Millisecond))
	defer fast.Stop()
	for {
		select {
		case <-n.ctx.Done():
			return
		case <-fast.C:
			a.causalTick()
		case <-t.C:
			a.hb.Touch()
			adm := n.rpcSrv.Admission()
			var load uint64
			if adm.MaxConcurrent > 0 {
				load = uint64(adm.Active) * 1000 / uint64(adm.MaxConcurrent)
			}
			a.hb.SetLoad(load)
			peers := a.alivePeers()
			a.timeRound(peers)
			a.healthRound(peers)
			a.gossipRound(peers, load, uint64(adm.Queued))
			a.bootstrapCRDT(peers)
			if a.limiter != nil {
				// The budget is per source across the whole cluster; each
				// node enforces its share, resized as membership changes.
				// Members themselves are exempt: floods come from outside.
				r, _ := a.limiter.Rate()
				if share := offload.Share(n.cfg.RateLimit, len(peers)+1); share != r {
					a.limiter.SetRate(share, max(share/4, 16))
				}
				var addrs []string
				for _, m := range peers {
					addrs = append(addrs, m.Addr)
				}
				a.limiter.Exempt(addrs)
			}
		}
	}
}

// ---- time ----

func (a *autonomy) timeRound(peers []swim.Member) {
	n := a.n
	// Elect the grandmaster among the alive members (every node runs the
	// same comparison over the same membership).
	cands := []vtime.Dataset{vtime.Default(n.cfg.Name)}
	addrOf := map[string]string{n.cfg.Name: n.cfg.Advertise}
	for _, m := range peers {
		cands = append(cands, vtime.Default(m.Name))
		addrOf[m.Name] = m.Addr
	}
	best, _ := vtime.Elect(cands)
	a.tmu.Lock()
	defer a.tmu.Unlock()
	epoch := epochOf(best.Identity)
	if best.Identity == n.cfg.Name {
		if a.master == nil || a.epoch != epoch {
			a.master, a.follower, a.epoch, a.gm = vtime.NewMaster(a.vclock, epoch), nil, epoch, n.cfg.Advertise
			n.events.add("time.grandmaster", "this node is the clock grandmaster")
		}
		for _, m := range peers {
			n.ep.SendRaw(m.Addr, a.master.Sync().Encode())
		}
		return
	}
	if a.follower == nil || a.epoch != epoch {
		a.master = nil
		a.follower = vtime.NewFollower(a.vclock, 8)
		a.follower.Adopt(epoch)
		a.epoch, a.gm = epoch, addrOf[best.Identity]
	}
}

func (a *autonomy) handlePTP(from string, p []byte) {
	m, err := vtime.Decode(p)
	if err != nil {
		return
	}
	a.tmu.Lock()
	defer a.tmu.Unlock()
	switch m.Type {
	case vtime.MsgSync:
		if a.follower != nil && from == a.gm {
			if req, ok := a.follower.HandleSync(m); ok {
				a.n.ep.SendRaw(from, req.Encode())
			}
		}
	case vtime.MsgDelayReq:
		if a.master != nil {
			if resp, ok := a.master.HandleDelayReq(m); ok {
				a.n.ep.SendRaw(from, resp.Encode())
			}
		}
	case vtime.MsgDelayResp:
		if a.follower != nil && from == a.gm {
			a.follower.HandleDelayResp(m)
		}
	}
}

// TimeStatus describes the node's clock discipline.
type TimeStatus struct {
	Grandmaster string           `json:"grandmaster"`
	Clock       vtime.ClockStats `json:"clock"`
	Earliest    time.Time        `json:"earliest"`
	Latest      time.Time        `json:"latest"`
	OffsetNs    int64            `json:"last_offset_ns"`
	DelayNs     int64            `json:"last_delay_ns"`
	HLC         string           `json:"hlc"`
}

// Time snapshots the clock discipline.
func (n *Node) Time() TimeStatus {
	a := n.auto
	iv := a.vclock.Interval()
	ts := TimeStatus{Clock: a.vclock.Stats(), Earliest: time.Unix(0, iv.Earliest), Latest: time.Unix(0, iv.Latest), HLC: n.clock.Now().String()}
	a.tmu.Lock()
	ts.Grandmaster = a.gm
	if a.follower != nil {
		ts.OffsetNs, ts.DelayNs = a.follower.Last()
	}
	a.tmu.Unlock()
	return ts
}

// ---- health ----

func (a *autonomy) healthRound(peers []swim.Member) {
	n := a.n
	type probe struct {
		m   swim.Member
		ack offload.Ack
		err error
	}
	results := make([]probe, len(peers))
	var wg sync.WaitGroup
	for i, m := range peers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ack, err := a.prober.Probe(m.Addr, n.cfg.ProbeInterval/2)
			results[i] = probe{m: m, ack: ack, err: err}
		}()
	}
	wg.Wait()
	now := time.Now()
	avoid := map[string]bool{}
	a.fmu.Lock()
	for _, r := range results {
		s := foresight.Sample{Probed: true, Answered: r.err == nil, ProbeRTT: r.ack.RTT, Staleness: r.ack.Staleness}
		if c := n.ep.Lookup(r.m.Addr); c != nil {
			st := c.Stats()
			s.SRTT, s.MinRTT, s.RTTVar, s.CWND = st.SRTT, st.MinRTT, st.RTTVar, st.CWND
			s.PacketsSent, s.PacketsLost, s.PTOs = st.PacketsSent, st.PacketsLost, st.PTOCount
		}
		before := a.detector.StateOf(r.m.Name)
		as := a.detector.Observe(r.m.Name, s, now)
		if st := a.detector.StateOf(r.m.Name); st != before {
			n.events.add("foresight."+st.String(), fmt.Sprintf("%s: score %.2f %v", r.m.Name, as.Score, as.Reasons))
		}
		if a.detector.StateOf(r.m.Name) == foresight.Failing {
			avoid[r.m.Name] = true
		}
	}
	// Forget peers that left.
	alive := map[string]bool{}
	for _, m := range peers {
		alive[m.Name] = true
	}
	for _, as := range a.detector.All() {
		if !alive[as.Peer] {
			a.detector.Forget(as.Peer)
		}
	}
	a.fmu.Unlock()
	if n.members.setAvoid(avoid) {
		go n.actors.Rebalance()
	}
	a.maybeHandOver()
}

// maybeHandOver: a leader whose links to most of its fellow voters are
// forecast to fail is probably the one about to be cut off -- it hands
// leadership to a voter it still reaches well, before its lease on the
// cluster runs out, instead of leaving the others to wait out an election
// timeout.
func (a *autonomy) maybeHandOver() {
	n := a.n
	if !n.ctl.IsLeader() {
		return
	}
	st := n.ctl.Status()
	a.fmu.Lock()
	defer a.fmu.Unlock()
	if time.Since(a.lastTransfer) < 10*n.cfg.ProbeInterval {
		return
	}
	// Judge only voters the rest of the cluster still considers alive: a
	// dead or suspected peer's bad link says nothing about *our* network.
	// And it takes at least two of them -- one bad link proves nothing.
	var others []string
	for _, v := range st.Voters {
		if m, ok := n.members.get(v); v != n.cfg.Name && ok && m.State == swim.StateAlive {
			others = append(others, v)
		}
	}
	if len(others) < 2 {
		return
	}
	// Hand over to the voter we reach best: a healthy one if there is any;
	// if every link looks bad, our own network is the likely culprit and
	// the least-bad voter still makes a better leader than us.
	bad, best := 0, ""
	bestScore := 2.0
	for _, v := range others {
		as, ok := a.detector.Assess(v, time.Now())
		if !ok {
			continue
		}
		if a.detector.StateOf(v) != foresight.Healthy {
			bad++
		}
		if as.Score < bestScore {
			best, bestScore = v, as.Score
		}
	}
	if bad*2 > len(others) && best != "" {
		a.lastTransfer = time.Now()
		n.events.add("foresight.handover", fmt.Sprintf("links to %d of %d voters forecast to fail: handing leadership to %s", bad, len(others), best))
		n.ctl.TransferLeadership(best)
	}
}

// Foresight returns the failure forecasts for every peer.
func (n *Node) Foresight() []foresight.Assessment {
	a := n.auto
	a.fmu.Lock()
	defer a.fmu.Unlock()
	return a.detector.All()
}

// ---- replicated state ----

func (a *autonomy) flushCausalLocked() {
	for _, o := range a.causal.Outbox() {
		a.n.ep.SendRaw(o.To, o.Payload)
	}
	for _, d := range a.causal.Deliveries() {
		if delta, err := crdt.DecodeDelta(d.Payload); err == nil {
			a.store.Merge(delta)
		}
	}
}

func (a *autonomy) handleCausal(from string, p []byte) {
	a.cmu.Lock()
	defer a.cmu.Unlock()
	a.causal.Handle(from, p)
	a.flushCausalLocked()
}

func (a *autonomy) causalTick() {
	var peers []string
	for _, m := range a.alivePeers() {
		peers = append(peers, m.Addr)
	}
	a.cmu.Lock()
	defer a.cmu.Unlock()
	a.causal.SetPeers(peers)
	a.causal.Tick()
	a.flushCausalLocked()
}

type snapshot struct {
	Deltas []crdt.Delta `json:"deltas"`
	VC     causal.VC    `json:"vc"`
}

func (a *autonomy) handleSnapshot(ctx context.Context, _ []byte) ([]byte, error) {
	a.cmu.Lock()
	defer a.cmu.Unlock()
	return json.Marshal(snapshot{Deltas: a.store.Snapshot(), VC: a.causal.Delivered()})
}

// bootstrapCRDT installs a peer's snapshot once, so a (re)joining node
// starts from the cluster's state instead of waiting for history that may
// already have been garbage collected.
func (a *autonomy) bootstrapCRDT(peers []swim.Member) {
	a.cmu.Lock()
	done := a.bootstrapped
	a.cmu.Unlock()
	if done || len(peers) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(a.n.ctx, a.n.cfg.ProbeInterval)
	defer cancel()
	b, err := a.n.rpcCli.Call(ctx, peers[0].Addr, "crdt.snapshot", nil, rpc.Idempotent())
	if err != nil {
		return
	}
	var s snapshot
	if json.Unmarshal(b, &s) != nil {
		return
	}
	a.cmu.Lock()
	defer a.cmu.Unlock()
	for _, d := range s.Deltas {
		if d.Obj != nil {
			a.store.Merge(d)
		}
	}
	a.causal.FastForward(s.VC)
	a.bootstrapped = true
	a.flushCausalLocked()
}

// CRDTApply performs a local update and replicates it.
func (n *Node) CRDTApply(op crdt.Op) (any, error) {
	a := n.auto
	a.cmu.Lock()
	defer a.cmu.Unlock()
	d, err := a.store.Apply(op)
	if err != nil {
		return nil, err
	}
	b, err := crdt.EncodeDelta(d)
	if err != nil {
		return nil, err
	}
	if _, err := a.causal.Broadcast(b); err != nil {
		return nil, err
	}
	a.flushCausalLocked()
	_, v, _ := a.store.Get(op.Name)
	return v, nil
}

// CRDTGet reads a replicated object.
func (n *Node) CRDTGet(name string) (kind string, value any, ok bool) {
	a := n.auto
	a.cmu.Lock()
	defer a.cmu.Unlock()
	return a.store.Get(name)
}

// ---- speculation ----

func (a *autonomy) gossipRound(peers []swim.Member, load, queue uint64) {
	var addrs []string
	for _, m := range peers {
		addrs = append(addrs, m.Addr)
	}
	var p50, p99 time.Duration
	for _, ms := range a.n.rpcSrv.Stats() {
		if ms.Method == "spec.kv.get" {
			p50 = time.Duration(ms.LatencyMs * float64(time.Millisecond))
			p99 = time.Duration(ms.MaxLatency * float64(time.Millisecond))
		}
	}
	a.gossip.Update(load, queue, p50, p99)
	for _, o := range a.gossip.Round(addrs) {
		a.n.ep.SendRaw(o.To, o.Payload)
	}
}

// SpecGet reads a key through whichever mesh coordinator answers first,
// hedging slow ones (every coordinator serves the same ring).
func (n *Node) SpecGet(ctx context.Context, key []byte) (speculate.Outcome, error) {
	a := n.auto
	cands := []string{n.cfg.Advertise}
	for _, m := range a.alivePeers() {
		cands = append(cands, m.Addr)
	}
	return a.spec.Do(ctx, cands, func(ctx context.Context, addr string) ([]byte, error) {
		if addr == n.cfg.Advertise { // this node's own coordinator: no network hop
			v, found, _, err := n.kv.Get(key)
			switch {
			case err != nil:
				return nil, rpc.Errorf(rpc.Unavailable, "%v", err)
			case !found:
				return nil, rpc.Errorf(rpc.NotFound, "no such key")
			}
			return v, nil
		}
		return n.rpcCli.Call(ctx, addr, "spec.kv.get", key, rpc.Idempotent())
	})
}

// ---- fabric ----

func parseFabricMeta(addr, s string) (fabric.Remote, bool) {
	parts := strings.Split(s, ":")
	if len(parts) != 3 {
		return fabric.Remote{}, false
	}
	id, e1 := strconv.ParseUint(parts[0], 10, 16)
	key, e2 := strconv.ParseUint(parts[1], 10, 32)
	size, e3 := strconv.ParseUint(parts[2], 10, 64)
	if e1 != nil || e2 != nil || e3 != nil {
		return fabric.Remote{}, false
	}
	return fabric.Remote{Addr: addr, Region: uint16(id), Key: uint32(key), Size: size}, true
}

// FabricIndex opens a member's fabric index (one-sided access).
func (n *Node) FabricIndex(ctx context.Context, member string) (*fabric.Index, error) {
	a := n.auto
	if member == n.cfg.Name || member == "" {
		if a.index == nil {
			return nil, errors.New("fabric index disabled on this node")
		}
		return a.index, nil
	}
	m, ok := n.members.get(member)
	if !ok {
		return nil, fmt.Errorf("no member %q", member)
	}
	rr, ok := parseFabricMeta(m.Addr, m.Meta["fabric"])
	if !ok {
		return nil, fmt.Errorf("member %q advertises no fabric index", member)
	}
	return fabric.OpenIndex(ctx, a.fab, rr)
}

// ---- HTTP ----

func (n *Node) autonomyRoutes(mux *http.ServeMux) {
	a := n.auto
	mux.HandleFunc("GET /mesh/time", func(w http.ResponseWriter, r *http.Request) { writeJSON(w, 200, n.Time()) })
	mux.HandleFunc("GET /mesh/foresight", func(w http.ResponseWriter, r *http.Request) { writeJSON(w, 200, n.Foresight()) })
	mux.HandleFunc("GET /mesh/offload", func(w http.ResponseWriter, r *http.Request) {
		var progs []map[string]any
		for _, p := range a.hook.Programs() {
			progs = append(progs, map[string]any{"name": p.Name(), "instructions": len(p.Bytecode()) / 8, "stats": p.Stats()})
		}
		out := map[string]any{"programs": progs, "filter": n.ep.FilterStats()}
		if a.limiter != nil {
			rate, burst := a.limiter.Rate()
			out["rate_limiter"] = map[string]any{"rate_per_peer": rate, "burst": burst, "tracked": a.limiter.Tracked()}
		}
		writeJSON(w, 200, out)
	})
	mux.HandleFunc("GET /mesh/pressure", func(w http.ResponseWriter, r *http.Request) {
		peers := map[string]any{}
		for _, m := range a.alivePeers() {
			if p, ok := n.rpcCli.Pressure(m.Addr); ok {
				peers[m.Name] = map[string]any{"credits": p.Credits, "load_permille": p.Load, "queue": p.Queue}
			}
		}
		writeJSON(w, 200, map[string]any{"server": n.rpcSrv.Admission(), "client": n.rpcCli.PressureStats(), "peers": peers})
	})
	mux.HandleFunc("GET /mesh/spec", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, map[string]any{"executor": a.spec.Stats(), "registry": a.registry.Stats(), "digests": a.gossip.All()})
	})
	mux.HandleFunc("GET /spec/kv/{key}", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := n.reqCtx(r)
		defer cancel()
		out, err := n.SpecGet(ctx, []byte(r.PathValue("key")))
		if err != nil {
			if rpc.CodeOf(err) == rpc.NotFound {
				errJSON(w, 404, err)
				return
			}
			errJSON(w, 503, err)
			return
		}
		writeJSON(w, 200, map[string]any{"value": string(out.Value), "winner": out.Winner, "copies": out.Copies, "latency_ms": ms(out.Latency)})
	})
	mux.HandleFunc("GET /replica", func(w http.ResponseWriter, r *http.Request) {
		a.cmu.Lock()
		objs := map[string]any{}
		for _, name := range a.store.Names() {
			k, v, _ := a.store.Get(name)
			objs[name] = map[string]any{"kind": k, "value": v}
		}
		st := a.causal.Stats()
		a.cmu.Unlock()
		writeJSON(w, 200, map[string]any{"objects": objs, "delivery": st})
	})
	mux.HandleFunc("GET /replica/{name}", func(w http.ResponseWriter, r *http.Request) {
		k, v, ok := n.CRDTGet(r.PathValue("name"))
		if !ok {
			errJSON(w, 404, errors.New("no such object"))
			return
		}
		writeJSON(w, 200, map[string]any{"name": r.PathValue("name"), "kind": k, "value": v})
	})
	mux.HandleFunc("POST /replica/{kind}/{name}", func(w http.ResponseWriter, r *http.Request) {
		var op crdt.Op
		body, _ := io.ReadAll(io.LimitReader(r.Body, 4096))
		if len(body) > 0 {
			if err := json.Unmarshal(body, &op); err != nil {
				errJSON(w, 400, err)
				return
			}
		}
		op.Kind, op.Name = r.PathValue("kind"), r.PathValue("name")
		v, err := n.CRDTApply(op)
		switch {
		case errors.Is(err, crdt.ErrKind), errors.Is(err, crdt.ErrAction):
			errJSON(w, 409, err)
		case err != nil:
			errJSON(w, 400, err)
		default:
			writeJSON(w, 200, map[string]any{"name": op.Name, "kind": op.Kind, "value": v})
		}
	})
	mux.HandleFunc("GET /mesh/fabric", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, map[string]any{"stats": a.fab.Stats(), "index": a.indexRR})
	})
	mux.HandleFunc("PUT /fabric/{key}", func(w http.ResponseWriter, r *http.Request) {
		x, err := n.FabricIndex(r.Context(), "")
		if err != nil {
			errJSON(w, 404, err)
			return
		}
		body, _ := io.ReadAll(io.LimitReader(r.Body, fabric.MaxEntry))
		if err := x.Put(r.Context(), []byte(r.PathValue("key")), body); err != nil {
			errJSON(w, 507, err)
			return
		}
		writeJSON(w, 200, map[string]any{"key": r.PathValue("key"), "bytes": len(body)})
	})
	mux.HandleFunc("GET /fabric/{member}/{key}", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := n.reqCtx(r)
		defer cancel()
		x, err := n.FabricIndex(ctx, r.PathValue("member"))
		if err != nil {
			errJSON(w, 404, err)
			return
		}
		v, err := x.Get(ctx, []byte(r.PathValue("key")))
		if errors.Is(err, fabric.ErrNotFound) {
			errJSON(w, 404, err)
			return
		}
		if err != nil {
			errJSON(w, 503, err)
			return
		}
		writeJSON(w, 200, map[string]any{"key": r.PathValue("key"), "value": string(v), "via": "one-sided verbs"})
	})
}
