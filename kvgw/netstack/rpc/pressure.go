package rpc

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/Sebby1770/kvstore/kvgw/netstack/endpoint"
	"github.com/Sebby1770/kvstore/kvgw/netstack/ktp"
	"github.com/Sebby1770/kvstore/kvgw/netstack/wire"
)

// # Application-aware backpressure
//
// KTP's flow control (MAX_DATA, MAX_STREAM_DATA) protects buffers: it stops
// a sender once the receiver's *memory* is full. It is blind to whether the
// receiver's *workers* can keep up -- a server whose handler pool is
// saturated still grants byte credit, so calls pile up in its queues and
// time out there. This file closes that gap with APP_PRESSURE frames:
//
//   - A Server with Limits runs at most MaxConcurrent handlers, queues up to
//     MaxQueue more, and rejects the rest with ResourceExhausted. Whenever
//     its occupancy changes it tells every client connection, in the
//     transport header, how many calls it will admit (Credits), how loaded
//     it is (Load, permille) and how deep its queue is.
//   - Credits are cumulative, like MAX_DATA: "you may have opened this many
//     calls on this connection in total" = calls received from it + free
//     capacity now. An absolute "N free" would be wrong by every call still
//     in flight towards the server; a cumulative limit is not, because the
//     client compares it with its own running total. Admitting a call takes
//     one free slot and adds one received call, so the limit moves only when
//     a handler finishes -- the server sends nothing per call.
//   - A Client counts the calls it opens on each connection and, at the
//     limit, blocks the *caller* (its execution loop, not a network buffer)
//     until the server reports capacity again or the call's context ends.
//   - Server-streaming calls made with Demand(n) get Reactive Streams
//     semantics: the client grants the server n messages at a time, as a
//     stream-scoped APP_PRESSURE frame, and the server's send blocks while
//     the client has granted nothing -- message-granular, not byte-granular.
//
// Credits are advisory across clients: each client sees the server's whole
// free capacity, so several clients together can over-commit it; the excess
// lands in the bounded queue or is rejected (and idempotent calls retry).
// What they guarantee is the important direction: when the server is full,
// every client stops sending.
//
// Pressure is opt-in per connection. A client announces support with its
// first frame (unlimited credits, i.e. "my server is not limited" -- a
// limited server overrides it with real state at once), and a server sends
// only to connections whose peer has spoken, so peers that do not know the
// frame (the Rust storage server) never see one.

// Limits bounds the work a Server does at once.
type Limits struct {
	// MaxConcurrent is how many handlers may run at once (0 = unlimited:
	// no admission control and no pressure frames).
	MaxConcurrent int
	// MaxQueue is how many calls may wait for a handler slot; calls beyond
	// it fail with ResourceExhausted.
	MaxQueue int
	// UpdateInterval is the minimum gap between pressure updates to one
	// connection (default 5ms); changes within it are coalesced.
	UpdateInterval time.Duration
}

// unlimitedCredits is what an unlimited server (or a client's hello)
// advertises.
const unlimitedCredits = 1 << 30

// metaDemand is the request metadata key that switches a streaming call to
// demand mode; its value is the initial grant.
const metaDemand = "rpc-demand"

// maxDemandWindow caps a client's demand window.
const maxDemandWindow = 1 << 20

// AdmissionStats describes a Server's worker pool.
type AdmissionStats struct {
	MaxConcurrent int    `json:"max_concurrent"`
	MaxQueue      int    `json:"max_queue"`
	Active        int    `json:"active"`
	Queued        int    `json:"queued"`
	Admitted      uint64 `json:"admitted"`
	Rejected      uint64 `json:"rejected"`
	Updates       uint64 `json:"pressure_updates"`
}

// admission is a Server's worker pool and pressure broadcaster.
type admission struct {
	mu       sync.Mutex
	lim      Limits
	active   int
	pending  int // calls accepted but not yet admitted, queued or rejected
	waiters  []chan struct{}
	conns    map[*endpoint.Conn]*connState
	admitted uint64
	rejected uint64
	updates  uint64
	dirty    chan struct{}
	stop     chan struct{}
	running  bool
}

type connState struct {
	received uint64 // calls (streams) accepted from this connection
	lastSent ktp.Pressure
	sentAny  bool
	sentAt   time.Time
}

func newAdmission() *admission {
	return &admission{conns: map[*endpoint.Conn]*connState{}, dirty: make(chan struct{}, 1), stop: make(chan struct{})}
}

// SetLimits installs (or, with a zero MaxConcurrent, removes) admission
// control. Calls already running are unaffected.
func (s *Server) SetLimits(l Limits) {
	a := s.adm
	if l.MaxConcurrent < 0 {
		l.MaxConcurrent = 0
	}
	if l.MaxQueue < 0 {
		l.MaxQueue = 0
	}
	if l.UpdateInterval <= 0 {
		l.UpdateInterval = 5 * time.Millisecond
	}
	a.mu.Lock()
	a.lim = l
	start := l.MaxConcurrent > 0 && !a.running
	if start {
		a.running = true
	}
	// Growing the pool admits queued calls at once.
	for len(a.waiters) > 0 && (a.lim.MaxConcurrent == 0 || a.active < a.lim.MaxConcurrent) {
		a.active++
		close(a.waiters[0])
		a.waiters = a.waiters[1:]
	}
	a.mu.Unlock()
	if start {
		go a.loop()
	}
	a.kick()
}

// Admission snapshots the worker pool.
func (s *Server) Admission() AdmissionStats {
	a := s.adm
	a.mu.Lock()
	defer a.mu.Unlock()
	return AdmissionStats{MaxConcurrent: a.lim.MaxConcurrent, MaxQueue: a.lim.MaxQueue, Active: a.active,
		Queued: len(a.waiters), Admitted: a.admitted, Rejected: a.rejected, Updates: a.updates}
}

func (a *admission) kick() {
	select {
	case a.dirty <- struct{}{}:
	default:
	}
}

// acquire takes a handler slot, waiting in FIFO order if the pool is full.
// Every call counted by arrived must end in exactly one acquire or abandon.
func (a *admission) acquire(ctx context.Context) (release func(), err error) {
	a.mu.Lock()
	a.pending = max(a.pending-1, 0)
	if a.lim.MaxConcurrent == 0 {
		a.mu.Unlock()
		return func() {}, nil
	}
	if a.active < a.lim.MaxConcurrent {
		a.active++
		a.admitted++
		a.mu.Unlock()
		a.kick()
		return a.release, nil
	}
	if len(a.waiters) >= a.lim.MaxQueue {
		a.rejected++
		a.mu.Unlock()
		a.kick()
		return nil, Errorf(ResourceExhausted, "server busy: %d running, %d queued", a.lim.MaxConcurrent, a.lim.MaxQueue)
	}
	ch := make(chan struct{})
	a.waiters = append(a.waiters, ch)
	a.mu.Unlock()
	a.kick()
	select {
	case <-ch: // release handed us its slot
		a.mu.Lock()
		a.admitted++
		a.mu.Unlock()
		return a.release, nil
	case <-ctx.Done():
		a.mu.Lock()
		for i, w := range a.waiters {
			if w == ch {
				a.waiters = append(a.waiters[:i], a.waiters[i+1:]...)
				a.mu.Unlock()
				a.kick()
				return nil, ctx.Err()
			}
		}
		a.mu.Unlock()
		// Lost the race: the slot was handed over just as we gave up.
		a.release()
		return nil, ctx.Err()
	}
}

func (a *admission) release() {
	a.mu.Lock()
	if len(a.waiters) > 0 && (a.lim.MaxConcurrent == 0 || a.active <= a.lim.MaxConcurrent) {
		close(a.waiters[0]) // the slot passes straight to the next caller
		a.waiters = a.waiters[1:]
	} else {
		a.active--
	}
	a.mu.Unlock()
	a.kick()
}

func (a *admission) track(c *endpoint.Conn) {
	a.mu.Lock()
	if a.conns[c] == nil {
		a.conns[c] = &connState{}
	}
	a.mu.Unlock()
	a.kick()
}

// arrived counts a call accepted on c; it is what makes credits cumulative.
// Until the call reaches acquire (or abandon) it holds a provisional slot,
// so the limit does not move while its request is still being read.
func (a *admission) arrived(c *endpoint.Conn) {
	a.mu.Lock()
	if st := a.conns[c]; st != nil {
		st.received++
	}
	a.pending++
	a.mu.Unlock()
}

// abandon releases the provisional slot of a call that never got as far as
// acquire (its request was malformed or the stream was reset).
func (a *admission) abandon() {
	a.mu.Lock()
	a.pending = max(a.pending-1, 0)
	a.mu.Unlock()
	a.kick()
}

func (a *admission) untrack(c *endpoint.Conn) {
	a.mu.Lock()
	delete(a.conns, c)
	a.mu.Unlock()
}

// pressureLocked is the pressure to advertise on a connection now.
func (a *admission) pressureLocked(st *connState) ktp.Pressure {
	l := a.lim
	if l.MaxConcurrent == 0 {
		return ktp.Pressure{Credits: unlimitedCredits}
	}
	free := max(l.MaxConcurrent+l.MaxQueue-a.active-len(a.waiters)-a.pending, 0)
	load := uint64(a.active) * wire.MaxLoad / uint64(l.MaxConcurrent)
	return ktp.Pressure{Credits: st.received + uint64(free), Load: min(load, wire.MaxLoad), Queue: uint64(len(a.waiters))}
}

// loop pushes pressure updates to every connection whose peer speaks
// APP_PRESSURE, coalescing changes within UpdateInterval.
func (a *admission) loop() {
	t := time.NewTimer(time.Hour)
	defer t.Stop()
	for {
		select {
		case <-a.stop:
			return
		case <-a.dirty:
		case <-t.C:
		}
		next := a.push(time.Now())
		if !t.Stop() {
			select {
			case <-t.C:
			default:
			}
		}
		// Re-check periodically too: a connection becomes eligible when its
		// peer's first frame arrives, which signals nothing here.
		t.Reset(min(next, 250*time.Millisecond))
	}
}

func (a *admission) push(now time.Time) (retry time.Duration) {
	type send struct {
		c *endpoint.Conn
		p ktp.Pressure
	}
	var out []send
	retry = time.Hour
	a.mu.Lock()
	gap := a.lim.UpdateInterval
	limited := a.lim.MaxConcurrent > 0
	for c, st := range a.conns {
		if !limited && !st.sentAny {
			continue // an unlimited server has nothing to say
		}
		cur := a.pressureLocked(st)
		same := st.sentAny && st.lastSent.Credits == cur.Credits && st.lastSent.Load == cur.Load && st.lastSent.Queue == cur.Queue
		if same {
			continue
		}
		if wait := st.sentAt.Add(gap).Sub(now); st.sentAny && wait > 0 {
			retry = min(retry, wait)
			continue
		}
		out = append(out, send{c, cur})
	}
	a.mu.Unlock()
	for _, s := range out {
		if !s.c.PeerSpeaksPressure() {
			continue
		}
		if s.c.SendPressure(s.p) != nil {
			continue
		}
		a.mu.Lock()
		if st := a.conns[s.c]; st != nil {
			st.lastSent, st.sentAny, st.sentAt = s.p, true, now
			a.updates++
		}
		a.mu.Unlock()
	}
	return retry
}

func (a *admission) close() {
	a.mu.Lock()
	running := a.running
	a.running = false
	a.mu.Unlock()
	if running {
		close(a.stop)
	}
}

// ---- client side ----

// Demand makes a server-streaming call flow-controlled in messages
// (Reactive Streams request(n)): the server may send at most n messages
// beyond those recv has consumed. Without it, only byte-level flow control
// applies.
func Demand(n int) CallOption {
	return func(o *callOpts) { o.demand = min(max(n, 1), maxDemandWindow) }
}

// creditState is a client's count of calls opened on one connection,
// compared against the server's cumulative limit.
type creditState struct {
	opened uint64
}

// PressureStats counts how often a client was throttled by servers.
type PressureStats struct {
	Throttled uint64        `json:"throttled"` // calls that had to wait for credit
	Probes    uint64        `json:"probes"`    // waits that ended by the probe timer
	Waited    time.Duration `json:"waited"`    // total time callers spent waiting
}

// Pressure is the latest load the server at addr reported, if any.
func (c *Client) Pressure(addr string) (ktp.Pressure, bool) {
	conn := c.ep.Lookup(addr)
	if conn == nil {
		return ktp.Pressure{}, false
	}
	p, ok := conn.PeerPressure()
	if ok && p.Credits >= unlimitedCredits {
		return ktp.Pressure{Seq: p.Seq, Credits: unlimitedCredits}, true
	}
	return p, ok
}

// PressureStats snapshots the client's throttling counters.
func (c *Client) PressureStats() PressureStats {
	c.pmu.Lock()
	defer c.pmu.Unlock()
	return c.pstats
}

// hello announces that this side speaks APP_PRESSURE, so the server starts
// reporting its capacity.
func hello(conn *endpoint.Conn) {
	if !conn.SentPressure() {
		conn.SendPressure(ktp.Pressure{Credits: unlimitedCredits})
	}
}

// admit counts one more call against the server's limit, blocking the
// caller while the limit is reached. A probe timer lets one call through if
// the server stays silent, so a stuck count can never wedge a client. The
// returned undo un-counts a call that was never opened.
func (c *Client) admit(ctx context.Context, conn *endpoint.Conn) (undo func(), err error) {
	cs := c.creditFor(conn)
	undo = func() {
		c.pmu.Lock()
		cs.opened--
		c.pmu.Unlock()
	}
	start := time.Time{}
	defer func() {
		if !start.IsZero() {
			c.pmu.Lock()
			c.pstats.Waited += time.Since(start)
			c.pmu.Unlock()
		}
	}()
	for {
		p, ok := conn.PeerPressure()
		c.pmu.Lock()
		if !ok || p.Credits >= unlimitedCredits || p.Credits > cs.opened {
			cs.opened++
			c.pmu.Unlock()
			return undo, nil
		}
		if start.IsZero() {
			start = time.Now()
			c.pstats.Throttled++
		}
		c.pmu.Unlock()
		wctx, cancel := context.WithTimeout(ctx, c.opts.PressureProbe)
		_, err := conn.WaitPeerPressure(wctx, p.Seq)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctxErr(ctx)
			}
			if wctx.Err() != nil { // probe: let one call through
				c.pmu.Lock()
				c.pstats.Probes++
				cs.opened++
				c.pmu.Unlock()
				return undo, nil
			}
			return nil, &Error{Code: Unavailable, Message: "connection closed while throttled: " + err.Error()}
		}
	}
}

func (c *Client) creditFor(conn *endpoint.Conn) *creditState {
	c.pmu.Lock()
	defer c.pmu.Unlock()
	cs := c.credits[conn]
	if cs == nil {
		c.pruneLocked()
		cs = &creditState{}
		c.credits[conn] = cs
	}
	return cs
}

// pruneLocked forgets connections that have closed.
func (c *Client) pruneLocked() {
	if len(c.credits) < 64 {
		return
	}
	for conn := range c.credits {
		select {
		case <-conn.Done():
			delete(c.credits, conn)
		default:
		}
	}
}

// demandGrant tracks a streaming call's Reactive Streams window.
type demandGrant struct {
	window, granted, received uint64
}

// consumed records one delivered message and reports a new cumulative grant
// to send once half the window has been used.
func (g *demandGrant) consumed() (grant uint64, send bool) {
	g.received++
	if g.granted-g.received <= g.window/2 {
		g.granted = g.received + g.window
		return g.granted, true
	}
	return 0, false
}

// serverDemand enforces a client's demand on a streaming handler's sends.
type serverDemand struct {
	conn    *endpoint.Conn
	stream  ktp.StreamID
	granted uint64
	seq     uint64
	sent    uint64
}

func parseDemand(md map[string]string) (uint64, bool) {
	v, ok := md[metaDemand]
	if !ok {
		return 0, false
	}
	n, err := strconv.ParseUint(v, 10, 32)
	if err != nil || n == 0 {
		return 0, false
	}
	return min(n, maxDemandWindow), true
}

// wait blocks until the client has granted another message.
func (d *serverDemand) wait(ctx context.Context) error {
	for d.sent >= d.granted {
		if p, ok := d.conn.PeerStreamPressure(d.stream); ok && p.Seq > d.seq {
			d.seq = p.Seq
			d.granted = max(d.granted, p.Credits)
			continue
		}
		if _, err := d.conn.WaitPeerStreamPressure(ctx, d.stream, d.seq); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}
	}
	d.sent++
	return nil
}
