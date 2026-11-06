package actor

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"
)

// HostConfig configures a Host.
type HostConfig struct {
	Node        string
	Placement   Placement
	Transport   Transport
	Store       StateStore
	IdleTimeout time.Duration // [5m]
	MailboxSize int           // [256]
	MaxHops     int           // [3]
	// CrashBackoff is the first delay before re-activating a crashed actor;
	// it doubles per consecutive crash up to 30s [100ms].
	CrashBackoff time.Duration
}

// Stats counts host activity.
type Stats struct {
	Activations   uint64 `json:"activations"`
	Deactivations uint64 `json:"deactivations"`
	Messages      uint64 `json:"messages"`
	Forwarded     uint64 `json:"forwarded"`
	Crashes       uint64 `json:"crashes"`
	Conflicts     uint64 `json:"conflicts"`
	Active        int    `json:"active"`
}

// ActivationInfo describes a live activation.
type ActivationInfo struct {
	Type      string    `json:"type"`
	Key       string    `json:"key"`
	Activated time.Time `json:"activated"`
	Messages  uint64    `json:"messages"`
	Version   uint64    `json:"version"`
}

type crashInfo struct {
	until time.Time
	count int
}

// Host runs the activations owned by one node.
type Host struct {
	cfg HostConfig

	mu        sync.Mutex
	factories map[string]Factory
	acts      map[ID]*activation
	crashes   map[ID]crashInfo
	closed    bool
	stats     Stats
	wg        sync.WaitGroup
}

// NewHost builds a host.
func NewHost(cfg HostConfig) *Host {
	if cfg.IdleTimeout <= 0 {
		cfg.IdleTimeout = 5 * time.Minute
	}
	if cfg.MailboxSize <= 0 {
		cfg.MailboxSize = 256
	}
	if cfg.MaxHops <= 0 {
		cfg.MaxHops = 3
	}
	if cfg.CrashBackoff <= 0 {
		cfg.CrashBackoff = 100 * time.Millisecond
	}
	return &Host{cfg: cfg, factories: map[string]Factory{}, acts: map[ID]*activation{}, crashes: map[ID]crashInfo{}}
}

// Register makes an actor type available on this host.
func (h *Host) Register(typ string, f Factory) {
	h.mu.Lock()
	h.factories[typ] = f
	h.mu.Unlock()
}

// Types lists registered actor types.
func (h *Host) Types() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []string
	for t := range h.factories {
		out = append(out, t)
	}
	sort.Strings(out)
	return out
}

func (h *Host) count(f func(*Stats)) {
	h.mu.Lock()
	f(&h.stats)
	h.mu.Unlock()
}

// Ask sends msg to the actor and waits for its reply.
func (h *Host) Ask(ctx context.Context, id ID, msg []byte) ([]byte, error) {
	return h.route(ctx, Envelope{ID: id, Msg: msg})
}

// Tell sends msg without waiting for the actor to process it.
func (h *Host) Tell(ctx context.Context, id ID, msg []byte) error {
	_, err := h.route(ctx, Envelope{ID: id, Msg: msg, Tell: true})
	return err
}

// HandleRemote is the transport's entry point on the receiving node.
func (h *Host) HandleRemote(ctx context.Context, env Envelope) ([]byte, error) {
	return h.route(ctx, env)
}

func (h *Host) route(ctx context.Context, env Envelope) ([]byte, error) {
	owner, ok := h.cfg.Placement.Owner(env.ID)
	if !ok {
		return nil, ErrNoOwner
	}
	if owner != h.cfg.Node {
		if env.Hops >= h.cfg.MaxHops {
			return nil, ErrTooManyHops
		}
		env.Hops++
		h.count(func(s *Stats) { s.Forwarded++ })
		return h.cfg.Transport.Deliver(ctx, owner, env)
	}
	return h.local(ctx, env)
}

type result struct {
	data []byte
	err  error
}

type request struct {
	ctx   context.Context
	msg   []byte
	reply chan result // nil for Tell and timers
	timer string
}

// local delivers to (activating if necessary) the local activation. The
// mailbox send happens under the host lock, and deactivation removes the
// activation from the map under the same lock before draining its mailbox,
// so no message can land in a mailbox nobody will read.
func (h *Host) local(ctx context.Context, env Envelope) ([]byte, error) {
	req := &request{ctx: ctx, msg: env.Msg}
	if !env.Tell {
		req.reply = make(chan result, 1)
	}
	for {
		h.mu.Lock()
		if h.closed {
			h.mu.Unlock()
			return nil, ErrClosed
		}
		a, err := h.activationLocked(env.ID)
		if err != nil {
			h.mu.Unlock()
			return nil, err
		}
		select {
		case a.mailbox <- req:
			h.stats.Messages++
			h.mu.Unlock()
			if req.reply == nil {
				return nil, nil
			}
			select {
			case r := <-req.reply:
				return r.data, r.err
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		default:
		}
		h.mu.Unlock()
		if env.Tell {
			return nil, ErrMailboxFull
		}
		select { // Ask: wait for room
		case <-a.room:
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-a.done:
		}
	}
}

func (h *Host) activationLocked(id ID) (*activation, error) {
	if a, ok := h.acts[id]; ok {
		return a, nil
	}
	f, ok := h.factories[id.Type]
	if !ok {
		return nil, fmt.Errorf("%w %q", ErrUnknownType, id.Type)
	}
	if c, ok := h.crashes[id]; ok && time.Now().Before(c.until) {
		return nil, ErrBackoff
	}
	a := &activation{id: id, actor: f(id), host: h, mailbox: make(chan *request, h.cfg.MailboxSize),
		room: make(chan struct{}, 1), stop: make(chan struct{}), done: make(chan struct{}),
		timers: map[string]*time.Timer{}, activated: time.Now()}
	h.acts[id] = a
	h.stats.Activations++
	h.wg.Add(1)
	go a.run()
	return a, nil
}

// Rebalance deactivates (persisting) every activation this node no longer
// owns. Call it after membership changes.
func (h *Host) Rebalance() int {
	h.mu.Lock()
	var moved []*activation
	for id, a := range h.acts {
		if owner, ok := h.cfg.Placement.Owner(id); !ok || owner != h.cfg.Node {
			moved = append(moved, a)
		}
	}
	h.mu.Unlock()
	for _, a := range moved {
		a.requestStop()
	}
	return len(moved)
}

// Activations lists live activations, sorted.
func (h *Host) Activations() []ActivationInfo {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]ActivationInfo, 0, len(h.acts))
	for id, a := range h.acts {
		a.infoMu.Lock()
		out = append(out, ActivationInfo{Type: id.Type, Key: id.Key, Activated: a.activated, Messages: a.messages, Version: a.infoVersion})
		a.infoMu.Unlock()
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Type != out[j].Type {
			return out[i].Type < out[j].Type
		}
		return out[i].Key < out[j].Key
	})
	return out
}

// Stats snapshots counters.
func (h *Host) Stats() Stats {
	h.mu.Lock()
	defer h.mu.Unlock()
	s := h.stats
	s.Active = len(h.acts)
	return s
}

// Close deactivates everything (persisting dirty state) and refuses new
// messages.
func (h *Host) Close(ctx context.Context) error {
	h.mu.Lock()
	h.closed = true
	acts := make([]*activation, 0, len(h.acts))
	for _, a := range h.acts {
		acts = append(acts, a)
	}
	h.mu.Unlock()
	for _, a := range acts {
		a.requestStop()
	}
	done := make(chan struct{})
	go func() { h.wg.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (h *Host) crashed(id ID) {
	h.mu.Lock()
	defer h.mu.Unlock()
	c := h.crashes[id]
	c.count++
	d := h.cfg.CrashBackoff << min(c.count-1, 8)
	c.until = time.Now().Add(min(d, 30*time.Second))
	h.crashes[id] = c
	h.stats.Crashes++
}

func (h *Host) healthy(id ID) {
	h.mu.Lock()
	delete(h.crashes, id)
	h.mu.Unlock()
}

// ---- activation ----

type activation struct {
	id      ID
	actor   Actor
	host    *Host
	mailbox chan *request
	room    chan struct{} // signalled when the mailbox drains a slot
	stop    chan struct{}
	stopped sync.Once
	done    chan struct{}

	// owned by the activation goroutine
	state      []byte
	version    uint64
	dirty      bool
	poisoned   bool
	deactivate bool
	crashed    bool
	timers     map[string]*time.Timer
	timerMu    sync.Mutex

	infoMu      sync.Mutex
	activated   time.Time
	messages    uint64
	infoVersion uint64
}

func (a *activation) requestStop() { a.stopped.Do(func() { close(a.stop) }) }

func (a *activation) run() {
	h := a.host
	defer h.wg.Done()
	ctx := context.Background()
	data, ver, err := h.cfg.Store.Load(ctx, a.id)
	if err == nil {
		a.state, a.version = data, ver
		a.infoVersion = ver
		if act, ok := a.actor.(Activator); ok {
			err = a.safe(func() error { return act.OnActivate(&Context{a: a, ctx: ctx}) })
		}
	}
	if err != nil {
		a.finish(fmt.Errorf("actor %s: activation failed: %w", a.id, err))
		return
	}
	idle := time.NewTimer(h.cfg.IdleTimeout)
	defer idle.Stop()
	for {
		select {
		case req := <-a.mailbox:
			select {
			case a.room <- struct{}{}:
			default:
			}
			a.handle(req)
			if !idle.Stop() {
				select {
				case <-idle.C:
				default:
				}
			}
			idle.Reset(h.cfg.IdleTimeout)
			if a.poisoned || a.deactivate || a.crashed {
				a.finish(nil)
				return
			}
		case <-idle.C:
			a.finish(nil)
			return
		case <-a.stop:
			a.finish(nil)
			return
		}
	}
}

// safe runs f, converting a panic into ErrActorPanicked.
func (a *activation) safe(f func() error) (err error) {
	defer func() {
		if p := recover(); p != nil {
			a.crashed = true
			err = fmt.Errorf("%w: %s: %v", ErrActorPanicked, a.id, p)
		}
	}()
	return f()
}

func (a *activation) handle(req *request) {
	var reply []byte
	err := a.safe(func() error {
		var e error
		reply, e = a.actor.Receive(&Context{a: a, ctx: req.ctx, timer: req.timer}, req.msg)
		return e
	})
	if a.poisoned && err == nil {
		err = ErrStaleActivation
	}
	a.infoMu.Lock()
	a.messages++
	a.infoVersion = a.version
	a.infoMu.Unlock()
	if req.reply != nil {
		req.reply <- result{data: reply, err: err}
	}
}

// finish tears the activation down: unregister (so new messages create a
// fresh activation), persist dirty state unless stale or crashed, run the
// hook, and hand any queued messages to the next activation.
func (a *activation) finish(failure error) {
	h := a.host
	h.mu.Lock()
	if h.acts[a.id] == a {
		delete(h.acts, a.id)
	}
	h.stats.Deactivations++
	h.mu.Unlock()
	a.timerMu.Lock()
	for _, t := range a.timers {
		t.Stop()
	}
	a.timers = nil
	a.timerMu.Unlock()
	ctx := context.Background()
	if failure == nil && !a.crashed {
		if d, ok := a.actor.(Deactivator); ok {
			a.safe(func() error { return d.OnDeactivate(&Context{a: a, ctx: ctx}) })
		}
		if a.dirty && !a.poisoned && !a.crashed {
			(&Context{a: a, ctx: ctx}).Persist()
		}
	}
	if a.crashed {
		h.crashed(a.id)
	} else {
		h.healthy(a.id)
	}
	close(a.done)
	// Leftover messages go to a fresh activation (or fail if we failed).
	for {
		select {
		case req := <-a.mailbox:
			if failure != nil {
				if req.reply != nil {
					req.reply <- result{err: failure}
				}
				continue
			}
			go func() {
				data, err := h.local(req.ctx, Envelope{ID: a.id, Msg: req.msg, Tell: req.reply == nil})
				if req.reply != nil {
					req.reply <- result{data: data, err: err}
				}
			}()
		default:
			return
		}
	}
}

func (a *activation) setTimer(name string, every time.Duration, payload []byte) {
	a.timerMu.Lock()
	defer a.timerMu.Unlock()
	if a.timers == nil {
		return
	}
	if t, ok := a.timers[name]; ok {
		t.Stop()
	}
	var fire func()
	fire = func() {
		select {
		case a.mailbox <- &request{ctx: context.Background(), msg: payload, timer: name}:
		default: // mailbox full: skip this tick
		}
		a.timerMu.Lock()
		if t, ok := a.timers[name]; ok && a.timers != nil {
			t.Reset(every)
		}
		a.timerMu.Unlock()
	}
	a.timers[name] = time.AfterFunc(every, fire)
}

func (a *activation) cancelTimer(name string) {
	a.timerMu.Lock()
	defer a.timerMu.Unlock()
	if t, ok := a.timers[name]; ok {
		t.Stop()
		delete(a.timers, name)
	}
}
