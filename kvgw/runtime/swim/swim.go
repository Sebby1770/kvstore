package swim

import (
	"maps"
	"math"
	"math/rand/v2"
	"sort"
	"time"
)

// State of a member as this node believes it.
type State uint8

const (
	StateAlive State = iota
	StateSuspect
	StateDead
	StateLeft
)

func (s State) String() string {
	switch s {
	case StateAlive:
		return "alive"
	case StateSuspect:
		return "suspect"
	case StateDead:
		return "dead"
	case StateLeft:
		return "left"
	}
	return "unknown"
}

// Member is one node as seen from here.
type Member struct {
	Name        string            `json:"name"`
	Addr        string            `json:"addr"`
	Meta        map[string]string `json:"meta,omitempty"`
	State       State             `json:"-"`
	StateName   string            `json:"state"`
	Incarnation uint64            `json:"incarnation"`
	Since       time.Time         `json:"since"`
}

// EventKind classifies membership events.
type EventKind uint8

const (
	EventJoin    EventKind = iota // a new member is alive
	EventUpdate                   // an alive member's metadata or address changed
	EventSuspect                  // a member is suspected
	EventAlive                    // a suspected or dead member is alive again
	EventDead                     // a member was declared dead
	EventLeave                    // a member left gracefully
)

func (k EventKind) String() string {
	return [...]string{"join", "update", "suspect", "alive", "dead", "leave"}[k]
}

// Event reports a membership change.
type Event struct {
	Kind   EventKind
	Member Member
}

// Message is a datagram to send.
type Message struct {
	To      string
	Payload []byte
}

// Config configures a Node. Zero durations take the defaults in brackets.
type Config struct {
	Name, Addr string
	Meta       map[string]string

	ProbeInterval      time.Duration // [1s] one probe per interval
	ProbeTimeout       time.Duration // [500ms] direct ack wait, scaled by local health
	IndirectChecks     int           // [3] ping-req helpers
	SuspicionMult      int           // [4]
	SuspicionMaxMult   int           // [6]
	RetransmitMult     int           // [4]
	GossipInterval     time.Duration // [200ms]
	GossipNodes        int           // [3]
	PushPullInterval   time.Duration // [30s]
	AwarenessMax       int           // [8]
	MaxMessageSize     int           // [1200]
	DeadReclaim        time.Duration // [30s] forget dead/left members after this
	Rand               *rand.Rand    // required for determinism; seeded from Name if nil
	DisableIndirect    bool          // tests only
	DisablePushPull    bool          // tests only
	DisableRefutation  bool          // tests only
	GossipToTheDeadFor time.Duration // [ProbeInterval×5] keep gossiping to recently dead
}

func (c *Config) defaults() {
	d := func(p *time.Duration, v time.Duration) {
		if *p <= 0 {
			*p = v
		}
	}
	i := func(p *int, v int) {
		if *p <= 0 {
			*p = v
		}
	}
	d(&c.ProbeInterval, time.Second)
	d(&c.ProbeTimeout, 500*time.Millisecond)
	i(&c.IndirectChecks, 3)
	i(&c.SuspicionMult, 4)
	i(&c.SuspicionMaxMult, 6)
	i(&c.RetransmitMult, 4)
	d(&c.GossipInterval, 200*time.Millisecond)
	i(&c.GossipNodes, 3)
	d(&c.PushPullInterval, 30*time.Second)
	i(&c.AwarenessMax, 8)
	i(&c.MaxMessageSize, 1200)
	d(&c.DeadReclaim, 30*time.Second)
	d(&c.GossipToTheDeadFor, 5*c.ProbeInterval)
	if c.Rand == nil {
		var h uint64 = 14695981039346656037
		for i := 0; i < len(c.Name); i++ {
			h = (h ^ uint64(c.Name[i])) * 1099511628211
		}
		c.Rand = rand.New(rand.NewPCG(h, 0x5ec5))
	}
}

type member struct {
	Member
	suspect *suspicion
}

// suspicion tracks the Lifeguard timer for one suspected member: it starts at
// the maximum timeout and shrinks toward the minimum as independent members
// confirm the suspicion.
type suspicion struct {
	start     time.Time
	confirmed map[string]bool
	k         int
	min, max  time.Duration
}

func (s *suspicion) deadline() time.Time {
	n := len(s.confirmed)
	if s.k <= 0 || n == 0 {
		return s.start.Add(s.max)
	}
	frac := math.Log(float64(n)+1) / math.Log(float64(s.k)+1)
	frac = min(frac, 1)
	d := s.max - time.Duration(frac*float64(s.max-s.min))
	return s.start.Add(max(d, s.min))
}

type broadcast struct {
	u     update
	sends int
}

type probe struct {
	target   string
	seq      uint64
	sentAt   time.Time
	timeout  time.Time // direct ack deadline
	indirect bool
	periodAt time.Time // end of the protocol period: indirect deadline
	nacks    int
	expNacks int
}

type relay struct {
	requester string // address to answer
	origSeq   uint64
	deadline  time.Time
}

// Node is one SWIM participant as a sans-IO state machine.
type Node struct {
	cfg  Config
	rng  *rand.Rand
	self *member
	mem  map[string]*member

	seq        uint64
	probeOrder []string
	probeIdx   int
	nextProbe  time.Time
	cur        *probe
	relays     map[uint64]relay
	awareness  int
	queue      []*broadcast
	nextGossip time.Time
	nextPush   time.Time
	leaving    bool
	seeds      []string

	out    []Message
	events []Event
}

// New creates a node that knows only itself.
func New(cfg Config, now time.Time) *Node {
	cfg.defaults()
	n := &Node{cfg: cfg, rng: cfg.Rand, mem: map[string]*member{}, relays: map[uint64]relay{}}
	n.self = &member{Member: Member{Name: cfg.Name, Addr: cfg.Addr, Meta: maps.Clone(cfg.Meta), State: StateAlive, Since: now}}
	n.mem[cfg.Name] = n.self
	n.nextProbe = now.Add(cfg.ProbeInterval)
	n.nextGossip = now.Add(cfg.GossipInterval)
	n.nextPush = now.Add(n.jitter(cfg.PushPullInterval))
	return n
}

func (n *Node) jitter(d time.Duration) time.Duration {
	return d/2 + time.Duration(n.rng.Int64N(int64(d)/2+1))
}

// ---- public API ----

// Join contacts seeds (addresses) with a push-pull state exchange.
func (n *Node) Join(seeds []string, now time.Time) {
	for _, s := range seeds {
		if s != n.cfg.Addr && !contains(n.seeds, s) {
			n.seeds = append(n.seeds, s)
		}
	}
	for _, s := range seeds {
		if s == n.cfg.Addr {
			continue
		}
		n.sendPushPull(s, true, false)
	}
}

// Leave announces a graceful departure.
func (n *Node) Leave(now time.Time) {
	n.leaving = true
	n.self.Incarnation++
	n.setState(n.self, StateLeft, now)
	u := n.updateOf(n.self)
	n.enqueue(u)
	// Tell everyone directly too: a leaver may not be around long enough
	// for gossip to carry the news.
	for _, m := range n.sorted() {
		if m != n.self && (m.State == StateAlive || m.State == StateSuspect) {
			n.send(m.Addr, &message{typ: msgGossip, updates: []update{u}})
		}
	}
}

// UpdateMeta changes this node's metadata and gossips it.
func (n *Node) UpdateMeta(meta map[string]string, now time.Time) {
	n.self.Meta = maps.Clone(meta)
	n.self.Incarnation++
	n.enqueue(n.updateOf(n.self))
}

// Outbox drains datagrams to send.
func (n *Node) Outbox() []Message {
	o := n.out
	n.out = nil
	return o
}

// Events drains membership events.
func (n *Node) Events() []Event {
	e := n.events
	n.events = nil
	return e
}

// Incarnation is this node's incarnation number.
func (n *Node) Incarnation() uint64 { return n.self.Incarnation }

// LocalHealth is the Lifeguard awareness score (0 = healthy).
func (n *Node) LocalHealth() int { return n.awareness }

func export(m *member) Member {
	c := m.Member
	c.Meta = maps.Clone(m.Meta)
	c.StateName = m.State.String()
	return c
}

// Members returns every known member (including this node), sorted by name.
func (n *Node) Members() []Member {
	out := make([]Member, 0, len(n.mem))
	for _, m := range n.mem {
		out = append(out, export(m))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// Alive returns the members believed alive (suspects included: SWIM keeps
// routing to a suspect until it is declared dead), sorted by name.
func (n *Node) Alive() []Member {
	var out []Member
	for _, m := range n.Members() {
		if m.State == StateAlive || m.State == StateSuspect {
			out = append(out, m)
		}
	}
	return out
}

// Member looks one member up.
func (n *Node) Member(name string) (Member, bool) {
	m, ok := n.mem[name]
	if !ok {
		return Member{}, false
	}
	return export(m), true
}

// NextDeadline is when Tick must next run.
func (n *Node) NextDeadline() time.Time {
	t := n.nextProbe
	consider := func(x time.Time) {
		if !x.IsZero() && x.Before(t) {
			t = x
		}
	}
	consider(n.nextGossip)
	if !n.cfg.DisablePushPull {
		consider(n.nextPush)
	}
	if p := n.cur; p != nil {
		if !p.indirect {
			consider(p.timeout)
		} else {
			consider(p.periodAt)
		}
	}
	for _, r := range n.relays {
		consider(r.deadline)
	}
	for _, m := range n.mem {
		if m.suspect != nil {
			consider(m.suspect.deadline())
		}
		if m.State == StateDead || m.State == StateLeft {
			consider(m.Since.Add(n.cfg.DeadReclaim))
		}
	}
	return t
}

// ---- helpers ----

// sorted returns the member table in name order: every loop with side
// effects iterates this way, so a seeded run is exactly reproducible.
func (n *Node) sorted() []*member {
	out := make([]*member, 0, len(n.mem))
	for _, m := range n.mem {
		out = append(out, m)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func (n *Node) updateOf(m *member) update {
	return update{name: m.Name, addr: m.Addr, state: m.State, incarnation: m.Incarnation, meta: maps.Clone(m.Meta)}
}

func (n *Node) live() int {
	c := 0
	for _, m := range n.mem {
		if m.State == StateAlive || m.State == StateSuspect {
			c++
		}
	}
	return c
}

func (n *Node) retransmitLimit() int {
	return n.cfg.RetransmitMult * int(math.Ceil(math.Log10(float64(n.live()+1))))
}

func (n *Node) enqueue(u update) {
	q := n.queue[:0]
	for _, b := range n.queue {
		if b.u.name != u.name {
			q = append(q, b)
		}
	}
	n.queue = append(q, &broadcast{u: u})
}

// piggyback adds as many queued updates as fit, least-transmitted first.
func (n *Node) piggyback(m *message) {
	sort.SliceStable(n.queue, func(i, j int) bool { return n.queue[i].sends < n.queue[j].sends })
	size := len(m.encode())
	limit := n.retransmitLimit()
	keep := n.queue[:0]
	for _, b := range n.queue {
		if sz := updateSize(b.u); size+sz <= n.cfg.MaxMessageSize {
			m.updates = append(m.updates, b.u)
			size += sz
			b.sends++
		}
		if b.sends < max(limit, 1) {
			keep = append(keep, b)
		}
	}
	n.queue = keep
}

func (n *Node) send(to string, m *message) {
	m.from, m.addr = n.cfg.Name, n.cfg.Addr
	n.out = append(n.out, Message{To: to, Payload: m.encode()})
}

func (n *Node) sendWithGossip(to string, m *message) {
	m.from, m.addr = n.cfg.Name, n.cfg.Addr
	n.piggyback(m)
	n.out = append(n.out, Message{To: to, Payload: m.encode()})
}

func (n *Node) setState(m *member, s State, now time.Time) {
	m.State = s
	m.Since = now
	if s != StateSuspect {
		m.suspect = nil
	}
}

func (n *Node) emit(k EventKind, m *member) {
	n.events = append(n.events, Event{Kind: k, Member: export(m)})
}

// ---- timers ----

// Tick runs every timer that has expired at now.
func (n *Node) Tick(now time.Time) {
	if p := n.cur; p != nil {
		if !p.indirect && !now.Before(p.timeout) {
			n.startIndirect(p, now)
		}
		if p.indirect && !now.Before(p.periodAt) {
			n.probeFailed(p, now)
		}
	}
	seqs := make([]uint64, 0, len(n.relays))
	for seq := range n.relays {
		seqs = append(seqs, seq)
	}
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
	for _, seq := range seqs {
		if r := n.relays[seq]; !now.Before(r.deadline) {
			n.send(r.requester, &message{typ: msgNack, seq: r.origSeq})
			delete(n.relays, seq)
		}
	}
	for _, m := range n.sorted() {
		if m.suspect != nil && !now.Before(m.suspect.deadline()) {
			n.setState(m, StateDead, now)
			n.enqueue(n.updateOf(m))
			n.emit(EventDead, m)
		}
		if m != n.self && (m.State == StateDead || m.State == StateLeft) && !now.Before(m.Since.Add(n.cfg.DeadReclaim)) {
			delete(n.mem, m.Name)
		}
	}
	if !now.Before(n.nextProbe) && n.cur == nil {
		n.startProbe(now)
	} else if !now.Before(n.nextProbe) && n.cur != nil {
		// The previous probe has not resolved within its period; resolve it.
		n.probeFailed(n.cur, now)
		n.startProbe(now)
	}
	if !now.Before(n.nextGossip) {
		n.gossip(now)
		n.nextGossip = now.Add(n.cfg.GossipInterval)
	}
	if !n.cfg.DisablePushPull && !now.Before(n.nextPush) {
		if t := n.randomMembers(1, "", func(m *member) bool { return m.State == StateAlive }); len(t) > 0 {
			n.sendPushPull(t[0].Addr, false, false)
		}
		n.reconnect()
		n.nextPush = now.Add(n.jitter(n.cfg.PushPullInterval))
	}
}

func (n *Node) scaled(d time.Duration) time.Duration { return d * time.Duration(n.awareness+1) }

func (n *Node) startProbe(now time.Time) {
	n.nextProbe = now.Add(n.scaled(n.cfg.ProbeInterval))
	target := n.nextTarget()
	if target == nil {
		return
	}
	n.seq++
	n.cur = &probe{target: target.Name, seq: n.seq, sentAt: now,
		timeout: now.Add(n.scaled(n.cfg.ProbeTimeout)), periodAt: now.Add(n.scaled(n.cfg.ProbeInterval))}
	n.sendWithGossip(target.Addr, &message{typ: msgPing, seq: n.seq, target: target.Name})
}

func (n *Node) nextTarget() *member {
	for tries := 0; tries < 2; tries++ {
		for n.probeIdx < len(n.probeOrder) {
			name := n.probeOrder[n.probeIdx]
			n.probeIdx++
			if m, ok := n.mem[name]; ok && m != n.self && (m.State == StateAlive || m.State == StateSuspect) {
				return m
			}
		}
		// Reshuffle for the next round.
		n.probeOrder = n.probeOrder[:0]
		for name, m := range n.mem {
			if m != n.self && (m.State == StateAlive || m.State == StateSuspect) {
				n.probeOrder = append(n.probeOrder, name)
			}
		}
		sort.Strings(n.probeOrder)
		n.rng.Shuffle(len(n.probeOrder), func(i, j int) { n.probeOrder[i], n.probeOrder[j] = n.probeOrder[j], n.probeOrder[i] })
		n.probeIdx = 0
	}
	return nil
}

func (n *Node) randomMembers(k int, exclude string, ok func(*member) bool) []*member {
	var cands []*member
	for _, m := range n.mem {
		if m != n.self && m.Name != exclude && ok(m) {
			cands = append(cands, m)
		}
	}
	sort.Slice(cands, func(i, j int) bool { return cands[i].Name < cands[j].Name })
	n.rng.Shuffle(len(cands), func(i, j int) { cands[i], cands[j] = cands[j], cands[i] })
	if len(cands) > k {
		cands = cands[:k]
	}
	return cands
}

func (n *Node) startIndirect(p *probe, now time.Time) {
	p.indirect = true
	if n.cfg.DisableIndirect {
		return
	}
	t, ok := n.mem[p.target]
	if !ok {
		return
	}
	helpers := n.randomMembers(n.cfg.IndirectChecks, p.target, func(m *member) bool { return m.State == StateAlive })
	p.expNacks = len(helpers)
	for _, h := range helpers {
		n.sendWithGossip(h.Addr, &message{typ: msgPingReq, seq: p.seq, target: t.Name, targetAddr: t.Addr})
	}
}

// probeFailed: neither the direct nor the indirect probes got an ack within
// the protocol period, so the target becomes a suspect.
func (n *Node) probeFailed(p *probe, now time.Time) {
	n.cur = nil
	// Lifeguard: if the helpers did not even nack, the problem may be ours.
	if p.nacks < p.expNacks {
		n.awareness = min(n.awareness+1, n.cfg.AwarenessMax-1)
	}
	m, ok := n.mem[p.target]
	if !ok || m.State != StateAlive {
		return
	}
	n.suspect(m, n.cfg.Name, now)
}

func (n *Node) suspicionBounds() (time.Duration, time.Duration, int) {
	nodes := max(n.live(), 1)
	minT := time.Duration(float64(n.cfg.SuspicionMult) * math.Max(1, math.Log10(float64(nodes))) * float64(n.cfg.ProbeInterval))
	k := min(n.cfg.IndirectChecks, max(nodes-2, 0))
	return minT, minT * time.Duration(n.cfg.SuspicionMaxMult), k
}

func (n *Node) suspect(m *member, by string, now time.Time) {
	if m.State != StateSuspect {
		n.setState(m, StateSuspect, now)
		lo, hi, k := n.suspicionBounds()
		m.suspect = &suspicion{start: now, confirmed: map[string]bool{}, k: k, min: lo, max: hi}
		n.enqueue(n.updateOf(m))
		n.emit(EventSuspect, m)
	}
	if by != n.cfg.Name && m.suspect != nil {
		m.suspect.confirmed[by] = true
	}
}

func (n *Node) gossip(now time.Time) {
	if len(n.queue) == 0 {
		return
	}
	targets := n.randomMembers(n.cfg.GossipNodes, "", func(m *member) bool {
		return m.State == StateAlive || m.State == StateSuspect ||
			((m.State == StateDead) && now.Sub(m.Since) < n.cfg.GossipToTheDeadFor)
	})
	for _, t := range targets {
		n.sendWithGossip(t.Addr, &message{typ: msgGossip})
	}
}

// reconnect heals partitions. Gossip only flows between members that
// believe each other alive, so two halves that have declared each other dead
// would stay apart forever. Like Serf, every push-pull period we also run a
// joining exchange with one recently failed member and one seed: if the
// network has healed, the other side learns we are alive (refuting its
// rumour of our death if it has one) and the halves merge.
func (n *Node) reconnect() {
	if dead := n.randomMembers(1, "", func(m *member) bool { return m.State == StateDead }); len(dead) > 0 {
		n.sendPushPull(dead[0].Addr, true, false)
	}
	if len(n.seeds) > 0 {
		s := n.seeds[n.rng.IntN(len(n.seeds))]
		known := false
		for _, m := range n.mem {
			if m.Addr == s && m.State == StateAlive {
				known = true
			}
		}
		if !known {
			n.sendPushPull(s, true, false)
		}
	}
}

func contains(xs []string, s string) bool {
	for _, x := range xs {
		if x == s {
			return true
		}
	}
	return false
}

func (n *Node) sendPushPull(to string, join, reply bool) {
	var all []update
	for _, m := range n.mem {
		all = append(all, n.updateOf(m))
	}
	sort.Slice(all, func(i, j int) bool { return all[i].name < all[j].name })
	// Chunk the state so every datagram fits.
	base := (&message{typ: msgPushPull, from: n.cfg.Name, addr: n.cfg.Addr, join: join, reply: reply}).encode()
	var chunk []update
	size := len(base)
	flush := func() {
		n.send(to, &message{typ: msgPushPull, join: join, reply: reply, updates: chunk})
		chunk, size = nil, len(base)
	}
	for _, u := range all {
		sz := updateSize(u)
		if len(chunk) > 0 && size+sz > n.cfg.MaxMessageSize {
			flush()
			join = false // only the first chunk asks for a reply
		}
		chunk = append(chunk, u)
		size += sz
	}
	if len(chunk) > 0 {
		flush()
	}
}

// ---- receiving ----

// Handle processes a datagram from the given address.
func (n *Node) Handle(from string, payload []byte, now time.Time) {
	m, err := decode(payload)
	if err != nil || m.from == n.cfg.Name {
		return
	}
	// Anything from a member proves it is alive enough to talk: but SWIM
	// relies on explicit rumours, not inference, so only updates count.
	for _, u := range m.updates {
		n.apply(u, m.from, now)
	}
	replyTo := m.addr
	if replyTo == "" {
		replyTo = from
	}
	switch m.typ {
	case msgPing:
		if m.target != "" && m.target != n.cfg.Name {
			return // meant for whoever used to live at this address
		}
		n.sendWithGossip(replyTo, &message{typ: msgAck, seq: m.seq})
	case msgAck:
		if p := n.cur; p != nil && p.seq == m.seq {
			n.cur = nil
			if !p.indirect {
				n.awareness = max(n.awareness-1, 0)
			}
			return
		}
		if r, ok := n.relays[m.seq]; ok {
			n.send(r.requester, &message{typ: msgAck, seq: r.origSeq})
			delete(n.relays, m.seq)
		}
	case msgNack:
		if p := n.cur; p != nil && p.seq == m.seq {
			p.nacks++
		}
	case msgPingReq:
		n.seq++
		n.relays[n.seq] = relay{requester: replyTo, origSeq: m.seq, deadline: now.Add(n.cfg.ProbeTimeout)}
		n.send(m.targetAddr, &message{typ: msgPing, seq: n.seq, target: m.target})
	case msgPushPull:
		if m.join && !m.reply {
			n.sendPushPull(replyTo, false, true)
		}
	case msgGossip:
	}
}

// apply merges one rumour using SWIM's precedence rules.
func (n *Node) apply(u update, from string, now time.Time) {
	if u.name == n.cfg.Name {
		n.applySelf(u, now)
		return
	}
	cur, known := n.mem[u.name]
	switch u.state {
	case StateAlive:
		if !known {
			m := &member{Member: Member{Name: u.name, Addr: u.addr, Meta: u.meta, State: StateAlive, Incarnation: u.incarnation, Since: now}}
			n.mem[u.name] = m
			n.enqueue(u)
			n.emit(EventJoin, m)
			return
		}
		if u.incarnation <= cur.Incarnation {
			return
		}
		was := cur.State
		cur.Incarnation, cur.Addr, cur.Meta = u.incarnation, u.addr, u.meta
		n.enqueue(u)
		if was != StateAlive {
			n.setState(cur, StateAlive, now)
			n.emit(EventAlive, cur)
		} else {
			n.emit(EventUpdate, cur)
		}
	case StateSuspect:
		if !known {
			return
		}
		switch {
		case cur.State == StateAlive && u.incarnation >= cur.Incarnation,
			cur.State == StateSuspect && u.incarnation > cur.Incarnation:
			cur.Incarnation = u.incarnation
			n.setState(cur, StateAlive, now) // reset so suspect() starts a fresh timer
			n.suspect(cur, from, now)
		case cur.State == StateSuspect && u.incarnation == cur.Incarnation:
			n.suspect(cur, from, now) // an independent confirmation
		}
	case StateDead, StateLeft:
		if !known {
			return
		}
		if cur.State == StateDead || cur.State == StateLeft {
			if u.incarnation > cur.Incarnation {
				cur.Incarnation = u.incarnation
			}
			return
		}
		if u.incarnation < cur.Incarnation {
			return
		}
		cur.Incarnation = u.incarnation
		n.setState(cur, u.state, now)
		n.enqueue(u)
		if u.state == StateLeft {
			n.emit(EventLeave, cur)
		} else {
			n.emit(EventDead, cur)
		}
	}
}

// applySelf refutes rumours of our own suspicion or death by out-bidding them.
func (n *Node) applySelf(u update, now time.Time) {
	if n.leaving || n.cfg.DisableRefutation {
		return
	}
	if (u.state == StateSuspect || u.state == StateDead) && u.incarnation >= n.self.Incarnation {
		n.self.Incarnation = u.incarnation + 1
		n.enqueue(n.updateOf(n.self))
		// Being suspected is evidence our own health is poor.
		n.awareness = min(n.awareness+1, n.cfg.AwarenessMax-1)
	}
}
