// Package multipath carries one byte stream over several network paths at
// once, in the manner of Multipath TCP: a Session is made of subflows, each
// its own KTP connection over its own path (another local interface,
// another remote address, or a segment-routed relay chain -- package
// segroute), and therefore with its own congestion control and loss
// recovery. The session cuts the stream into segments numbered by data
// sequence number (DSN), a scheduler spreads them across the subflows, and
// the receiver reassembles them in DSN order. Bandwidth adds up; a path
// that degrades or dies costs only the data in flight on it, which is
// reinjected on the others.
//
// Schedulers:
//
//   - ECF (earliest completion first, the default): send each segment on
//     the path where it would arrive soonest, counting the data already
//     queued there and the path's rate and RTT -- what keeps a slow,
//     high-latency path (satellite) from holding the stream hostage.
//   - MinRTT: the lowest-RTT path with congestion-window room (Linux MPTCP's
//     default).
//   - RoundRobin: rotate across paths with room.
//   - Redundant: every segment on every path; the first copy wins (for
//     latency-critical, low-volume traffic over lossy paths).
//
// Data-level acknowledgements are cumulative and travel as KTP datagrams,
// so they never queue behind stream data; they acknowledge what the
// application has read, which makes the sender's buffer the receiver's
// flow-control window too.
//
// A subflow whose oldest segment has gone unacknowledged far beyond its RTT
// while its connection has received nothing is suspected: its segments are
// reinjected elsewhere and it gets no new data until its connection hears
// from the peer again. A subflow whose connection closes is dropped.
//
// Joins are authenticated twice: by the session token, and by requiring the
// joining connection's KTP peer identity to match the session's.
package multipath

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"io"
	"sort"
	"sync"
	"time"

	"github.com/Sebby1770/kvstore/kvgw/netstack/endpoint"
)

// Scheduler picks the subflow for each segment.
type Scheduler int

const (
	ECF Scheduler = iota
	MinRTT
	RoundRobin
	Redundant
)

func (s Scheduler) String() string {
	return [...]string{"ecf", "minrtt", "roundrobin", "redundant"}[s]
}

// Config tunes a session.
type Config struct {
	Scheduler    Scheduler
	SegmentSize  int           // bytes per segment (default 16 KiB)
	SendBuffer   int           // unacknowledged bytes allowed (default 8 MiB)
	StallTimeout time.Duration // floor for suspecting a subflow (default 500ms)
	AckInterval  time.Duration // data-ACK period (default 5ms)
}

func (c *Config) defaults() {
	if c.SegmentSize <= 0 {
		c.SegmentSize = 16 << 10
	}
	if c.SendBuffer <= 0 {
		c.SendBuffer = 8 << 20
	}
	if c.StallTimeout <= 0 {
		c.StallTimeout = 500 * time.Millisecond
	}
	if c.AckInterval <= 0 {
		c.AckInterval = 5 * time.Millisecond
	}
}

// Path is one way to reach the peer.
type Path struct {
	Name     string
	Endpoint *endpoint.Endpoint
	Addr     string
}

// Errors.
var (
	ErrClosed   = errors.New("multipath: session closed")
	ErrNoPaths  = errors.New("multipath: every subflow has failed")
	ErrRejected = errors.New("multipath: join rejected")
)

// stream records
const (
	recHello   = 1 // client -> server, first on a subflow
	recHelloOK = 2 // server -> client
	recData    = 3
	recFin     = 4
	ackTag     = 0x4D // 'M': data-ACK datagram

	helloNew  = 0
	helloJoin = 1
)

var helloMagic = []byte("KTPMP1")

type segment struct {
	dsn  uint64
	data []byte
	fin  bool
	sent map[*subflow]time.Time // where (and when) copies went
}

func (g *segment) end() uint64 { return g.dsn + uint64(len(g.data)) }

type subflow struct {
	s       *Session
	name    string
	conn    *endpoint.Conn
	st      *endpoint.Stream
	wmu     sync.Mutex
	w       *bufio.Writer
	queue   chan *segment
	queued  int // bytes handed to the writer, not yet written
	bytes   uint64
	suspect bool
	pktsAt  uint64 // PacketsReceived when suspected
	dead    bool

	lastPkts     uint64    // PacketsReceived at the last stall check
	lastProgress time.Time // when PacketsReceived last moved

	readerBuf *bufio.Reader // the stream's reader (it may hold bytes read past the hello)
}

// Session is one multipath byte stream (a full-duplex io.ReadWriteCloser).
type Session struct {
	cfg   Config
	token [16]byte
	peer  ed25519.PublicKey

	mu       sync.Mutex
	wake     chan struct{}
	subflows []*subflow
	rr       int

	// send side
	sndNext  uint64
	sndUna   uint64
	pending  []*segment // waiting for a subflow (new or reinjected)
	inflight map[uint64]*segment
	finSent  bool

	// receive side
	rcvNext   uint64
	ooo       map[uint64]*segment
	readBuf   bytes.Buffer
	readTotal uint64 // bytes the application has consumed
	ackedOut  uint64
	rcvFin    bool
	finDSN    uint64
	gotFin    bool

	readCh chan struct{}

	closed bool
	err    error
	done   chan struct{}
	once   sync.Once
	stats  Stats
}

// Stats describes a session.
type Stats struct {
	Subflows       []SubflowStats `json:"subflows"`
	BytesSent      uint64         `json:"bytes_sent"`
	BytesReceived  uint64         `json:"bytes_received"`
	Reinjected     uint64         `json:"reinjected_bytes"`
	DupReceived    uint64         `json:"duplicate_bytes"`
	Suspicions     uint64         `json:"suspicions"`
	Scheduler      string         `json:"scheduler"`
	OutOfOrderPeak int            `json:"out_of_order_peak"`
}

// SubflowStats describes one path.
type SubflowStats struct {
	Name    string        `json:"name"`
	Bytes   uint64        `json:"bytes"` // data bytes scheduled on it
	SRTT    time.Duration `json:"srtt"`
	CWND    int           `json:"cwnd"`
	Suspect bool          `json:"suspect"`
	Dead    bool          `json:"dead"`
}

func newSession(cfg Config, token [16]byte, peer ed25519.PublicKey) *Session {
	cfg.defaults()
	s := &Session{cfg: cfg, token: token, peer: peer, wake: make(chan struct{}, 1),
		inflight: map[uint64]*segment{}, ooo: map[uint64]*segment{}, done: make(chan struct{})}
	go s.scheduleLoop()
	go s.ackLoop()
	return s
}

func (s *Session) kick() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// broadcast wakes everything waiting on session state.
func (s *Session) broadcastLocked() {
	s.kick()
}

// ---- dial / listen ----

// Dial opens a session over paths: the first carries the session's
// creation, the rest join it concurrently. Paths that fail to join are left
// out (Stats shows which made it); only the first must succeed.
func Dial(ctx context.Context, cfg Config, paths ...Path) (*Session, error) {
	if len(paths) == 0 {
		return nil, ErrNoPaths
	}
	var token [16]byte
	rand.Read(token[:])
	first, err := openSubflow(ctx, paths[0], token, helloNew)
	if err != nil {
		return nil, err
	}
	s := newSession(cfg, token, first.conn.PeerIdentity())
	s.attach(first)
	var wg sync.WaitGroup
	for _, p := range paths[1:] {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if sf, err := openSubflow(ctx, p, token, helloJoin); err == nil {
				s.attach(sf)
			}
		}()
	}
	wg.Wait()
	return s, nil
}

func openSubflow(ctx context.Context, p Path, token [16]byte, kind byte) (*subflow, error) {
	conn, err := p.Endpoint.Dial(ctx, p.Addr)
	if err != nil {
		return nil, err
	}
	st, err := conn.OpenStream(ctx)
	if err != nil {
		conn.Close()
		return nil, err
	}
	hello := append([]byte{recHello}, helloMagic...)
	hello = append(hello, kind)
	hello = append(hello, token[:]...)
	hello = append(hello, byte(len(p.Name)))
	hello = append(hello, p.Name...)
	if _, err := st.Write(hello); err != nil {
		conn.Close()
		return nil, err
	}
	r := bufio.NewReader(st)
	if dl, ok := ctx.Deadline(); ok {
		st.SetReadDeadline(dl)
	}
	b, err := r.ReadByte()
	st.SetReadDeadline(time.Time{})
	if err != nil || b != recHelloOK {
		conn.Close()
		return nil, ErrRejected
	}
	return &subflow{name: p.Name, conn: conn, st: st, w: bufio.NewWriter(st), queue: make(chan *segment, 2), readerBuf: r}, nil
}

// Listener accepts sessions on one or more endpoints.
type Listener struct {
	cfg      Config
	mu       sync.Mutex
	sessions map[[16]byte]*Session
	acceptCh chan *Session
	ctx      context.Context
	cancel   context.CancelFunc
}

// Listen accepts subflows on every endpoint given (one per local address
// or interface the server is reachable on).
func Listen(cfg Config, eps ...*endpoint.Endpoint) *Listener {
	ctx, cancel := context.WithCancel(context.Background())
	l := &Listener{cfg: cfg, sessions: map[[16]byte]*Session{}, acceptCh: make(chan *Session, 16), ctx: ctx, cancel: cancel}
	for _, ep := range eps {
		go func() {
			for {
				conn, err := ep.Accept(ctx)
				if err != nil {
					return
				}
				go l.handle(conn)
			}
		}()
	}
	return l
}

func (l *Listener) handle(conn *endpoint.Conn) {
	ctx, cancel := context.WithTimeout(l.ctx, 10*time.Second)
	defer cancel()
	st, err := conn.AcceptStream(ctx)
	if err != nil {
		conn.Close()
		return
	}
	r := bufio.NewReader(st)
	st.SetReadDeadline(time.Now().Add(5 * time.Second))
	hdr := make([]byte, 1+len(helloMagic)+1+16+1)
	if _, err := io.ReadFull(r, hdr); err != nil || hdr[0] != recHello || !bytes.Equal(hdr[1:1+len(helloMagic)], helloMagic) {
		conn.Close()
		return
	}
	name := make([]byte, hdr[len(hdr)-1])
	if _, err := io.ReadFull(r, name); err != nil {
		conn.Close()
		return
	}
	st.SetReadDeadline(time.Time{})
	kind := hdr[1+len(helloMagic)]
	var token [16]byte
	copy(token[:], hdr[2+len(helloMagic):])
	sf := &subflow{name: string(name), conn: conn, st: st, w: bufio.NewWriter(st), queue: make(chan *segment, 2), readerBuf: r}
	l.mu.Lock()
	s := l.sessions[token]
	switch {
	case kind == helloNew && s == nil:
		s = newSession(l.cfg, token, conn.PeerIdentity())
		l.sessions[token] = s
		l.mu.Unlock()
		st.Write([]byte{recHelloOK})
		s.attach(sf)
		select {
		case l.acceptCh <- s:
		case <-l.ctx.Done():
		}
		go func() { <-s.done; l.mu.Lock(); delete(l.sessions, token); l.mu.Unlock() }()
	case kind == helloJoin && s != nil && bytes.Equal(s.peer, conn.PeerIdentity()):
		l.mu.Unlock()
		st.Write([]byte{recHelloOK})
		s.attach(sf)
	default:
		l.mu.Unlock()
		conn.CloseWithError(0x4d50, "multipath join rejected")
	}
}

// Accept returns the next new session.
func (l *Listener) Accept(ctx context.Context) (*Session, error) {
	select {
	case s := <-l.acceptCh:
		return s, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-l.ctx.Done():
		return nil, ErrClosed
	}
}

// Close stops accepting (sessions already accepted stay open).
func (l *Listener) Close() { l.cancel() }

// ---- subflow plumbing ----

func (s *Session) attach(sf *subflow) {
	sf.s = s
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		sf.conn.Close()
		return
	}
	s.subflows = append(s.subflows, sf)
	s.mu.Unlock()
	go sf.writeLoop()
	go sf.readLoop()
	go sf.ackRecvLoop()
	s.kick()
}

func (sf *subflow) writeRecord(b []byte) error {
	sf.wmu.Lock()
	defer sf.wmu.Unlock()
	if _, err := sf.w.Write(b); err != nil {
		return err
	}
	return sf.w.Flush()
}

func (sf *subflow) writeLoop() {
	s := sf.s
	for {
		select {
		case g := <-sf.queue:
			rec := make([]byte, 0, 13+len(g.data))
			if g.fin {
				rec = append(rec, recFin)
				rec = binary.BigEndian.AppendUint64(rec, g.dsn)
			} else {
				rec = append(rec, recData)
				rec = binary.BigEndian.AppendUint64(rec, g.dsn)
				rec = binary.BigEndian.AppendUint32(rec, uint32(len(g.data)))
				rec = append(rec, g.data...)
			}
			err := sf.writeRecord(rec)
			s.mu.Lock()
			sf.queued -= len(g.data)
			if err != nil {
				s.failLocked(sf)
			}
			s.mu.Unlock()
			s.kick()
		case <-sf.conn.Done():
			s.mu.Lock()
			s.failLocked(sf)
			s.mu.Unlock()
			return
		case <-s.done:
			return
		}
	}
}

// failLocked drops a subflow and reinjects what it carried.
func (s *Session) failLocked(sf *subflow) {
	if sf.dead {
		return
	}
	sf.dead = true
	s.reinjectLocked(sf)
	alive := false
	for _, o := range s.subflows {
		if !o.dead {
			alive = true
		}
	}
	if !alive && !s.closed {
		s.err = ErrNoPaths
		s.shutdownLocked()
	}
}

// reinjectLocked puts segments last sent on sf that are not yet
// acknowledged back at the front of the queue.
func (s *Session) reinjectLocked(sf *subflow) {
	var re []*segment
	for _, g := range s.inflight {
		if _, on := g.sent[sf]; on && g.end() > s.sndUna {
			delete(g.sent, sf)
			if len(g.sent) == 0 {
				re = append(re, g)
				delete(s.inflight, g.dsn)
			}
		}
	}
	sort.Slice(re, func(i, j int) bool { return re[i].dsn < re[j].dsn })
	for _, g := range re {
		s.stats.Reinjected += uint64(len(g.data))
	}
	s.pending = append(re, s.pending...)
	s.kick()
}

// ---- writing ----

// Write queues p on the session, blocking while the send buffer is full.
func (s *Session) Write(p []byte) (int, error) {
	n := 0
	for len(p) > 0 {
		s.mu.Lock()
		for !s.closed && s.finSent == false && int(s.sndNext-s.sndUna) >= s.cfg.SendBuffer {
			s.mu.Unlock()
			select {
			case <-time.After(time.Millisecond):
			case <-s.done:
			}
			s.mu.Lock()
		}
		if s.closed {
			s.mu.Unlock()
			return n, s.errLocked()
		}
		if s.finSent {
			s.mu.Unlock()
			return n, ErrClosed
		}
		k := min(len(p), s.cfg.SegmentSize)
		g := &segment{dsn: s.sndNext, data: append([]byte(nil), p[:k]...), sent: map[*subflow]time.Time{}}
		s.sndNext += uint64(k)
		s.pending = append(s.pending, g)
		s.mu.Unlock()
		s.kick()
		p, n = p[k:], n+k
	}
	return n, nil
}

// CloseWrite sends FIN after everything written so far.
func (s *Session) CloseWrite() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.finSent || s.closed {
		return nil
	}
	s.finSent = true
	s.pending = append(s.pending, &segment{dsn: s.sndNext, fin: true, sent: map[*subflow]time.Time{}})
	s.kick()
	return nil
}

func (s *Session) errLocked() error {
	if s.err != nil {
		return s.err
	}
	return ErrClosed
}

// scheduleLoop assigns pending segments to subflows and watches for stalls.
func (s *Session) scheduleLoop() {
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	lastStall := time.Now()
	for {
		select {
		case <-s.done:
			return
		case <-s.wake:
		case <-tick.C:
		}
		s.mu.Lock()
		if time.Since(lastStall) >= 25*time.Millisecond {
			s.checkStallsLocked()
			lastStall = time.Now()
		}
		for len(s.pending) > 0 {
			g := s.pending[0]
			targets := s.pickLocked(g)
			if len(targets) == 0 {
				break
			}
			s.pending = s.pending[1:]
			now := time.Now()
			for _, sf := range targets {
				g.sent[sf] = now
				sf.queued += len(g.data)
				sf.bytes += uint64(len(g.data))
				s.stats.BytesSent += uint64(len(g.data))
				sf.queue <- g // never blocks: room was checked
			}
			s.inflight[g.dsn] = g
		}
		s.mu.Unlock()
	}
}

type pathInfo struct {
	sf          *subflow
	srtt        time.Duration
	cwnd, room  int
	outstanding int
}

func (s *Session) pathsLocked() []pathInfo {
	var out []pathInfo
	for _, sf := range s.subflows {
		if sf.dead || sf.suspect || len(sf.queue) == cap(sf.queue) {
			continue
		}
		st := sf.conn.Stats()
		srtt := st.SRTT
		if srtt <= 0 {
			srtt = 100 * time.Millisecond
		}
		in := st.BytesInFlight + sf.queued
		out = append(out, pathInfo{sf: sf, srtt: srtt, cwnd: st.CWND, room: st.CWND + s.cfg.SegmentSize - in, outstanding: in})
	}
	return out
}

// pickLocked chooses the subflow(s) for a segment, or none if every path
// is full right now.
func (s *Session) pickLocked(g *segment) []*subflow {
	paths := s.pathsLocked()
	var open []pathInfo
	for _, p := range paths {
		if p.room > 0 {
			open = append(open, p)
		}
	}
	if len(open) == 0 {
		return nil
	}
	switch s.cfg.Scheduler {
	case Redundant:
		out := make([]*subflow, len(open))
		for i, p := range open {
			out[i] = p.sf
		}
		return out
	case RoundRobin:
		s.rr++
		return []*subflow{open[s.rr%len(open)].sf}
	case MinRTT:
		best := open[0]
		for _, p := range open[1:] {
			if p.srtt < best.srtt {
				best = p
			}
		}
		return []*subflow{best.sf}
	}
	// ECF: estimated arrival of this segment on each path, counting
	// everything already queued ahead of it, at the path's rate (window per
	// RTT). Only paths with room may take it now, but a full fast path may
	// still win: then we wait for it rather than send on a slow one.
	arrival := func(p pathInfo) time.Duration {
		rate := float64(max(p.cwnd, 1)) / p.srtt.Seconds() // bytes per second
		ahead := float64(p.outstanding + len(g.data))
		return time.Duration(ahead/rate*float64(time.Second)) + p.srtt/2
	}
	best, bestAt := paths[0], arrival(paths[0])
	for _, p := range paths[1:] {
		if at := arrival(p); at < bestAt {
			best, bestAt = p, at
		}
	}
	if best.room <= 0 {
		// Waiting for the best path is worth it only if it beats the best
		// open one by a margin; otherwise use the open one.
		ob, obAt := open[0], arrival(open[0])
		for _, p := range open[1:] {
			if at := arrival(p); at < obAt {
				ob, obAt = p, at
			}
		}
		if obAt <= bestAt+best.srtt/2 {
			return []*subflow{ob.sf}
		}
		return nil
	}
	return []*subflow{best.sf}
}

// checkStallsLocked suspects subflows that hold old unacknowledged data
// while hearing nothing, and reinstates suspects that hear again.
func (s *Session) checkStallsLocked() {
	now := time.Now()
	for _, sf := range s.subflows {
		if sf.dead {
			continue
		}
		st := sf.conn.Stats()
		if st.PacketsReceived != sf.lastPkts || sf.lastProgress.IsZero() {
			sf.lastPkts, sf.lastProgress = st.PacketsReceived, now
		}
		if sf.suspect {
			if st.PacketsReceived > sf.pktsAt {
				sf.suspect = false
			}
			continue
		}
		limit := max(s.cfg.StallTimeout, 4*st.SRTT+200*time.Millisecond)
		// Old unacknowledged data alone proves nothing: data ACKs are
		// cumulative, so a fast path's segments legitimately wait for a slow
		// path's. A path is stalled only if its connection has also heard
		// nothing from the peer for as long.
		if now.Sub(sf.lastProgress) <= limit {
			continue
		}
		oldest := time.Time{}
		for _, g := range s.inflight {
			if at, on := g.sent[sf]; on && (oldest.IsZero() || at.Before(oldest)) {
				oldest = at
			}
		}
		if !oldest.IsZero() && now.Sub(oldest) > limit {
			sf.suspect, sf.pktsAt = true, st.PacketsReceived
			s.stats.Suspicions++
			s.reinjectLocked(sf)
		}
	}
}

// ackRecvLoop reads data-ACK datagrams on a subflow's connection.
func (sf *subflow) ackRecvLoop() {
	s := sf.s
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		select {
		case <-s.done:
		case <-sf.conn.Done():
		}
		cancel()
	}()
	for {
		d, err := sf.conn.ReceiveDatagram(ctx)
		if err != nil {
			return
		}
		if len(d) != 9 || d[0] != ackTag {
			continue
		}
		s.handleAck(binary.BigEndian.Uint64(d[1:]))
	}
}

func (s *Session) handleAck(ack uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ack <= s.sndUna || ack > s.sndNext+1 {
		return
	}
	s.sndUna = ack
	for dsn, g := range s.inflight {
		if g.end() <= ack && !(g.fin && dsn >= ack) {
			delete(s.inflight, dsn)
		}
	}
	s.kick()
}

// ---- reading ----

func (sf *subflow) readLoop() {
	s := sf.s
	r := sf.readerBuf
	hdr := make([]byte, 12)
	for {
		t, err := r.ReadByte()
		if err != nil {
			s.mu.Lock()
			s.failLocked(sf)
			s.mu.Unlock()
			return
		}
		switch t {
		case recData:
			if _, err := io.ReadFull(r, hdr); err != nil {
				return
			}
			dsn, n := binary.BigEndian.Uint64(hdr), binary.BigEndian.Uint32(hdr[8:])
			if int(n) > 1<<20 {
				sf.conn.CloseWithError(0x4d51, "segment too large")
				return
			}
			data := make([]byte, n)
			if _, err := io.ReadFull(r, data); err != nil {
				return
			}
			s.receive(&segment{dsn: dsn, data: data})
		case recFin:
			if _, err := io.ReadFull(r, hdr[:8]); err != nil {
				return
			}
			s.receive(&segment{dsn: binary.BigEndian.Uint64(hdr), fin: true})
		default:
			sf.conn.CloseWithError(0x4d52, "unknown multipath record")
			return
		}
	}
}

func (s *Session) receive(g *segment) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stats.BytesReceived += uint64(len(g.data))
	if g.fin {
		s.gotFin, s.finDSN = true, g.dsn
	} else if g.end() <= s.rcvNext {
		s.stats.DupReceived += uint64(len(g.data))
	} else if _, dup := s.ooo[g.dsn]; dup {
		s.stats.DupReceived += uint64(len(g.data))
	} else {
		s.ooo[g.dsn] = g
		s.stats.OutOfOrderPeak = max(s.stats.OutOfOrderPeak, len(s.ooo))
	}
	// Deliver everything now contiguous.
	for {
		h, ok := s.ooo[s.rcvNext]
		if !ok {
			break
		}
		delete(s.ooo, s.rcvNext)
		s.readBuf.Write(h.data)
		s.rcvNext = h.end()
	}
	// A segment may overlap rcvNext if reinjected with different cuts (it
	// is not, but tolerate it): trim and deliver.
	for dsn, h := range s.ooo {
		if dsn < s.rcvNext && h.end() > s.rcvNext {
			delete(s.ooo, dsn)
			s.readBuf.Write(h.data[s.rcvNext-dsn:])
			s.rcvNext = h.end()
		} else if h.end() <= s.rcvNext {
			delete(s.ooo, dsn)
		}
	}
	if s.gotFin && s.rcvNext == s.finDSN {
		s.rcvFin = true
	}
	s.broadcastLocked()
	s.notifyReaders()
}

func (s *Session) notifyReaders() {
	if s.readCh == nil {
		return
	}
	select {
	case s.readCh <- struct{}{}:
	default:
	}
}

// Read reads stream bytes in order.
func (s *Session) Read(p []byte) (int, error) {
	for {
		s.mu.Lock()
		if s.readCh == nil {
			s.readCh = make(chan struct{}, 1)
		}
		if s.readBuf.Len() > 0 {
			n, _ := s.readBuf.Read(p)
			s.readTotal += uint64(n)
			s.mu.Unlock()
			return n, nil
		}
		if s.rcvFin {
			s.mu.Unlock()
			return 0, io.EOF
		}
		if s.closed {
			err := s.errLocked()
			s.mu.Unlock()
			return 0, err
		}
		ch := s.readCh
		s.mu.Unlock()
		select {
		case <-ch:
		case <-s.done:
		}
	}
}

// ackLoop sends cumulative data ACKs (what the application has consumed)
// on the lowest-RTT live subflow, as datagrams.
func (s *Session) ackLoop() {
	t := time.NewTicker(s.cfg.AckInterval)
	defer t.Stop()
	for {
		select {
		case <-s.done:
			return
		case <-t.C:
		}
		s.mu.Lock()
		ack := s.readTotal
		if s.rcvFin {
			ack++ // the FIN occupies one sequence number
		}
		if ack == s.ackedOut {
			s.mu.Unlock()
			continue
		}
		var best *subflow
		var bestRTT time.Duration
		var all []*subflow
		for _, sf := range s.subflows {
			if sf.dead {
				continue
			}
			all = append(all, sf)
			if rtt := sf.conn.Stats().SRTT; best == nil || rtt < bestRTT {
				best, bestRTT = sf, rtt
			}
		}
		s.ackedOut = ack
		s.mu.Unlock()
		msg := binary.BigEndian.AppendUint64([]byte{ackTag}, ack)
		if best != nil {
			best.conn.SendDatagram(msg)
		}
		// Also on the others now and then: an ACK lost on one path must not
		// wait for the next delivery to be repaired.
		for _, sf := range all {
			if sf != best {
				sf.conn.SendDatagram(msg)
			}
		}
	}
}

// ---- lifecycle and stats ----

// Close tears the session down (both directions) and closes its subflows.
func (s *Session) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.shutdownLocked()
	return nil
}

func (s *Session) shutdownLocked() {
	if s.closed {
		return
	}
	s.closed = true
	s.once.Do(func() { close(s.done) })
	for _, sf := range s.subflows {
		go sf.conn.Close()
	}
}

// Done is closed when the session ends.
func (s *Session) Done() <-chan struct{} { return s.done }

// Flushed reports whether everything written has been acknowledged.
func (s *Session) Flushed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	end := s.sndNext
	if s.finSent {
		end++
	}
	return s.sndUna >= end
}

// Stats snapshots the session.
func (s *Session) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.stats
	st.Scheduler = s.cfg.Scheduler.String()
	st.Subflows = nil
	for _, sf := range s.subflows {
		cs := sf.conn.Stats()
		st.Subflows = append(st.Subflows, SubflowStats{Name: sf.name, Bytes: sf.bytes, SRTT: cs.SRTT, CWND: cs.CWND, Suspect: sf.suspect, Dead: sf.dead})
	}
	return st
}
