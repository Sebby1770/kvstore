// Package endpoint drives KTP connections over a packet socket. One Endpoint
// owns one net.PacketConn -- a UDP socket in production, a simnet conn in
// tests -- and multiplexes every connection to and from it: it dials and
// accepts, routes datagrams to connections by connection ID, runs their
// timers, answers packets for unknown connections with stateless resets, and
// carries connectionless Raw datagrams (SWIM gossip) on the same port.
//
// The protocol logic lives in package ktp as a pure state machine; this
// package is only the plumbing around it. All ktp state is guarded by one
// endpoint-wide mutex. A reader goroutine feeds packets in, a timer
// goroutine fires timeouts, and API calls (stream reads and writes) act
// directly under the lock; whoever changed a connection polls its output and
// writes it to the socket before unlocking. Blocking calls wait on a
// per-connection broadcast channel that is replaced whenever anything about
// the connection changes, so waiters re-check their condition instead of
// relying on precisely targeted wakeups.
//
// Everything blocks on channels and time.Timer only, so an Endpoint over
// simnet runs inside a testing/synctest bubble on virtual time.
package endpoint

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/Sebby1770/kvstore/kvgw/netstack/ktp"
	"github.com/Sebby1770/kvstore/kvgw/netstack/wire"
)

// Options tunes an Endpoint. The zero value is usable.
type Options struct {
	// Resolve maps an address string to a net.Addr for the PacketConn.
	// Defaults to UDP resolution; simnet users pass simnet.Resolve.
	Resolve func(addr string) (net.Addr, error)
	// Logf receives diagnostics (nil = silent).
	Logf func(format string, args ...any)
	// AcceptBacklog bounds established-but-unaccepted connections (64).
	AcceptBacklog int
	// RawBacklog bounds undelivered Raw datagrams (1024); extras drop.
	RawBacklog int
	// MaxConns bounds concurrent connections (4096); extra Initials drop.
	MaxConns int
	// ResetRate bounds stateless resets sent per second (100).
	ResetRate int
}

// Errors.
var (
	ErrClosed      = errors.New("endpoint: closed")
	ErrTooManyConn = errors.New("endpoint: connection limit reached")
)

// RawPacket is a connectionless datagram received by RecvRaw.
type RawPacket struct {
	From    string
	Payload []byte
}

// Endpoint is one socket and every KTP connection on it.
type Endpoint struct {
	pc   net.PacketConn
	cfg  *ktp.Config
	opts Options

	mu      sync.Mutex
	conns   map[wire.ConnID]*Conn // by our connection ID
	initial map[wire.ConnID]*Conn // server conns by the client's original DCID
	byAddr  map[string]*Conn      // the connection Conn(ctx, addr) hands out
	dialing map[string]chan struct{}
	closed  bool
	resetAt time.Time
	resets  int

	acceptCh chan *Conn
	rawCh    chan RawPacket
	wake     chan struct{}
	done     chan struct{}
	wg       sync.WaitGroup
	buf      []byte

	stats EndpointStats
}

// EndpointStats counts endpoint-level events.
type EndpointStats struct {
	Datagrams       uint64 `json:"datagrams"`
	RawIn           uint64 `json:"raw_in"`
	RawOut          uint64 `json:"raw_out"`
	RawDropped      uint64 `json:"raw_dropped"`
	ResetsSent      uint64 `json:"resets_sent"`
	Unroutable      uint64 `json:"unroutable"`
	Accepted        uint64 `json:"accepted"`
	Dialed          uint64 `json:"dialed"`
	AcceptOverflows uint64 `json:"accept_overflows"`
}

// New starts an endpoint on pc. cfg is shared by every connection.
func New(pc net.PacketConn, cfg *ktp.Config, opts Options) (*Endpoint, error) {
	if cfg == nil || len(cfg.Identity) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("endpoint: config needs an Ed25519 identity")
	}
	if opts.Resolve == nil {
		opts.Resolve = func(a string) (net.Addr, error) { return net.ResolveUDPAddr("udp", a) }
	}
	if opts.AcceptBacklog <= 0 {
		opts.AcceptBacklog = 64
	}
	if opts.RawBacklog <= 0 {
		opts.RawBacklog = 1024
	}
	if opts.MaxConns <= 0 {
		opts.MaxConns = 4096
	}
	if opts.ResetRate <= 0 {
		opts.ResetRate = 100
	}
	e := &Endpoint{
		pc: pc, cfg: cfg, opts: opts,
		conns: make(map[wire.ConnID]*Conn), initial: make(map[wire.ConnID]*Conn),
		byAddr: make(map[string]*Conn), dialing: make(map[string]chan struct{}),
		acceptCh: make(chan *Conn, opts.AcceptBacklog),
		rawCh:    make(chan RawPacket, opts.RawBacklog),
		wake:     make(chan struct{}, 1), done: make(chan struct{}),
		buf: make([]byte, 65536),
	}
	e.wg.Add(2)
	go e.readLoop()
	go e.timerLoop()
	return e, nil
}

// Listen opens a UDP socket on addr and starts an endpoint on it.
func Listen(addr string, cfg *ktp.Config, opts Options) (*Endpoint, error) {
	pc, err := net.ListenPacket("udp", addr)
	if err != nil {
		return nil, err
	}
	e, err := New(pc, cfg, opts)
	if err != nil {
		pc.Close()
		return nil, err
	}
	return e, nil
}

// LocalAddr is the socket's address.
func (e *Endpoint) LocalAddr() net.Addr { return e.pc.LocalAddr() }

// Config is the KTP configuration shared by this endpoint's connections.
func (e *Endpoint) Config() *ktp.Config { return e.cfg }

func (e *Endpoint) logf(format string, args ...any) {
	if e.opts.Logf != nil {
		e.opts.Logf(format, args...)
	}
}

func (e *Endpoint) kick() {
	select {
	case e.wake <- struct{}{}:
	default:
	}
}

// ---- I/O loops ----

func (e *Endpoint) readLoop() {
	defer e.wg.Done()
	buf := make([]byte, 65536)
	for {
		n, addr, err := e.pc.ReadFrom(buf)
		if err != nil {
			select {
			case <-e.done:
				return
			default:
			}
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				continue
			}
			if errors.Is(err, net.ErrClosed) {
				return
			}
			e.logf("endpoint: read: %v", err)
			continue
		}
		e.handleDatagram(append([]byte(nil), buf[:n]...), addr)
	}
}

func (e *Endpoint) timerLoop() {
	defer e.wg.Done()
	t := time.NewTimer(time.Hour)
	defer t.Stop()
	for {
		e.mu.Lock()
		next, ok := time.Time{}, false
		for _, c := range e.conns {
			if at, has := c.kc.Timeout(); has && (!ok || at.Before(next)) {
				next, ok = at, true
			}
		}
		e.mu.Unlock()
		d := time.Hour
		if ok {
			d = max(time.Until(next), 0)
		}
		if !t.Stop() {
			select {
			case <-t.C:
			default:
			}
		}
		t.Reset(d)
		select {
		case <-e.done:
			return
		case <-e.wake:
		case <-t.C:
			e.mu.Lock()
			now := time.Now()
			for _, c := range e.conns {
				if at, has := c.kc.Timeout(); has && !at.After(now) {
					c.kc.HandleTimeout(now)
					e.flushLocked(c)
				}
			}
			e.mu.Unlock()
		}
	}
}

func (e *Endpoint) handleDatagram(pkt []byte, from net.Addr) {
	h, err := ktp.PeekHeader(pkt)
	if err != nil {
		return
	}
	if h.Type == wire.RawPacket {
		payload, err := ktp.OpenRaw(e.cfg, pkt)
		if err != nil {
			return
		}
		select {
		case e.rawCh <- RawPacket{From: from.String(), Payload: payload}:
			e.mu.Lock()
			e.stats.RawIn++
			e.mu.Unlock()
		default:
			e.mu.Lock()
			e.stats.RawDropped++
			e.mu.Unlock()
		}
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return
	}
	e.stats.Datagrams++
	now := time.Now()
	c := e.conns[h.DstCID]
	if c == nil && h.Type == wire.InitialPacket {
		c = e.initial[h.DstCID]
		if c == nil {
			e.acceptLocked(pkt, from, now)
			return
		}
	}
	if c == nil {
		e.stats.Unroutable++
		if h.Type != wire.ResetPacket {
			e.resetLocked(h, from, now)
		}
		return
	}
	c.kc.HandlePacket(pkt, now)
	e.flushLocked(c)
}

func (e *Endpoint) acceptLocked(pkt []byte, from net.Addr, now time.Time) {
	if len(e.conns) >= e.opts.MaxConns {
		return
	}
	kc, err := ktp.NewServerConn(e.cfg, pkt, now)
	if err != nil {
		return
	}
	c := newConn(e, kc, from, from.String())
	e.conns[kc.LocalCID()] = c
	e.initial[kc.OriginalDCID()] = c
	e.flushLocked(c)
	e.kick()
}

func (e *Endpoint) resetLocked(h wire.Header, to net.Addr, now time.Time) {
	if now.Sub(e.resetAt) >= time.Second {
		e.resetAt, e.resets = now, 0
	}
	if e.resets >= e.opts.ResetRate {
		return
	}
	e.resets++
	e.stats.ResetsSent++
	e.pc.WriteTo(ktp.StatelessReset(e.cfg, h), to)
}

// flushLocked sends everything c has queued, processes its events, wakes its
// waiters, and forgets it once it is fully closed.
func (e *Endpoint) flushLocked(c *Conn) {
	now := time.Now()
	for {
		n := c.kc.Poll(e.buf, now)
		if n == 0 {
			break
		}
		if _, err := e.pc.WriteTo(e.buf[:n], c.addr); err != nil {
			e.logf("endpoint: write to %s: %v", c.addrStr, err)
		}
	}
	for {
		ev, ok := c.kc.PollEvent()
		if !ok {
			break
		}
		switch ev.Type {
		case ktp.EventConnected:
			c.markReady()
			if !c.kc.IsClient() {
				select {
				case e.acceptCh <- c:
					e.stats.Accepted++
				default:
					e.stats.AcceptOverflows++
					c.kc.Close(ktp.CodeInternal, "accept backlog full")
				}
			}
		case ktp.EventStreamOpened:
			c.acceptQ = append(c.acceptQ, ev.Stream)
		case ktp.EventClosed:
			c.markDone(c.kc.Err())
		}
	}
	if st := c.kc.State(); st >= ktp.StateClosing {
		c.markDone(c.kc.Err())
		if st == ktp.StateClosed {
			e.forgetLocked(c)
		}
	}
	c.broadcast()
	e.kick()
}

func (e *Endpoint) forgetLocked(c *Conn) {
	delete(e.conns, c.kc.LocalCID())
	if e.initial[c.kc.OriginalDCID()] == c {
		delete(e.initial, c.kc.OriginalDCID())
	}
	if e.byAddr[c.addrStr] == c {
		delete(e.byAddr, c.addrStr)
	}
}

// ---- dial / accept ----

// Dial opens a new connection to addr and waits for the handshake.
func (e *Endpoint) Dial(ctx context.Context, addr string) (*Conn, error) {
	a, err := e.opts.Resolve(addr)
	if err != nil {
		return nil, err
	}
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil, ErrClosed
	}
	if len(e.conns) >= e.opts.MaxConns {
		e.mu.Unlock()
		return nil, ErrTooManyConn
	}
	kc, err := ktp.NewClientConn(e.cfg, time.Now())
	if err != nil {
		e.mu.Unlock()
		return nil, err
	}
	c := newConn(e, kc, a, addr)
	e.conns[kc.LocalCID()] = c
	e.stats.Dialed++
	e.flushLocked(c)
	e.mu.Unlock()

	select {
	case <-c.ready:
		if err := c.Err(); err != nil && !c.kc.Established() {
			return nil, err
		}
		return c, nil
	case <-ctx.Done():
		c.CloseWithError(ktp.CodeNoError, "dial cancelled")
		return nil, ctx.Err()
	case <-e.done:
		return nil, ErrClosed
	}
}

// Conn returns an established connection to addr, dialing one if needed.
// Concurrent callers for the same address share a single dial, so higher
// layers get one connection per peer.
func (e *Endpoint) Conn(ctx context.Context, addr string) (*Conn, error) {
	for {
		e.mu.Lock()
		if c := e.byAddr[addr]; c != nil && c.kc.Established() {
			e.mu.Unlock()
			return c, nil
		}
		if wait, busy := e.dialing[addr]; busy {
			e.mu.Unlock()
			select {
			case <-wait:
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		wait := make(chan struct{})
		e.dialing[addr] = wait
		e.mu.Unlock()

		c, err := e.Dial(ctx, addr)

		e.mu.Lock()
		delete(e.dialing, addr)
		if err == nil {
			e.byAddr[addr] = c
		}
		close(wait)
		e.mu.Unlock()
		return c, err
	}
}

// Accept returns the next connection a peer established with us.
func (e *Endpoint) Accept(ctx context.Context) (*Conn, error) {
	select {
	case c := <-e.acceptCh:
		return c, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-e.done:
		return nil, ErrClosed
	}
}

// ---- raw datagrams ----

// SendRaw sends a connectionless datagram (sealed with the cluster key).
func (e *Endpoint) SendRaw(addr string, payload []byte) error {
	a, err := e.opts.Resolve(addr)
	if err != nil {
		return err
	}
	pkt, err := ktp.SealRaw(e.cfg, payload)
	if err != nil {
		return err
	}
	e.mu.Lock()
	e.stats.RawOut++
	e.mu.Unlock()
	_, err = e.pc.WriteTo(pkt, a)
	return err
}

// RecvRaw returns the next connectionless datagram.
func (e *Endpoint) RecvRaw(ctx context.Context) (RawPacket, error) {
	select {
	case p := <-e.rawCh:
		return p, nil
	case <-ctx.Done():
		return RawPacket{}, ctx.Err()
	case <-e.done:
		return RawPacket{}, ErrClosed
	}
}

// ---- introspection / shutdown ----

// ConnInfo describes one live connection.
type ConnInfo struct {
	Addr     string    `json:"addr"`
	Peer     string    `json:"peer"` // hex Ed25519 key prefix
	Client   bool      `json:"client"`
	Stats    ktp.Stats `json:"stats"`
	LocalCID string    `json:"local_cid"`
}

// Conns snapshots every connection.
func (e *Endpoint) Conns() []ConnInfo {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]ConnInfo, 0, len(e.conns))
	for _, c := range e.conns {
		info := ConnInfo{Addr: c.addrStr, Client: c.kc.IsClient(), Stats: c.kc.Stats(), LocalCID: c.kc.LocalCID().String()}
		if id := c.kc.PeerIdentity(); id != nil {
			info.Peer = fmt.Sprintf("%x", []byte(id)[:8])
		}
		out = append(out, info)
	}
	return out
}

// Stats returns endpoint-level counters.
func (e *Endpoint) Stats() EndpointStats {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.stats
}

// Close closes every connection (telling peers), then the socket.
func (e *Endpoint) Close() error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil
	}
	for _, c := range e.conns {
		c.kc.Close(ktp.CodeNoError, "endpoint closing")
		e.flushLocked(c)
		c.markDone(ErrClosed)
		c.broadcast()
	}
	e.closed = true
	close(e.done)
	e.mu.Unlock()
	err := e.pc.Close()
	e.wg.Wait()
	return err
}
