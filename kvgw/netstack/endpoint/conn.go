package endpoint

import (
	"context"
	"crypto/ed25519"
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"time"

	"github.com/Sebby1770/kvstore/kvgw/netstack/ktp"
)

// Conn is a goroutine-safe handle on one KTP connection.
type Conn struct {
	ep      *Endpoint
	kc      *ktp.Conn
	addr    net.Addr
	addrStr string

	// guarded by ep.mu
	notify    chan struct{}
	acceptQ   []ktp.StreamID
	readyOnce sync.Once
	doneOnce  sync.Once
	ready     chan struct{} // closed once established or failed
	done      chan struct{} // closed once closing/closed
	err       error
}

func newConn(e *Endpoint, kc *ktp.Conn, addr net.Addr, addrStr string) *Conn {
	return &Conn{ep: e, kc: kc, addr: addr, addrStr: addrStr,
		notify: make(chan struct{}), ready: make(chan struct{}), done: make(chan struct{})}
}

func (c *Conn) broadcast() {
	close(c.notify)
	c.notify = make(chan struct{})
}

func (c *Conn) markReady() { c.readyOnce.Do(func() { close(c.ready) }) }

func (c *Conn) markDone(err error) {
	c.doneOnce.Do(func() {
		if err == nil {
			err = ErrClosed
		}
		c.err = err
		close(c.done)
	})
	c.markReady()
}

// RemoteAddr is the peer's socket address.
func (c *Conn) RemoteAddr() net.Addr { return c.addr }

// RemoteAddrString is the address string the connection was dialed with (or
// the peer's socket address for accepted connections).
func (c *Conn) RemoteAddrString() string { return c.addrStr }

// PeerIdentity is the peer's authenticated Ed25519 key.
func (c *Conn) PeerIdentity() ed25519.PublicKey {
	c.ep.mu.Lock()
	defer c.ep.mu.Unlock()
	return c.kc.PeerIdentity()
}

// Stats snapshots the transport counters.
func (c *Conn) Stats() ktp.Stats {
	c.ep.mu.Lock()
	defer c.ep.mu.Unlock()
	return c.kc.Stats()
}

// Done is closed when the connection starts closing.
func (c *Conn) Done() <-chan struct{} { return c.done }

// Err is why the connection closed (nil while open).
func (c *Conn) Err() error {
	c.ep.mu.Lock()
	defer c.ep.mu.Unlock()
	return c.err
}

// Alive reports whether the connection is established and not closing.
func (c *Conn) Alive() bool {
	c.ep.mu.Lock()
	defer c.ep.mu.Unlock()
	return c.kc.Established()
}

// CloseWithError closes the connection with an application error code.
func (c *Conn) CloseWithError(code uint64, reason string) error {
	c.ep.mu.Lock()
	defer c.ep.mu.Unlock()
	c.kc.Close(code, reason)
	c.ep.flushLocked(c)
	return nil
}

// Close closes the connection without an error.
func (c *Conn) Close() error { return c.CloseWithError(ktp.CodeNoError, "") }

// waitCtx blocks until ch fires, ctx ends, or the endpoint closes.
func (c *Conn) waitCtx(ctx context.Context, ch <-chan struct{}) error {
	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-c.ep.done:
		return ErrClosed
	}
}

func (c *Conn) closedErrLocked() error {
	if c.err != nil {
		return c.err
	}
	if err := c.kc.Err(); err != nil {
		return err
	}
	return ErrClosed
}

// OpenStream opens a stream, waiting for stream credit if the peer's limit
// is reached.
func (c *Conn) OpenStream(ctx context.Context) (*Stream, error) {
	for {
		c.ep.mu.Lock()
		id, err := c.kc.OpenStream()
		if err == nil {
			c.ep.mu.Unlock()
			return &Stream{c: c, id: id}, nil
		}
		if !errors.Is(err, ktp.ErrStreamLimit) {
			if c.kc.State() >= ktp.StateClosing {
				err = c.closedErrLocked()
			}
			c.ep.mu.Unlock()
			return nil, err
		}
		ch := c.notify
		c.ep.mu.Unlock()
		if err := c.waitCtx(ctx, ch); err != nil {
			return nil, err
		}
	}
}

// AcceptStream waits for the peer to open a stream.
func (c *Conn) AcceptStream(ctx context.Context) (*Stream, error) {
	for {
		c.ep.mu.Lock()
		if len(c.acceptQ) > 0 {
			id := c.acceptQ[0]
			c.acceptQ = c.acceptQ[1:]
			c.kc.AcceptStream() // keep ktp's own queue drained
			c.ep.mu.Unlock()
			return &Stream{c: c, id: id}, nil
		}
		if c.kc.State() >= ktp.StateClosing {
			err := c.closedErrLocked()
			c.ep.mu.Unlock()
			return nil, err
		}
		ch := c.notify
		c.ep.mu.Unlock()
		if err := c.waitCtx(ctx, ch); err != nil {
			return nil, err
		}
	}
}

// SendDatagram sends an unreliable datagram.
func (c *Conn) SendDatagram(p []byte) error {
	c.ep.mu.Lock()
	defer c.ep.mu.Unlock()
	if err := c.kc.SendDatagram(p); err != nil {
		return err
	}
	c.ep.flushLocked(c)
	return nil
}

// ReceiveDatagram waits for a datagram.
func (c *Conn) ReceiveDatagram(ctx context.Context) ([]byte, error) {
	for {
		c.ep.mu.Lock()
		if d, ok := c.kc.RecvDatagram(); ok {
			c.ep.mu.Unlock()
			return d, nil
		}
		if c.kc.State() >= ktp.StateClosing {
			err := c.closedErrLocked()
			c.ep.mu.Unlock()
			return nil, err
		}
		ch := c.notify
		c.ep.mu.Unlock()
		if err := c.waitCtx(ctx, ch); err != nil {
			return nil, err
		}
	}
}

// Stream is one bidirectional KTP stream. Read and Write may be used from
// different goroutines; deadlines follow net.Conn semantics
// (os.ErrDeadlineExceeded).
type Stream struct {
	c  *Conn
	id ktp.StreamID

	dmu           sync.Mutex
	readDeadline  time.Time
	writeDeadline time.Time
}

// ID is the stream ID.
func (s *Stream) ID() ktp.StreamID { return s.id }

// Conn is the stream's connection.
func (s *Stream) Conn() *Conn { return s.c }

func (s *Stream) deadlines() (r, w time.Time) {
	s.dmu.Lock()
	defer s.dmu.Unlock()
	return s.readDeadline, s.writeDeadline
}

// waitDeadline blocks until ch fires or the deadline passes.
func (s *Stream) waitDeadline(ch <-chan struct{}, deadline time.Time) error {
	if deadline.IsZero() {
		select {
		case <-ch:
			return nil
		case <-s.c.ep.done:
			return ErrClosed
		}
	}
	d := time.Until(deadline)
	if d <= 0 {
		return os.ErrDeadlineExceeded
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ch:
		return nil
	case <-t.C:
		return os.ErrDeadlineExceeded
	case <-s.c.ep.done:
		return ErrClosed
	}
}

// Read reads stream data, blocking until some arrives, the peer finishes
// (io.EOF), the stream is reset, or the read deadline passes.
func (s *Stream) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	ep := s.c.ep
	for {
		ep.mu.Lock()
		n, err := s.c.kc.StreamRead(s.id, p)
		if n > 0 || err != nil {
			if n > 0 {
				ep.flushLocked(s.c) // may carry a window update
			}
			if errors.Is(err, ktp.ErrUnknownStream) {
				err = io.EOF // retired after its FIN was read
			}
			ep.mu.Unlock()
			return n, err
		}
		if s.c.kc.State() >= ktp.StateClosing {
			err := s.c.closedErrLocked()
			ep.mu.Unlock()
			return 0, err
		}
		ch := s.c.notify
		ep.mu.Unlock()
		rd, _ := s.deadlines()
		if err := s.waitDeadline(ch, rd); err != nil {
			return 0, err
		}
	}
}

// Write writes all of p, blocking while the send buffer is full.
func (s *Stream) Write(p []byte) (int, error) {
	ep := s.c.ep
	written := 0
	for written < len(p) {
		ep.mu.Lock()
		n, err := s.c.kc.StreamWrite(s.id, p[written:])
		written += n
		if n > 0 {
			ep.flushLocked(s.c)
		}
		if err != nil {
			if s.c.kc.State() >= ktp.StateClosing {
				err = s.c.closedErrLocked()
			}
			ep.mu.Unlock()
			return written, err
		}
		if written == len(p) {
			ep.mu.Unlock()
			break
		}
		ch := s.c.notify
		ep.mu.Unlock()
		_, wd := s.deadlines()
		if err := s.waitDeadline(ch, wd); err != nil {
			return written, err
		}
	}
	return written, nil
}

// Close half-closes the stream: everything written is delivered, then FIN.
// Reading continues to work until the peer finishes.
func (s *Stream) Close() error {
	ep := s.c.ep
	ep.mu.Lock()
	defer ep.mu.Unlock()
	err := s.c.kc.StreamCloseWrite(s.id)
	ep.flushLocked(s.c)
	if errors.Is(err, ktp.ErrUnknownStream) {
		return nil
	}
	return err
}

// CancelRead abandons the receive half (sends STOP_SENDING).
func (s *Stream) CancelRead(code uint64) {
	ep := s.c.ep
	ep.mu.Lock()
	defer ep.mu.Unlock()
	s.c.kc.StreamStopSending(s.id, code)
	ep.flushLocked(s.c)
}

// Reset abandons the send half (sends RESET_STREAM).
func (s *Stream) Reset(code uint64) {
	ep := s.c.ep
	ep.mu.Lock()
	defer ep.mu.Unlock()
	s.c.kc.StreamReset(s.id, code)
	ep.flushLocked(s.c)
}

// SetDeadline sets both deadlines.
func (s *Stream) SetDeadline(t time.Time) error {
	s.dmu.Lock()
	s.readDeadline, s.writeDeadline = t, t
	s.dmu.Unlock()
	s.poke()
	return nil
}

// SetReadDeadline sets the read deadline (zero = none).
func (s *Stream) SetReadDeadline(t time.Time) error {
	s.dmu.Lock()
	s.readDeadline = t
	s.dmu.Unlock()
	s.poke()
	return nil
}

// SetWriteDeadline sets the write deadline (zero = none).
func (s *Stream) SetWriteDeadline(t time.Time) error {
	s.dmu.Lock()
	s.writeDeadline = t
	s.dmu.Unlock()
	s.poke()
	return nil
}

// poke wakes waiters so they re-read their deadlines.
func (s *Stream) poke() {
	s.c.ep.mu.Lock()
	s.c.broadcast()
	s.c.ep.mu.Unlock()
}
