package simnet

import (
	"net"
	"os"
	"time"
)

// PacketConn is a simnet endpoint. It implements net.PacketConn: WriteTo
// never blocks (the network queues or drops, like UDP), ReadFrom blocks until a
// packet arrives, the read deadline passes, or the conn is closed.
type PacketConn struct {
	net  *Network
	addr string

	// Guarded by net.mu: one lock for the whole network keeps delivery,
	// topology changes and reads trivially consistent, and nothing here is
	// hot enough for that to matter in tests.
	queue     []rpacket
	closed    bool
	rdeadline time.Time
	wdeadline time.Time
	rdCh      chan struct{} // closed and replaced whenever the read deadline changes

	notify chan struct{} // cap 1: "the queue may be non-empty"
	done   chan struct{} // closed by Close
}

var _ net.PacketConn = (*PacketConn)(nil)

type rpacket struct {
	from string
	data []byte
}

// signal wakes one blocked reader. Called with net.mu held.
func (c *PacketConn) signal() {
	select {
	case c.notify <- struct{}{}:
	default:
	}
}

func (c *PacketConn) opError(op string, remote net.Addr, err error) error {
	return &net.OpError{Op: op, Net: "sim", Source: Addr(c.addr), Addr: remote, Err: err}
}

// ReadFrom reads one packet into b. If b is too small the excess is discarded
// (datagram semantics) and n == len(b). Timeouts return an error for which
// errors.Is(err, os.ErrDeadlineExceeded) holds and net.Error.Timeout() is true.
func (c *PacketConn) ReadFrom(b []byte) (int, net.Addr, error) {
	n := c.net
	for {
		n.mu.Lock()
		if c.closed {
			n.mu.Unlock()
			return 0, nil, c.opError("read", nil, net.ErrClosed)
		}
		dl := c.rdeadline
		if !dl.IsZero() && !time.Now().Before(dl) {
			n.mu.Unlock()
			return 0, nil, c.opError("read", nil, os.ErrDeadlineExceeded)
		}
		if len(c.queue) > 0 {
			pkt := c.queue[0]
			c.queue[0] = rpacket{}
			c.queue = c.queue[1:]
			if len(c.queue) > 0 {
				// The notify channel holds at most one token; pass the
				// baton so a second concurrent reader also wakes.
				c.signal()
			}
			n.mu.Unlock()
			return copy(b, pkt.data), Addr(pkt.from), nil
		}
		rdCh := c.rdCh
		n.mu.Unlock()

		var timer *time.Timer
		var timeout <-chan time.Time
		if !dl.IsZero() {
			timer = time.NewTimer(time.Until(dl))
			timeout = timer.C
		}
		select {
		case <-c.notify:
		case <-rdCh:
		case <-c.done:
		case <-timeout:
		}
		if timer != nil {
			timer.Stop()
		}
	}
}

// WriteTo sends b to addr. Like UDP it reports success for packets the network
// later drops; it fails only if the conn is closed, addr is nil, or the write
// deadline has passed. b may be reused as soon as WriteTo returns.
func (c *PacketConn) WriteTo(b []byte, addr net.Addr) (int, error) {
	if addr == nil {
		return 0, c.opError("write", nil, errMissingAddress)
	}
	n := c.net
	n.mu.Lock()
	defer n.mu.Unlock()
	if c.closed {
		return 0, c.opError("write", addr, net.ErrClosed)
	}
	if !c.wdeadline.IsZero() && !time.Now().Before(c.wdeadline) {
		return 0, c.opError("write", addr, os.ErrDeadlineExceeded)
	}
	if n.closed {
		return 0, c.opError("write", addr, errNetworkClosed)
	}
	n.sendLocked(c.addr, addr.String(), b)
	return len(b), nil
}

// Close closes the conn, discards queued packets, and unblocks readers with
// net.ErrClosed. The address becomes free for a new Listen.
func (c *PacketConn) Close() error {
	n := c.net
	n.mu.Lock()
	defer n.mu.Unlock()
	if c.closed {
		return c.opError("close", nil, net.ErrClosed)
	}
	c.closed = true
	c.queue = nil
	close(c.done)
	if n.conns[c.addr] == c {
		delete(n.conns, c.addr)
	}
	return nil
}

// LocalAddr returns the conn's Addr.
func (c *PacketConn) LocalAddr() net.Addr { return Addr(c.addr) }

// SetDeadline sets both deadlines.
func (c *PacketConn) SetDeadline(t time.Time) error {
	if err := c.SetReadDeadline(t); err != nil {
		return err
	}
	return c.SetWriteDeadline(t)
}

// SetReadDeadline sets the read deadline; a zero time clears it. Blocked
// readers re-evaluate immediately, so moving the deadline into the past
// interrupts them.
func (c *PacketConn) SetReadDeadline(t time.Time) error {
	n := c.net
	n.mu.Lock()
	defer n.mu.Unlock()
	if c.closed {
		return c.opError("set", nil, net.ErrClosed)
	}
	c.rdeadline = t
	close(c.rdCh)
	c.rdCh = make(chan struct{})
	return nil
}

// SetWriteDeadline sets the write deadline; a zero time clears it. Writes
// never block, so the deadline only matters once it has passed.
func (c *PacketConn) SetWriteDeadline(t time.Time) error {
	n := c.net
	n.mu.Lock()
	defer n.mu.Unlock()
	if c.closed {
		return c.opError("set", nil, net.ErrClosed)
	}
	c.wdeadline = t
	return nil
}

// Queued returns the number of packets waiting to be read.
func (c *PacketConn) Queued() int {
	c.net.mu.Lock()
	defer c.net.mu.Unlock()
	return len(c.queue)
}
