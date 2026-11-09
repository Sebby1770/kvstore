package pmd

import (
	"errors"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// Addr is a port-level address (memif peers are named, not numbered).
type Addr string

func (Addr) Network() string  { return "pmd" }
func (a Addr) String() string { return string(a) }

// PacketConn adapts a point-to-point Port to net.PacketConn so that a KTP
// endpoint can run over it. Reads poll the port with adaptive back-off;
// writes copy into a pool buffer and enqueue it (one copy at the boundary --
// code that wants zero copies uses the Port API directly). A full ring or an
// empty pool drops the packet, exactly as a NIC queue would; KTP recovers.
type PacketConn struct {
	port          Port
	local, remote Addr

	mu       sync.Mutex // serialises readers
	pending  []Mbuf
	burst    []Mbuf
	idle     Idler
	closed   chan struct{}
	once     sync.Once
	deadline atomic.Int64 // read deadline, Unix nanoseconds (0 = none)
	drops    atomic.Uint64
}

// NewPacketConn wraps port; local and remote name the two ends.
func NewPacketConn(port Port, local, remote string) *PacketConn {
	return &PacketConn{port: port, local: Addr(local), remote: Addr(remote),
		burst: make([]Mbuf, Burst), closed: make(chan struct{})}
}

// Resolve maps address strings to pmd addresses (for endpoint.Options).
func Resolve(addr string) (net.Addr, error) { return Addr(addr), nil }

// ReadFrom returns the next packet from the peer.
func (c *PacketConn) ReadFrom(b []byte) (int, net.Addr, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for {
		if len(c.pending) > 0 {
			m := c.pending[0]
			c.pending = c.pending[1:]
			n := copy(b, m.Data())
			m.p.Put(m)
			c.idle.Busy()
			return n, c.remote, nil
		}
		select {
		case <-c.closed:
			return 0, nil, net.ErrClosed
		default:
		}
		if d := c.deadline.Load(); d != 0 && time.Now().UnixNano() >= d {
			return 0, nil, os.ErrDeadlineExceeded
		}
		if n := c.port.RxBurst(c.burst); n > 0 {
			c.pending = append(c.pending[:0], c.burst[:n]...)
			continue
		}
		c.idle.Idle()
	}
}

// WriteTo sends b to the peer (addr is ignored: the link is point to point).
func (c *PacketConn) WriteTo(b []byte, addr net.Addr) (int, error) {
	select {
	case <-c.closed:
		return 0, net.ErrClosed
	default:
	}
	pool := c.port.Pool()
	m, err := pool.Get()
	if err != nil {
		c.countDrop()
		return len(b), nil
	}
	if len(b) > len(m.Room()) {
		pool.Put(m)
		return 0, errors.New("pmd: packet larger than the mbuf data room")
	}
	copy(m.Room(), b)
	m.SetLen(len(b))
	if c.port.TxBurst([]Mbuf{m}) == 0 {
		pool.Put(m)
		c.countDrop()
	}
	return len(b), nil
}

func (c *PacketConn) countDrop() { c.drops.Add(1) }

// Drops is how many packets WriteTo dropped (pool empty or ring full).
func (c *PacketConn) Drops() uint64 { return c.drops.Load() }

// Close stops the conn (the port is closed too).
func (c *PacketConn) Close() error {
	c.once.Do(func() { close(c.closed) })
	return nil
}

func (c *PacketConn) LocalAddr() net.Addr { return c.local }

func (c *PacketConn) SetDeadline(t time.Time) error { return c.SetReadDeadline(t) }

// SetReadDeadline bounds ReadFrom; a blocked reader notices at its next poll.
func (c *PacketConn) SetReadDeadline(t time.Time) error {
	if t.IsZero() {
		c.deadline.Store(0)
	} else {
		c.deadline.Store(t.UnixNano())
	}
	return nil
}

func (c *PacketConn) SetWriteDeadline(time.Time) error { return nil }
