// Package segroute is source routing for KTP, in the spirit of SRv6: the
// sender writes the path into the packet. Conn wraps a node's packet socket
// and, for destinations with a route, prepends a segment list naming the
// relays to traverse; each Relay pops itself off the list and forwards to
// the next segment, the last one delivering the original packet to the
// destination. Replies need no header: every relay remembers, per KTP
// connection, which hop a flow came from, and sends packets addressed to
// that connection back the way they came.
//
// Relays never see plaintext -- KTP is encrypted end to end -- and keep only
// a soft flow table (connection ID -> previous hop) that expires when idle.
// Multipath sessions use segment routes to pin subflows to disjoint paths
// through the network: one direct, one through a relay on another
// provider's backbone, one through a long-haul relay.
//
// Header (before the KTP packet):
//
//	0xE5 'S' 'R' | segments left | segment count | count × (1B length, address)
package segroute

import (
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Sebby1770/kvstore/kvgw/netstack/wire"
)

const (
	tag         = 0xE5
	maxSegments = 8
)

// ErrBadHeader reports a malformed segment-routing header.
var ErrBadHeader = errors.New("segroute: malformed header")

// Header is a decoded segment list.
type Header struct {
	Left     int // segments still to visit (the next is Segments[len-Left])
	Segments []string
}

// Encode prepends the header to pkt.
func (h Header) Encode(pkt []byte) []byte {
	out := []byte{tag, 'S', 'R', byte(h.Left), byte(len(h.Segments))}
	for _, s := range h.Segments {
		out = append(out, byte(len(s)))
		out = append(out, s...)
	}
	return append(out, pkt...)
}

// Decode splits a packet into its header and payload.
func Decode(b []byte) (Header, []byte, error) {
	if len(b) < 5 || b[0] != tag || b[1] != 'S' || b[2] != 'R' {
		return Header{}, nil, ErrBadHeader
	}
	h := Header{Left: int(b[3])}
	n := int(b[4])
	if n == 0 || n > maxSegments || h.Left > n {
		return Header{}, nil, ErrBadHeader
	}
	b = b[5:]
	for i := 0; i < n; i++ {
		if len(b) < 1 || len(b) < 1+int(b[0]) || b[0] == 0 {
			return Header{}, nil, ErrBadHeader
		}
		h.Segments = append(h.Segments, string(b[1:1+int(b[0])]))
		b = b[1+int(b[0]):]
	}
	return h, b, nil
}

// IsRouted reports whether b starts with a segment-routing header.
func IsRouted(b []byte) bool { return len(b) > 0 && b[0] == tag }

// Conn wraps a packet socket and source-routes packets to destinations
// that have a route. Everything else passes through untouched.
type Conn struct {
	net.PacketConn
	resolve func(string) (net.Addr, error)
	mu      sync.RWMutex
	routes  map[string][]string // destination -> relays
}

// Wrap source-routes over pc; resolve maps relay addresses (as for
// endpoint.Options.Resolve).
func Wrap(pc net.PacketConn, resolve func(string) (net.Addr, error)) *Conn {
	return &Conn{PacketConn: pc, resolve: resolve, routes: map[string][]string{}}
}

// SetRoute sends packets for dst through relays, in order (nil removes it).
func (c *Conn) SetRoute(dst string, relays ...string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(relays) == 0 {
		delete(c.routes, dst)
		return
	}
	c.routes[dst] = append([]string(nil), relays...)
}

// WriteTo sends b to addr, through its route if it has one.
func (c *Conn) WriteTo(b []byte, addr net.Addr) (int, error) {
	c.mu.RLock()
	relays := c.routes[addr.String()]
	c.mu.RUnlock()
	if len(relays) == 0 {
		return c.PacketConn.WriteTo(b, addr)
	}
	segs := append(append([]string(nil), relays[1:]...), addr.String())
	first, err := c.resolve(relays[0])
	if err != nil {
		return 0, err
	}
	if _, err := c.PacketConn.WriteTo(Header{Left: len(segs), Segments: segs}.Encode(b), first); err != nil {
		return 0, err
	}
	return len(b), nil
}

// Relay forwards source-routed packets.
type Relay struct {
	pc      net.PacketConn
	resolve func(string) (net.Addr, error)
	idle    time.Duration

	mu    sync.Mutex
	flows map[wire.ConnID]flow

	forwarded, returned, dropped atomic.Uint64
	done                         chan struct{}
}

type flow struct {
	prev net.Addr
	seen time.Time
}

// RelayStats counts a relay's traffic.
type RelayStats struct {
	Forwarded, Returned, Dropped uint64
	Flows                        int
}

// NewRelay serves on pc until Close.
func NewRelay(pc net.PacketConn, resolve func(string) (net.Addr, error)) *Relay {
	r := &Relay{pc: pc, resolve: resolve, idle: time.Minute, flows: map[wire.ConnID]flow{}, done: make(chan struct{})}
	go r.loop()
	return r
}

func (r *Relay) loop() {
	buf := make([]byte, 65536)
	for {
		n, from, err := r.pc.ReadFrom(buf)
		if err != nil {
			select {
			case <-r.done:
				return
			default:
			}
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}
			return
		}
		r.handle(append([]byte(nil), buf[:n]...), from)
	}
}

func (r *Relay) handle(b []byte, from net.Addr) {
	now := time.Now()
	if !IsRouted(b) {
		// A reply: send it back towards whoever brought us this flow.
		h, err := wire.ParseHeader(b)
		if err != nil {
			r.dropped.Add(1)
			return
		}
		r.mu.Lock()
		f, ok := r.flows[h.DstCID]
		if ok {
			f.seen = now
			r.flows[h.DstCID] = f
		}
		r.mu.Unlock()
		if !ok {
			r.dropped.Add(1)
			return
		}
		r.pc.WriteTo(b, f.prev)
		r.returned.Add(1)
		return
	}
	h, inner, err := Decode(b)
	if err != nil || h.Left == 0 {
		r.dropped.Add(1)
		return
	}
	// Remember the way back for this flow (the sender's connection ID).
	if kh, err := wire.ParseHeader(inner); err == nil {
		r.mu.Lock()
		r.flows[kh.SrcCID] = flow{prev: from, seen: now}
		if len(r.flows) > 65536 {
			for k, f := range r.flows {
				if now.Sub(f.seen) > r.idle {
					delete(r.flows, k)
				}
			}
		}
		r.mu.Unlock()
	}
	next := h.Segments[len(h.Segments)-h.Left]
	h.Left--
	to, err := r.resolve(next)
	if err != nil {
		r.dropped.Add(1)
		return
	}
	out := inner
	if h.Left > 0 {
		out = h.Encode(inner)
	}
	r.pc.WriteTo(out, to)
	r.forwarded.Add(1)
}

// Stats snapshots the relay.
func (r *Relay) Stats() RelayStats {
	r.mu.Lock()
	n := len(r.flows)
	r.mu.Unlock()
	return RelayStats{Forwarded: r.forwarded.Load(), Returned: r.returned.Load(), Dropped: r.dropped.Load(), Flows: n}
}

// Close stops the relay and closes its socket.
func (r *Relay) Close() error {
	close(r.done)
	return r.pc.Close()
}
