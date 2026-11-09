package endpoint

import (
	"net"
	"sync/atomic"
)

// A PacketFilter runs on every datagram at the edge of the endpoint --
// ingress before any KTP processing (before connection lookup, before
// decryption), egress just before the socket write. It is the XDP / TC hook
// of this userspace stack: package offload attaches verified eBPF-style
// programs here so that rate limiting, circuit breaking, heartbeat replies
// and load balancing happen in the data path, without a connection, a
// goroutine, or the application ever seeing the packet.
type PacketFilter interface {
	// Ingress may rewrite pkt in place. For VerdictRedirect it returns the
	// address to forward to.
	Ingress(pkt []byte, from net.Addr) (Verdict, net.Addr)
	// Egress may drop a packet about to be sent (VerdictPass or VerdictDrop).
	Egress(pkt []byte, to net.Addr) Verdict
}

// Verdict is what a filter decided, numbered like XDP's actions.
type Verdict int

const (
	VerdictAborted  Verdict = 0 // program error: drop and count
	VerdictDrop     Verdict = 1
	VerdictPass     Verdict = 2 // continue into the stack
	VerdictTX       Verdict = 3 // send the (rewritten) packet back where it came from
	VerdictRedirect Verdict = 4 // forward to another address (encapsulated, see below)
	VerdictConsumed Verdict = 5 // the filter took the packet (e.g. into a user queue)
)

// FilterStats counts filter verdicts.
type FilterStats struct {
	Dropped, Aborted, TX, Redirected, Consumed, EgressDropped, Decapsulated uint64
}

// Redirected packets are encapsulated so the receiver learns the original
// sender -- otherwise a backend behind a load balancer would answer the
// balancer instead of the client. The header is:
//
//	0xE2 'L' 'B' | 1B address length | address | original packet
//
// Receivers decapsulate only packets that arrive from addresses listed with
// TrustEncapsulation: anyone else could otherwise claim any source.
const (
	encapTag = 0xE2
)

func encapsulate(pkt []byte, origin net.Addr) []byte {
	a := origin.String()
	if len(a) > 255 {
		return nil
	}
	out := make([]byte, 0, 4+len(a)+len(pkt))
	out = append(out, encapTag, 'L', 'B', byte(len(a)))
	out = append(out, a...)
	return append(out, pkt...)
}

func decapsulate(pkt []byte) (inner []byte, origin string, ok bool) {
	if len(pkt) < 4 || pkt[0] != encapTag || pkt[1] != 'L' || pkt[2] != 'B' {
		return nil, "", false
	}
	n := int(pkt[3])
	if len(pkt) < 4+n {
		return nil, "", false
	}
	return pkt[4+n:], string(pkt[4 : 4+n]), true
}

// SetFilter installs (or with nil removes) the packet filter.
func (e *Endpoint) SetFilter(f PacketFilter) {
	e.hmu.Lock()
	e.filter = f
	e.hmu.Unlock()
}

// TrustEncapsulation lists load balancers whose encapsulated packets this
// endpoint unwraps (replacing the old list).
func (e *Endpoint) TrustEncapsulation(addrs ...string) {
	m := make(map[string]bool, len(addrs))
	for _, a := range addrs {
		m[a] = true
	}
	e.hmu.Lock()
	e.trustEncap = m
	e.hmu.Unlock()
}

// FilterStats snapshots the filter counters.
func (e *Endpoint) FilterStats() FilterStats {
	s := &e.fstats
	return FilterStats{Dropped: atomic.LoadUint64(&s.Dropped), Aborted: atomic.LoadUint64(&s.Aborted), TX: atomic.LoadUint64(&s.TX),
		Redirected: atomic.LoadUint64(&s.Redirected), Consumed: atomic.LoadUint64(&s.Consumed),
		EgressDropped: atomic.LoadUint64(&s.EgressDropped), Decapsulated: atomic.LoadUint64(&s.Decapsulated)}
}

// ingress runs decapsulation and the filter; nil means the packet is done.
func (e *Endpoint) ingress(pkt []byte, from net.Addr) ([]byte, net.Addr) {
	e.hmu.RLock()
	f, trust := e.filter, e.trustEncap
	e.hmu.RUnlock()
	if len(pkt) > 0 && pkt[0] == encapTag && trust[from.String()] {
		inner, origin, ok := decapsulate(pkt)
		if !ok {
			return nil, nil
		}
		a, err := e.opts.Resolve(origin)
		if err != nil {
			return nil, nil
		}
		atomic.AddUint64(&e.fstats.Decapsulated, 1)
		pkt, from = inner, a
	}
	if f == nil {
		return pkt, from
	}
	v, to := f.Ingress(pkt, from)
	switch v {
	case VerdictPass:
		return pkt, from
	case VerdictTX:
		atomic.AddUint64(&e.fstats.TX, 1)
		e.pc.WriteTo(pkt, from)
	case VerdictRedirect:
		if to != nil {
			if enc := encapsulate(pkt, from); enc != nil {
				atomic.AddUint64(&e.fstats.Redirected, 1)
				e.pc.WriteTo(enc, to)
				return nil, nil
			}
		}
		atomic.AddUint64(&e.fstats.Aborted, 1)
	case VerdictConsumed:
		atomic.AddUint64(&e.fstats.Consumed, 1)
	case VerdictDrop:
		atomic.AddUint64(&e.fstats.Dropped, 1)
	default:
		atomic.AddUint64(&e.fstats.Aborted, 1)
	}
	return nil, nil
}

// write sends a datagram through the egress filter.
func (e *Endpoint) write(pkt []byte, to net.Addr) (int, error) {
	e.hmu.RLock()
	f := e.filter
	e.hmu.RUnlock()
	if f != nil && f.Egress(pkt, to) != VerdictPass {
		atomic.AddUint64(&e.fstats.EgressDropped, 1)
		return len(pkt), nil // as if the network lost it: KTP recovers
	}
	return e.pc.WriteTo(pkt, to)
}

// WriteRaw sends bytes that are not a KTP packet (heartbeat probes, for
// instance) from this endpoint's socket, through the egress filter.
func (e *Endpoint) WriteRaw(pkt []byte, addr string) error {
	a, err := e.opts.Resolve(addr)
	if err != nil {
		return err
	}
	_, err = e.write(pkt, a)
	return err
}
