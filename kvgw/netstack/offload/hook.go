package offload

import (
	"hash/fnv"
	"net"
	"sync"
	"time"

	"github.com/Sebby1770/kvstore/kvgw/netstack/endpoint"
)

// Hook attaches programs to an endpoint (it implements
// endpoint.PacketFilter). Ingress programs run in order on every received
// datagram before the KTP stack sees it; the first verdict other than PASS
// decides the packet. Egress programs see every datagram about to be sent;
// any verdict but PASS drops it.
type Hook struct {
	mu      sync.RWMutex
	ingress []*Loaded
	egress  []*Loaded
	// Clock is the programs' ktime (default: wall clock nanoseconds).
	Clock func() int64
}

// NewHook returns an empty hook; install it with ep.SetFilter(h).
func NewHook() *Hook { return &Hook{} }

// AttachIngress appends a program to the ingress chain.
func (h *Hook) AttachIngress(l *Loaded) {
	h.mu.Lock()
	h.ingress = append(h.ingress, l)
	h.mu.Unlock()
}

// AttachEgress appends a program to the egress chain.
func (h *Hook) AttachEgress(l *Loaded) {
	h.mu.Lock()
	h.egress = append(h.egress, l)
	h.mu.Unlock()
}

// Detach removes a program from both chains.
func (h *Hook) Detach(l *Loaded) {
	h.mu.Lock()
	defer h.mu.Unlock()
	drop := func(s []*Loaded) []*Loaded {
		out := s[:0]
		for _, x := range s {
			if x != l {
				out = append(out, x)
			}
		}
		return out
	}
	h.ingress, h.egress = drop(h.ingress), drop(h.egress)
}

// Programs lists the attached programs (ingress first).
func (h *Hook) Programs() []*Loaded {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return append(append([]*Loaded(nil), h.ingress...), h.egress...)
}

func (h *Hook) now() int64 {
	if h.Clock != nil {
		return h.Clock()
	}
	return time.Now().UnixNano()
}

// PeerKey is the CtxSrcHash a program sees for an address: FNV-1a over its
// string form. Go code uses it to address per-peer map entries.
func PeerKey(a net.Addr) uint64 {
	if a == nil {
		return 0
	}
	return PeerKeyString(a.String())
}

// PeerKeyString is PeerKey for an address string.
func PeerKeyString(s string) uint64 {
	f := fnv.New64a()
	f.Write([]byte(s))
	return f.Sum64()
}

// Ingress implements endpoint.PacketFilter.
func (h *Hook) Ingress(pkt []byte, from net.Addr) (endpoint.Verdict, net.Addr) {
	h.mu.RLock()
	progs := h.ingress
	h.mu.RUnlock()
	if len(progs) == 0 {
		return endpoint.VerdictPass, nil
	}
	in := &Input{Packet: pkt, PeerKey: PeerKey(from), Now: h.now()}
	for _, l := range progs {
		res := l.Run(in)
		switch res.Action {
		case XDPPass:
			continue
		case XDPDrop:
			return endpoint.VerdictDrop, nil
		case XDPTX:
			return endpoint.VerdictTX, nil
		case XDPRedirect:
			if res.Queue != nil {
				res.Queue.deliver(res.QueueIdx, Packet{Data: append([]byte(nil), pkt...), From: from})
				return endpoint.VerdictConsumed, nil
			}
			return endpoint.VerdictRedirect, res.Redirect
		default:
			return endpoint.VerdictAborted, nil
		}
	}
	return endpoint.VerdictPass, nil
}

// Egress implements endpoint.PacketFilter.
func (h *Hook) Egress(pkt []byte, to net.Addr) endpoint.Verdict {
	h.mu.RLock()
	progs := h.egress
	h.mu.RUnlock()
	if len(progs) == 0 {
		return endpoint.VerdictPass
	}
	in := &Input{Packet: pkt, PeerKey: PeerKey(to), Egress: true, Now: h.now()}
	for _, l := range progs {
		if l.Run(in).Action != XDPPass {
			return endpoint.VerdictDrop
		}
	}
	return endpoint.VerdictPass
}
