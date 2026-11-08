package ktp

import (
	"errors"
	"testing"
	"time"

	"github.com/Sebby1770/kvstore/kvgw/netstack/simnet"
)

// A peer that vanishes is declared dead after IdleTimeout even though PTO
// probes and keep-alive pings keep going out: the idle timer restarts on the
// first ack-eliciting packet after a receive, not on every one (RFC 9000
// §10.1). Otherwise a connection to a crashed host lives -- and pings --
// forever.
func TestVanishedPeerTimesOutDespiteProbesAndPings(t *testing.T) {
	for _, tc := range []struct {
		name string
		data bool
	}{{"keep-alive pings", false}, {"data in flight", true}} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, 16, cleanLink)
			h.cfg[0].IdleTimeout = 3 * time.Second
			h.cfg[0].KeepAlive = 500 * time.Millisecond
			h.dial()
			h.run(5*time.Second, h.established)
			if tc.data {
				id, err := h.conns[0].OpenStream()
				if err != nil {
					t.Fatal(err)
				}
				h.conns[0].StreamWrite(id, make([]byte, 50_000))
			}
			h.links[0], h.links[1] = simnet.Link{Loss: 1}, simnet.Link{Loss: 1} // the peer is gone
			h.flush()
			start := h.now
			h.run(2*time.Minute, func() bool { return h.conns[0].State() == StateClosed })
			if !errors.Is(h.conns[0].Err(), ErrIdleTimeout) {
				t.Fatalf("after %v: state %v, err %v; want an idle timeout", h.now.Sub(start), h.conns[0].State(), h.conns[0].Err())
			}
			if took := h.now.Sub(start); took > 5*time.Second {
				t.Fatalf("declared dead after %v with a 3s idle timeout", took)
			}
		})
	}
}
