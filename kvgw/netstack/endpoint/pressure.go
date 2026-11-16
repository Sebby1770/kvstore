package endpoint

import (
	"context"

	"github.com/Sebby1770/kvstore/kvgw/netstack/ktp"
)

// SendPressure sends application backpressure to the peer (an APP_PRESSURE
// frame; see ktp.Pressure). Only send it to peers known to understand it:
// ones that have sent pressure themselves (PeerSpeaksPressure) or that the
// application otherwise knows run this stack.
func (c *Conn) SendPressure(p ktp.Pressure) error {
	c.ep.mu.Lock()
	defer c.ep.mu.Unlock()
	if err := c.kc.SendPressure(p); err != nil {
		return err
	}
	c.ep.flushLocked(c)
	return nil
}

// PeerPressure is the peer's latest connection-scoped pressure.
func (c *Conn) PeerPressure() (ktp.Pressure, bool) {
	c.ep.mu.Lock()
	defer c.ep.mu.Unlock()
	return c.kc.PeerPressure()
}

// PeerStreamPressure is the peer's latest demand for one stream.
func (c *Conn) PeerStreamPressure(id ktp.StreamID) (ktp.Pressure, bool) {
	c.ep.mu.Lock()
	defer c.ep.mu.Unlock()
	return c.kc.PeerStreamPressure(id)
}

// PeerSpeaksPressure reports whether the peer has sent APP_PRESSURE.
func (c *Conn) PeerSpeaksPressure() bool {
	c.ep.mu.Lock()
	defer c.ep.mu.Unlock()
	return c.kc.PeerSpeaksPressure()
}

// SentPressure reports whether this side has sent APP_PRESSURE.
func (c *Conn) SentPressure() bool {
	c.ep.mu.Lock()
	defer c.ep.mu.Unlock()
	return c.kc.SentPressure()
}

// WaitPeerPressure blocks until the peer reports connection-scoped pressure
// newer than sequence number after, and returns it.
func (c *Conn) WaitPeerPressure(ctx context.Context, after uint64) (ktp.Pressure, error) {
	return c.waitPressure(ctx, after, func() (ktp.Pressure, bool) { return c.kc.PeerPressure() })
}

// WaitPeerStreamPressure blocks until the peer reports demand for stream id
// newer than sequence number after, and returns it.
func (c *Conn) WaitPeerStreamPressure(ctx context.Context, id ktp.StreamID, after uint64) (ktp.Pressure, error) {
	return c.waitPressure(ctx, after, func() (ktp.Pressure, bool) { return c.kc.PeerStreamPressure(id) })
}

func (c *Conn) waitPressure(ctx context.Context, after uint64, get func() (ktp.Pressure, bool)) (ktp.Pressure, error) {
	for {
		c.ep.mu.Lock()
		if p, ok := get(); ok && p.Seq > after {
			c.ep.mu.Unlock()
			return p, nil
		}
		if c.kc.State() >= ktp.StateClosing {
			err := c.closedErrLocked()
			c.ep.mu.Unlock()
			return ktp.Pressure{}, err
		}
		ch := c.notify
		c.ep.mu.Unlock()
		if err := c.waitCtx(ctx, ch); err != nil {
			return ktp.Pressure{}, err
		}
	}
}

// Lookup returns the established connection Conn(ctx, addr) would hand out,
// without dialing; nil if there is none.
func (e *Endpoint) Lookup(addr string) *Conn {
	e.mu.Lock()
	defer e.mu.Unlock()
	if c := e.byAddr[addr]; c != nil && c.kc.Established() {
		return c
	}
	return nil
}
