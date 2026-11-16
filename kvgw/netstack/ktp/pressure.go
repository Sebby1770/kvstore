package ktp

import "github.com/Sebby1770/kvstore/kvgw/netstack/wire"

// Pressure is application-level backpressure (an APP_PRESSURE frame): how
// much work, rather than how many bytes, the sender can take. See
// wire.AppPressure for the semantics of each field.
type Pressure = wire.AppPressure

// maxPressureScopes bounds how many stream-scoped pressure states a
// connection keeps for its peer; frames for further streams are ignored.
const maxPressureScopes = 4096

type pressureKey struct {
	stream bool
	id     uint64
}

func keyOf(p Pressure) pressureKey {
	if !p.Stream {
		return pressureKey{}
	}
	return pressureKey{stream: true, id: p.StreamID}
}

// pressureState is the per-connection APP_PRESSURE bookkeeping. Each scope
// (the connection, or one stream) has at most one frame pending: a newer
// SendPressure replaces an unsent older one, because only the latest state
// matters. A lost frame is resent only if it is still the latest sent for
// its scope -- the same rule MAX_DATA follows.
type pressureState struct {
	seq     uint64
	queue   []pressureKey // scopes with a pending frame, in send order
	pending map[pressureKey]Pressure
	sent    map[pressureKey]Pressure // latest sent per scope
	peer    map[pressureKey]Pressure // peer's latest per scope
	spoke   bool                     // the peer has sent APP_PRESSURE
	sentAny bool
	in, out uint64
}

func (ps *pressureState) init() {
	if ps.pending == nil {
		ps.pending = map[pressureKey]Pressure{}
		ps.sent = map[pressureKey]Pressure{}
		ps.peer = map[pressureKey]Pressure{}
	}
}

// SendPressure queues an APP_PRESSURE frame. Seq is assigned here (strictly
// increasing per connection), so callers fill in only the scope and the
// state. A stream scope must name an open stream.
func (c *Conn) SendPressure(p Pressure) error {
	if c.state != StateEstablished {
		if c.err != nil {
			return c.err
		}
		return ErrNotEstablished
	}
	if p.Load > wire.MaxLoad {
		p.Load = wire.MaxLoad
	}
	if p.Stream {
		if _, ok := c.streams[p.StreamID]; !ok {
			return ErrUnknownStream
		}
	}
	ps := &c.press
	ps.init()
	ps.seq++
	p.Seq = ps.seq
	k := keyOf(p)
	if _, queued := ps.pending[k]; !queued {
		ps.queue = append(ps.queue, k)
	}
	ps.pending[k] = p
	ps.sentAny = true
	return nil
}

// PeerPressure is the latest connection-scoped pressure the peer reported.
func (c *Conn) PeerPressure() (Pressure, bool) {
	p, ok := c.press.peer[pressureKey{}]
	return p, ok
}

// PeerStreamPressure is the latest pressure (demand) the peer reported for
// one stream.
func (c *Conn) PeerStreamPressure(id StreamID) (Pressure, bool) {
	p, ok := c.press.peer[pressureKey{stream: true, id: uint64(id)}]
	return p, ok
}

// PeerSpeaksPressure reports whether the peer has sent any APP_PRESSURE
// frame, i.e. whether it is known to understand them. Peers that do not
// (older nodes, the Rust storage server) treat an unknown frame as a protocol
// violation, so applications send pressure only to peers that spoke first --
// or speak first themselves only where they know the peer.
func (c *Conn) PeerSpeaksPressure() bool { return c.press.spoke }

// SentPressure reports whether this side has sent any APP_PRESSURE frame.
func (c *Conn) SentPressure() bool { return c.press.sentAny }

// pollPressure adds pending APP_PRESSURE frames while they fit.
func (c *Conn) pollPressure(add func(wire.Frame, sentFrame) bool) {
	ps := &c.press
	for len(ps.queue) > 0 {
		k := ps.queue[0]
		p, ok := ps.pending[k]
		if !ok { // its stream retired after it was queued
			ps.queue = ps.queue[1:]
			continue
		}
		if !add(p, sentFrame{kind: fkPressure, ctrl: p}) {
			return
		}
		ps.queue = ps.queue[1:]
		delete(ps.pending, k)
		ps.sent[k] = p
		ps.out++
	}
}

// requeuePressure resends a lost frame if nothing newer superseded it.
func (c *Conn) requeuePressure(f wire.Frame) {
	p, ok := f.(Pressure)
	if !ok {
		return
	}
	ps := &c.press
	k := keyOf(p)
	if latest, ok := ps.sent[k]; !ok || latest.Seq != p.Seq {
		return
	}
	if _, queued := ps.pending[k]; queued {
		return
	}
	if p.Stream {
		if _, ok := c.streams[p.StreamID]; !ok {
			return
		}
	}
	ps.pending[k] = p
	ps.queue = append(ps.queue, k)
}

// handlePressure records the peer's pressure if it is newer than what we
// have for that scope.
func (c *Conn) handlePressure(f Pressure) error {
	ps := &c.press
	ps.init()
	ps.spoke = true
	ps.in++
	k := keyOf(f)
	if f.Stream {
		// Like any frame naming a stream, this may be the first we hear of
		// a stream the peer opened (the frame can overtake the stream's
		// data); retired streams are ignored.
		s, err := c.peerStream(f.StreamID)
		if err != nil {
			return err
		}
		if s == nil {
			return nil
		}
	}
	old, have := ps.peer[k]
	if have && f.Seq <= old.Seq {
		return nil
	}
	if !have && len(ps.peer) >= maxPressureScopes {
		return nil
	}
	ps.peer[k] = f
	c.emit(EventPressure, StreamID(f.StreamID), nil)
	return nil
}

// forgetPressure drops a retired stream's pressure state.
func (c *Conn) forgetPressure(id uint64) {
	k := pressureKey{stream: true, id: id}
	delete(c.press.pending, k)
	delete(c.press.sent, k)
	delete(c.press.peer, k)
}
