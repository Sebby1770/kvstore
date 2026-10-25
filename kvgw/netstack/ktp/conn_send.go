package ktp

import (
	"time"

	"github.com/Sebby1770/kvstore/kvgw/netstack/wire"
)

// Poll writes the next datagram to send into buf (which must hold
// MaxPacketSize bytes) and returns its length, or 0 when there is nothing to
// send right now. Call it until it returns 0 after every HandlePacket,
// HandleTimeout, or API call that queues data.
func (c *Conn) Poll(buf []byte, now time.Time) int {
	switch c.state {
	case StateClosed, StateDraining:
		return 0
	case StateClosing:
		return c.pollClose(buf, now)
	}
	maxSize := c.cfg.MaxPacketSize
	if !c.isClient && !c.addrValidated {
		// Anti-amplification: until the client proves it can receive at its
		// address, send at most 3x what it sent us.
		budget := int64(3*c.bytesRecv) - int64(c.bytesSent)
		if budget < int64(wire.HeaderLen+64) {
			return 0
		}
		maxSize = min(maxSize, int(budget))
	}
	switch {
	case c.isClient && c.sendProt == nil:
		return c.pollInitial(buf, now)
	case !c.isClient && c.pendingSHello:
		return c.pollServerHello(buf, now, maxSize)
	}
	return c.pollOneRTT(buf, now, maxSize)
}

// pollInitial sends (or resends) the client's HELLO, padded to 1200 bytes.
func (c *Conn) pollInitial(buf []byte, now time.Time) int {
	if !c.pendingHello && !c.acks.due(now, c.cfg.MaxAckDelay) {
		return 0
	}
	payload := c.scratch[:0]
	var frames []sentFrame
	eliciting := false
	if c.pendingHello {
		hs := wire.Handshake{Data: c.helloMsg}
		payload = hs.Append(payload)
		frames = append(frames, sentFrame{kind: fkCtrl, ctrl: hs})
		eliciting = true
		c.pendingHello = false
	}
	if c.acks.anyNew {
		payload = c.acks.frame(now).Append(payload)
	}
	if pad := MinInitialSize - wire.HeaderLen - 4 - len(payload); pad > 0 {
		payload = wire.Padding{N: pad}.Append(payload)
	}
	return c.finish(buf, wire.InitialPacket, payload, frames, eliciting, now)
}

// pollServerHello sends the server's SERVER_HELLO in a plaintext packet.
func (c *Conn) pollServerHello(buf []byte, now time.Time, maxSize int) int {
	hs := wire.Handshake{Data: c.serverHelloMsg}
	payload := hs.Append(c.scratch[:0])
	if c.acks.anyNew {
		payload = c.acks.frame(now).Append(payload)
	}
	if wire.HeaderLen+len(payload)+4 > maxSize {
		return 0 // wait for more anti-amplification budget
	}
	c.pendingSHello = false
	return c.finish(buf, wire.HandshakePacket, payload, []sentFrame{{kind: fkCtrl, ctrl: hs}}, true, now)
}

func (c *Conn) pollClose(buf []byte, now time.Time) int {
	if c.closeSent || c.closeFrame == nil {
		return 0
	}
	c.closeSent = true
	if c.drainUntil.IsZero() {
		c.drainUntil = now.Add(c.drainPeriod())
	}
	payload := c.closeFrame.Append(c.scratch[:0])
	pt := wire.OneRTTPacket
	if c.sendProt == nil {
		pt = wire.InitialPacket
		if !c.isClient {
			pt = wire.HandshakePacket
		}
	}
	return c.finish(buf, pt, payload, nil, false, now)
}

// pollOneRTT assembles an application packet in priority order: ACK, control
// frames, credit updates, datagrams, retransmitted stream data, new stream
// data. Ack-eliciting content is gated by the congestion window and the
// pacer, except for PTO probes.
func (c *Conn) pollOneRTT(buf []byte, now time.Time, maxSize int) int {
	room := maxSize - wire.HeaderLen - c.sendProt.overhead()
	payload := c.scratch[:0]
	ackIncluded := false
	if c.acks.anyNew {
		payload = c.acks.frame(now).Append(payload)
		ackIncluded = true
	}

	probe := c.probes > 0
	cc := c.rec.cc
	canSend := probe
	c.paceAt = time.Time{}
	if !canSend && c.rec.bytesInFlight+maxSize <= cc.Window() {
		ok, at := c.pace.allow(now, maxSize, cc.Window(), c.mss(), &c.rec.rtt)
		canSend = ok
		if !ok && c.hasPendingWork() {
			c.paceAt = at
		}
	}

	var frames []sentFrame
	add := func(f wire.Frame, sf sentFrame) bool {
		if len(payload)+f.Len() > room {
			return false
		}
		payload = f.Append(payload)
		frames = append(frames, sf)
		return true
	}
	if canSend {
		// Control frames (CLIENT_AUTH first: the server drops anything it
		// cannot attribute to an authenticated client).
		for len(c.ctrl) > 0 {
			f := c.ctrl[0]
			if !add(f, sentFrame{kind: fkCtrl, ctrl: f}) {
				break
			}
			c.ctrl = c.ctrl[1:]
		}
		if c.needMaxData {
			if add(wire.MaxData{Max: c.recvMaxData}, sentFrame{kind: fkMaxData}) {
				c.needMaxData = false
			}
		}
		if c.needMaxStreams {
			if add(wire.MaxStreams{Max: c.localMaxIdx}, sentFrame{kind: fkMaxStreams}) {
				c.needMaxStreams = false
			}
		}
		for _, id := range c.order {
			s := c.streams[id]
			if s.needMax && s.finalSize < 0 && !s.readClosed {
				if !add(wire.MaxStreamData{StreamID: id, Max: s.recvMax}, sentFrame{kind: fkMaxStreamData, streamID: id}) {
					break
				}
				s.needMax = false
			}
		}
		for len(c.dgOut) > 0 {
			d := wire.Datagram{Data: c.dgOut[0]}
			if !add(d, sentFrame{}) {
				break
			}
			c.dgOut = c.dgOut[1:]
		}
		payload, frames = c.fillStreams(payload, frames, room)
		if c.pingQueued || (probe && len(frames) == 0) {
			if add(wire.Ping{}, sentFrame{}) {
				c.pingQueued = false
			}
		}
	}
	eliciting := len(frames) > 0
	if !eliciting {
		if !ackIncluded || !c.acks.due(now, c.cfg.MaxAckDelay) {
			return 0
		}
	}
	if probe && eliciting {
		c.probes--
	}
	n := c.finish(buf, wire.OneRTTPacket, payload, frames, eliciting, now)
	if eliciting && !probe {
		c.pace.spend(n)
	}
	return n
}

// fillStreams adds STREAM frames round-robin across streams: retransmissions
// before new data within each stream, new data bounded by stream and
// connection credit.
func (c *Conn) fillStreams(payload []byte, frames []sentFrame, room int) ([]byte, []sentFrame) {
	if len(c.order) == 0 {
		return payload, frames
	}
	start := c.rr % len(c.order)
	for k := 0; k < len(c.order); k++ {
		id := c.order[(start+k)%len(c.order)]
		s := c.streams[id]
		for {
			left := room - len(payload)
			if left < 24 {
				c.rr = (start + k) % len(c.order)
				return payload, frames
			}
			f, sf, ok := c.nextStreamFrame(s, left)
			if !ok {
				break
			}
			payload = f.Append(payload)
			frames = append(frames, sf)
		}
	}
	c.rr = start + 1
	return payload, frames
}

func (c *Conn) nextStreamFrame(s *stream, left int) (wire.Stream, sentFrame, bool) {
	if s.resetSent || s.stopErr != nil {
		return wire.Stream{}, sentFrame{}, false
	}
	finAt := func(end uint64) bool { return s.finQueued && end == s.sendEnd() }
	// Retransmission first.
	if len(s.retx) > 0 {
		r := s.retx[0]
		over := wire.StreamOverhead(s.id, r.lo, int(r.hi-r.lo))
		n := min(r.hi-r.lo, uint64(left-over))
		if n == 0 {
			return wire.Stream{}, sentFrame{}, false
		}
		fin := finAt(r.lo + n)
		s.retx.remove(r.lo, r.lo+n)
		if fin {
			s.finSent, s.finLost = true, false
		}
		c.retxBytes += n
		return wire.Stream{StreamID: s.id, Offset: r.lo, Data: s.bytesAt(r.lo, n), Fin: fin},
			sentFrame{kind: fkStream, streamID: s.id, offset: r.lo, length: n, fin: fin}, true
	}
	// New data.
	credit := c.sendMaxData - c.sentData
	limit := min(s.sendEnd(), s.sendMax, s.sendNext+credit)
	if limit > s.sendNext {
		over := wire.StreamOverhead(s.id, s.sendNext, int(limit-s.sendNext))
		n := min(limit-s.sendNext, uint64(left-over))
		if n == 0 {
			return wire.Stream{}, sentFrame{}, false
		}
		off := s.sendNext
		fin := finAt(off + n)
		s.sendNext += n
		c.sentData += n
		if fin {
			s.finSent, s.finLost = true, false
		}
		return wire.Stream{StreamID: s.id, Offset: off, Data: s.bytesAt(off, n), Fin: fin},
			sentFrame{kind: fkStream, streamID: s.id, offset: off, length: n, fin: fin}, true
	}
	// A bare FIN (all data already sent), or a lost FIN to repeat.
	if s.finQueued && s.sendNext == s.sendEnd() && (!s.finSent || s.finLost) && !s.finAcked {
		s.finSent, s.finLost = true, false
		end := s.sendEnd()
		return wire.Stream{StreamID: s.id, Offset: end, Fin: true},
			sentFrame{kind: fkStream, streamID: s.id, offset: end, fin: true}, true
	}
	return wire.Stream{}, sentFrame{}, false
}

func (c *Conn) hasPendingWork() bool {
	if len(c.ctrl) > 0 || len(c.dgOut) > 0 || c.needMaxData || c.needMaxStreams || c.pingQueued {
		return true
	}
	credit := c.sendMaxData - c.sentData
	for _, s := range c.streams {
		if s.hasSendWork(credit) || s.needMax {
			return true
		}
	}
	return false
}

// finish seals the payload into buf and records the packet.
func (c *Conn) finish(buf []byte, pt wire.PacketType, payload []byte, frames []sentFrame, eliciting bool, now time.Time) int {
	h := wire.Header{Type: pt, Version: wire.Version, DstCID: c.remoteCID, SrcCID: c.localCID, PacketNumber: c.nextPN}
	c.nextPN++
	var prot *protector
	if pt == wire.OneRTTPacket {
		prot = c.sendProt
	}
	out := prot.seal(buf[:0], h, payload)
	n := len(out)
	if &out[0] != &buf[0] {
		copy(buf, out) // seal grew past buf's capacity (never with a full-size buf)
	}
	c.rec.onSent(&sentPacket{pn: h.PacketNumber, sentAt: now, size: n, ackEliciting: eliciting, frames: frames})
	if c.acks.anyNew {
		c.acks.sent()
	}
	c.bytesSent += uint64(n)
	c.pktsSent++
	c.lastSend = now
	if eliciting {
		c.lastAckElicitSent = now
	}
	return n
}

// ---- timers ----

func (c *Conn) idleDeadline() time.Time {
	last := c.lastRecv
	if c.lastAckElicitSent.After(last) {
		last = c.lastAckElicitSent
	}
	return last.Add(max(c.cfg.IdleTimeout, c.drainPeriod()))
}

func (c *Conn) keepAliveAt() time.Time {
	if c.cfg.KeepAlive <= 0 || c.state != StateEstablished || c.pingQueued {
		return time.Time{}
	}
	last := c.lastSend
	if c.lastRecv.After(last) {
		last = c.lastRecv
	}
	return last.Add(c.cfg.KeepAlive)
}

// Timeout reports when HandleTimeout must next be called.
func (c *Conn) Timeout() (time.Time, bool) {
	if c.state == StateClosed {
		return time.Time{}, false
	}
	var t time.Time
	consider := func(x time.Time) {
		if !x.IsZero() && (t.IsZero() || x.Before(t)) {
			t = x
		}
	}
	if c.state >= StateClosing {
		consider(c.drainUntil)
		return t, !t.IsZero()
	}
	consider(c.idleDeadline())
	if c.state == StateHandshake {
		consider(c.created.Add(c.cfg.HandshakeTimeout))
	}
	if rt, ok := c.rec.timer(c.cfg.InitialRTT, c.ackDelayForPTO()); ok {
		consider(rt)
	}
	if d, ok := c.acks.deadline(c.cfg.MaxAckDelay); ok {
		consider(d)
	}
	consider(c.keepAliveAt())
	consider(c.paceAt)
	return t, !t.IsZero()
}

func (c *Conn) ackDelayForPTO() time.Duration {
	if !c.confirmed {
		return 0
	}
	return c.cfg.MaxAckDelay
}

// HandleTimeout runs whatever timers have expired at now.
func (c *Conn) HandleTimeout(now time.Time) {
	if c.state == StateClosed {
		return
	}
	if c.state >= StateClosing {
		if !c.drainUntil.IsZero() && !now.Before(c.drainUntil) {
			c.state = StateClosed
		}
		return
	}
	if c.state == StateHandshake && !now.Before(c.created.Add(c.cfg.HandshakeTimeout)) {
		c.closeNow(ErrHandshakeTimeout)
		return
	}
	if !now.Before(c.idleDeadline()) {
		c.closeNow(ErrIdleTimeout)
		return
	}
	if rt, ok := c.rec.timer(c.cfg.InitialRTT, c.ackDelayForPTO()); ok && !now.Before(rt) {
		lost, probe := c.rec.onTimeout(now, c.ackDelayForPTO())
		c.onLost(lost)
		if probe {
			// Probe with the oldest outstanding frames (they are probably
			// what was lost); the original packet stays tracked, and
			// whichever copy is acknowledged first wins.
			c.probes = 2
			if p := c.rec.oldestUnacked(); p != nil {
				c.requeue(p.frames)
			} else {
				c.pingQueued = true
			}
		}
	}
	if ka := c.keepAliveAt(); !ka.IsZero() && !now.Before(ka) {
		c.pingQueued = true
	}
	if !c.paceAt.IsZero() && !now.Before(c.paceAt) {
		c.paceAt = time.Time{}
	}
}
