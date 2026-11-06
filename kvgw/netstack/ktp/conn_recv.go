package ktp

import (
	"crypto/ecdh"
	"crypto/ed25519"
	"crypto/hmac"
	"errors"
	"time"

	"github.com/Sebby1770/kvstore/kvgw/netstack/wire"
)

var errDropped = errors.New("ktp: packet dropped")

// HandlePacket processes one received datagram. Errors describe why a packet
// was dropped (they are counted and otherwise harmless); fatal problems close
// the connection and surface through Err and EventClosed.
func (c *Conn) HandlePacket(pkt []byte, now time.Time) error {
	if c.state == StateClosed {
		return ErrClosed
	}
	h, err := wire.ParseHeader(pkt)
	if err != nil {
		c.pktsDropped++
		return err
	}
	if h.Type == wire.ResetPacket {
		return c.handleReset(h, pkt)
	}
	if h.DstCID != c.localCID && !(h.Type == wire.InitialPacket && !c.isClient && h.DstCID == c.origDCID) {
		c.pktsDropped++
		return errDropped
	}
	c.bytesRecv += uint64(len(pkt))
	if c.acks.seen(h.PacketNumber) {
		c.pktsDropped++
		return errDropped
	}
	var payload []byte
	switch h.Type {
	case wire.InitialPacket:
		if c.isClient || c.authed {
			c.pktsDropped++
			return errDropped
		}
		payload, err = (*protector)(nil).open(h, pkt)
	case wire.HandshakePacket:
		if !c.isClient || c.recvProt != nil {
			c.pktsDropped++
			return errDropped
		}
		payload, err = (*protector)(nil).open(h, pkt)
	case wire.OneRTTPacket:
		if c.recvProt == nil {
			c.pktsDropped++
			return errDropped
		}
		payload, err = c.recvProt.open(h, pkt)
	default:
		c.pktsDropped++
		return errDropped
	}
	if err != nil {
		c.pktsDropped++
		return err
	}
	frames, err := wire.ParseFrames(payload)
	if err != nil {
		c.pktsDropped++
		c.closeWith(errViolation)
		return err
	}

	if c.state >= StateClosing {
		// Closing: answer with another CONNECTION_CLOSE (rate-limited by
		// one per received packet); draining: stay silent. A CLOSE from the
		// peer moves us from closing to draining.
		for _, f := range frames {
			if _, ok := f.(wire.ConnectionClose); ok {
				c.state = StateDraining
			}
		}
		if c.state == StateClosing {
			c.closeSent = false
		}
		return nil
	}

	switch h.Type {
	case wire.InitialPacket:
		// A retransmitted HELLO: the client has not seen our reply.
		c.pendingSHello = true
	case wire.HandshakePacket:
		if err := c.clientHandshake(h, frames, now); err != nil {
			return err
		}
	case wire.OneRTTPacket:
		if !c.isClient && !c.authed {
			if err := c.serverAuth(frames, now); err != nil {
				return err
			}
		}
		if !c.isClient {
			c.addrValidated = true
		}
	}

	ackEliciting := false
	for _, f := range frames {
		if f.AckEliciting() {
			ackEliciting = true
		}
		if err := c.handleFrame(f, h.Type, now); err != nil {
			var ce *CloseError
			if errors.As(err, &ce) {
				c.closeWith(ce)
			} else {
				c.closeWith(&CloseError{Code: CodeInternal, Reason: err.Error()})
			}
			return err
		}
		if c.state >= StateClosing {
			break
		}
	}
	c.acks.record(now, h.PacketNumber, ackEliciting)
	c.pktsRecv++
	c.lastRecv = now
	return nil
}

func (c *Conn) handleReset(h wire.Header, pkt []byte) error {
	if h.DstCID != c.localCID {
		return errDropped
	}
	body, err := (*protector)(nil).open(h, pkt)
	if err != nil || !hmac.Equal(body, resetToken(c.cfg.ClusterKey, c.remoteCID)) {
		c.pktsDropped++
		return errDropped
	}
	c.closeNow(ErrStatelessReset)
	return nil
}

// clientHandshake processes the server's SERVER_HELLO.
func (c *Conn) clientHandshake(h wire.Header, frames []wire.Frame, now time.Time) error {
	var msg []byte
	for _, f := range frames {
		if hs, ok := f.(wire.Handshake); ok {
			msg = hs.Data
		}
	}
	if msg == nil {
		return nil // e.g. an ACK-only handshake packet
	}
	sh, err := parseServerHello(msg)
	if err != nil {
		c.closeNow(&CloseError{Code: CodeHandshakeFailed, Reason: err.Error()})
		return err
	}
	pub := ed25519.PublicKey(append([]byte(nil), sh.pub...))
	th := transcriptHash(c.helloMsg, sh.core)
	if !ed25519.Verify(pub, signedMessage("server", th), sh.sig) {
		err := &CloseError{Code: CodeHandshakeFailed, Reason: "bad server signature"}
		c.closeNow(err)
		return err
	}
	peerEph, err := ecdh.X25519().NewPublicKey(sh.eph)
	if err != nil {
		c.closeNow(err)
		return err
	}
	shared, err := c.eph.ECDH(peerEph)
	if err != nil {
		c.closeNow(err)
		return err
	}
	keys, err := deriveKeys(shared, c.cfg.ClusterKey, th)
	if err != nil {
		c.closeNow(err)
		return err
	}
	if !hmac.Equal(sh.confirm, confirmTag(keys.confirm, th)) {
		c.closeNow(ErrClusterKeyMismatch)
		return ErrClusterKeyMismatch
	}
	if c.sendProt, err = newProtector(keys.c2sKey, keys.c2sIV, c.cfg.Insecure); err != nil {
		return err
	}
	if c.recvProt, err = newProtector(keys.s2cKey, keys.s2cIV, c.cfg.Insecure); err != nil {
		return err
	}
	c.th = th
	c.remoteCID = h.SrcCID
	c.peerID = pub
	c.applyPeerParams(sh.params)
	c.pendingHello = false
	if c.cfg.VerifyPeer != nil {
		if err := c.cfg.VerifyPeer(pub); err != nil {
			c.closeWith(&CloseError{Code: CodePeerRejected, Reason: "server identity rejected"})
			return err
		}
	}
	auth := append([]byte{msgClientAuth}, c.cfg.Identity.Public().(ed25519.PublicKey)...)
	auth = append(auth, ed25519.Sign(c.cfg.Identity, signedMessage("client", th))...)
	c.ctrl = append([]wire.Frame{wire.Handshake{Data: auth}}, c.ctrl...)
	c.authed = true
	c.establish(now)
	return nil
}

// serverAuth requires the client's CLIENT_AUTH before anything else in its
// first 1-RTT packets is processed. Packets without it are dropped unacked;
// the client retransmits.
func (c *Conn) serverAuth(frames []wire.Frame, now time.Time) error {
	for _, f := range frames {
		hs, ok := f.(wire.Handshake)
		if !ok {
			continue
		}
		pubB, sig, err := parseClientAuth(hs.Data)
		if err != nil {
			continue
		}
		pub := ed25519.PublicKey(append([]byte(nil), pubB...))
		if !ed25519.Verify(pub, signedMessage("client", c.th), sig) {
			c.closeWith(&CloseError{Code: CodeHandshakeFailed, Reason: "bad client signature"})
			return errDropped
		}
		if c.cfg.VerifyPeer != nil {
			if err := c.cfg.VerifyPeer(pub); err != nil {
				c.closeWith(&CloseError{Code: CodePeerRejected, Reason: "client identity rejected"})
				return err
			}
		}
		c.peerID = pub
		c.authed, c.confirmed = true, true
		c.pendingSHello = false
		c.ctrl = append(c.ctrl, wire.HandshakeDone{})
		c.establish(now)
		return nil
	}
	c.pktsDropped++
	return errDropped
}

func (c *Conn) establish(now time.Time) {
	c.state = StateEstablished
	c.establishedAt = now
	c.emit(EventConnected, 0, nil)
}

func (c *Conn) handleFrame(f wire.Frame, pt wire.PacketType, now time.Time) error {
	plaintext := pt != wire.OneRTTPacket
	switch f := f.(type) {
	case wire.Padding, wire.Ping:
	case wire.Ack:
		return c.handleAck(f, now)
	case wire.ConnectionClose:
		c.err = &CloseError{Code: f.Code, Reason: string(f.Reason), Remote: true}
		c.state = StateDraining
		c.drainUntil = now.Add(c.drainPeriod())
		c.emit(EventClosed, 0, c.err)
	case wire.Handshake:
		// HELLO / SERVER_HELLO / CLIENT_AUTH were handled before the loop.
	default:
		if plaintext {
			return errViolation // only handshake, ACK and close travel in clear
		}
		return c.handleAppFrame(f, now)
	}
	return nil
}

func (c *Conn) handleAppFrame(f wire.Frame, now time.Time) error {
	switch f := f.(type) {
	case wire.Stream:
		s, err := c.peerStream(f.StreamID)
		if err != nil || s == nil {
			return err
		}
		grew, err := s.receive(f.Offset, f.Data, f.Fin)
		if err != nil {
			return err
		}
		c.recvData += grew
		if c.recvData > c.recvMaxData {
			return errFlowControl
		}
		if s.readClosed || s.resetErr != nil {
			c.consume(grew)
		}
		if s.readable() {
			c.emit(EventStreamReadable, s.id, nil)
		}
	case wire.ResetStream:
		s, err := c.peerStream(f.StreamID)
		if err != nil || s == nil {
			return err
		}
		if s.resetErr != nil {
			return nil
		}
		if f.FinalSize < s.recvHigh || (s.finalSize >= 0 && uint64(s.finalSize) != f.FinalSize) {
			return errFinalSize
		}
		if f.FinalSize > s.recvMax {
			return errFlowControl
		}
		grew := f.FinalSize - s.recvHigh
		c.recvData += grew
		if c.recvData > c.recvMaxData {
			return errFlowControl
		}
		// Everything the peer sent but we never delivered is abandoned:
		// release its connection-level credit.
		c.consume(f.FinalSize - s.readOff)
		s.recvHigh, s.finalSize = f.FinalSize, int64(f.FinalSize)
		s.ready, s.chunks = nil, nil
		s.resetErr = &StreamResetError{Code: f.Code, Remote: true}
		c.emit(EventStreamReset, s.id, s.resetErr)
		c.emit(EventStreamReadable, s.id, nil)
		c.maybeRetire(s)
	case wire.StopSending:
		s, err := c.peerStream(f.StreamID)
		if err != nil || s == nil {
			return err
		}
		if s.stopErr == nil && !s.finAcked {
			s.stopErr = &StreamResetError{Code: f.Code, Remote: true, Stop: true}
			if !s.resetSent {
				s.resetSent = true
				c.ctrl = append(c.ctrl, wire.ResetStream{StreamID: s.id, Code: f.Code, FinalSize: s.sendNext})
				s.sendBuf, s.retx = nil, nil
			}
			c.emit(EventStopSending, s.id, s.stopErr)
			c.maybeRetire(s)
		}
	case wire.MaxData:
		if f.Max > c.sendMaxData {
			c.sendMaxData = f.Max
		}
	case wire.MaxStreamData:
		s, err := c.peerStream(f.StreamID)
		if err != nil || s == nil {
			return err
		}
		if f.Max > s.sendMax {
			s.sendMax = f.Max
			c.emit(EventStreamWritable, s.id, nil)
		}
	case wire.MaxStreams:
		if f.Max > c.peerMaxIdx {
			c.peerMaxIdx = f.Max
			c.emit(EventStreamWritable, 0, nil) // wake openers blocked on the limit
		}
	case wire.DataBlocked, wire.StreamDataBlocked:
		// Informational; our credit updates are driven by reads.
	case wire.HandshakeDone:
		if !c.isClient {
			return errViolation
		}
		c.confirmed = true
	case wire.Datagram:
		if len(c.dgIn) >= c.cfg.MaxDatagramQueue {
			c.dgIn = c.dgIn[1:]
		}
		c.dgIn = append(c.dgIn, append([]byte(nil), f.Data...))
		c.emit(EventDatagram, 0, nil)
	default:
		return errViolation
	}
	return nil
}

// peerStream resolves a stream ID from a received frame, implicitly opening
// peer-initiated streams (and any lower-numbered ones). It returns (nil, nil)
// for a stream that existed and has been retired: late frames for it are
// harmless and ignored.
func (c *Conn) peerStream(id uint64) (*stream, error) {
	if s, ok := c.streams[id]; ok {
		return s, nil
	}
	idx := id >> 1
	if id&1 == c.localBit() {
		if idx < c.nextLocalIdx {
			return nil, nil // retired local stream
		}
		return nil, errStreamState // a stream we never opened
	}
	if idx < c.peerOpenedIdx {
		return nil, nil // retired peer stream
	}
	if idx >= c.localMaxIdx {
		return nil, errStreamLimit
	}
	var s *stream
	for i := c.peerOpenedIdx; i <= idx; i++ {
		sid := i<<1 | (c.localBit() ^ 1)
		s = c.addStream(sid)
		c.acceptQ = append(c.acceptQ, sid)
		c.emit(EventStreamOpened, sid, nil)
	}
	c.peerOpenedIdx = idx + 1
	return s, nil
}

func (c *Conn) handleAck(f wire.Ack, now time.Time) error {
	if f.Largest() >= c.nextPN {
		return &CloseError{Code: CodeProtocolViolation, Reason: "ACK for a packet never sent"}
	}
	delay := time.Duration(f.Delay) * time.Microsecond
	mad := c.cfg.MaxAckDelay
	if !c.confirmed {
		mad = 0
	}
	acked, lost := c.rec.onAck(now, f, delay, mad, c.rec.cc.Window())
	for _, p := range acked {
		for _, sf := range p.frames {
			if sf.kind != fkStream {
				continue
			}
			if s, ok := c.streams[sf.streamID]; ok {
				full := len(s.sendBuf) >= c.cfg.MaxSendBuffer
				s.onAcked(sf.offset, sf.length, sf.fin)
				if full && len(s.sendBuf) < c.cfg.MaxSendBuffer {
					c.emit(EventStreamWritable, s.id, nil)
				}
				c.maybeRetire(s)
			}
		}
	}
	c.onLost(lost)
	return nil
}

// onLost puts whatever the lost packets carried that still matters back into
// the send queues.
func (c *Conn) onLost(lost []*sentPacket) {
	for _, p := range lost {
		c.requeue(p.frames)
	}
}

func (c *Conn) requeue(frames []sentFrame) {
	for _, sf := range frames {
		switch sf.kind {
		case fkStream:
			if s, ok := c.streams[sf.streamID]; ok {
				s.onLost(sf.offset, sf.length, sf.fin)
			}
		case fkMaxData:
			c.needMaxData = true
		case fkMaxStreamData:
			if s, ok := c.streams[sf.streamID]; ok && s.finalSize < 0 {
				s.needMax = true
			}
		case fkMaxStreams:
			c.needMaxStreams = true
		case fkCtrl:
			c.requeueCtrl(sf.ctrl)
		}
	}
}

func (c *Conn) requeueCtrl(f wire.Frame) {
	switch f := f.(type) {
	case wire.Handshake:
		if len(f.Data) == 0 {
			return
		}
		switch f.Data[0] {
		case msgHello:
			if c.sendProt == nil {
				c.pendingHello = true
			}
		case msgServerHello:
			if !c.authed {
				c.pendingSHello = true
			}
		case msgClientAuth:
			if !c.confirmed {
				c.ctrl = append([]wire.Frame{f}, c.ctrl...)
			}
		}
	case wire.Ping:
		// A lost PING needs no repair: the PTO will probe again if needed.
	default:
		c.ctrl = append(c.ctrl, f)
	}
}
