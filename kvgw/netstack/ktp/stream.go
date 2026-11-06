package ktp

import (
	"errors"
	"io"
	"sort"
)

// StreamID identifies a stream within a connection. Client-initiated streams
// are even, server-initiated streams odd.
type StreamID = uint64

// Stream errors.
var (
	ErrStreamLimit   = errors.New("ktp: peer's stream limit reached")
	ErrUnknownStream = errors.New("ktp: no such stream")
	ErrWriteClosed   = errors.New("ktp: stream write side closed")
	ErrReadClosed    = errors.New("ktp: stream read side closed")
)

// StreamResetError is returned by reads after the peer reset the stream, and
// by writes after the peer sent STOP_SENDING.
type StreamResetError struct {
	Code   uint64
	Remote bool
	Stop   bool // STOP_SENDING rather than RESET_STREAM
}

func (e *StreamResetError) Error() string {
	what := "reset"
	if e.Stop {
		what = "stop-sending"
	}
	who := "local"
	if e.Remote {
		who = "peer"
	}
	return "ktp: stream " + what + " by " + who + " (code " + itoa(e.Code) + ")"
}

func itoa(v uint64) string {
	if v == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for v > 0 {
		i--
		b[i] = byte('0' + v%10)
		v /= 10
	}
	return string(b[i:])
}

type chunk struct {
	off  uint64
	data []byte
}

// stream is both halves of one bidirectional stream.
type stream struct {
	id uint64

	// ---- send half ----
	sendBuf   []byte   // bytes from sendBase: unacknowledged or unsent
	sendBase  uint64   // every offset below is acknowledged
	sendNext  uint64   // first never-transmitted offset
	sendMax   uint64   // peer's MAX_STREAM_DATA
	acked     rangeSet // acknowledged offsets >= sendBase
	retx      rangeSet // lost ranges awaiting retransmission
	finQueued bool     // app closed the write side
	finSent   bool
	finAcked  bool
	finLost   bool // FIN must be re-sent (possibly with no data)
	resetSent bool // we reset our send half
	stopErr   *StreamResetError

	// ---- receive half ----
	ready          []byte  // contiguous bytes from readOff, ready to read
	readOff        uint64  // offset of ready[0]; app has consumed everything below
	chunks         []chunk // out-of-order data beyond readOff+len(ready)
	recvHigh       uint64  // highest offset+1 seen (for connection flow control)
	finalSize      int64   // -1 until FIN or RESET reveals it
	recvMax        uint64  // our advertised MAX_STREAM_DATA
	window         uint64  // receive window to keep ahead of readOff
	resetErr       *StreamResetError
	resetDelivered bool // the application has read resetErr
	readClosed     bool // app cancelled reading (we sent STOP_SENDING)
	needMax        bool // a MAX_STREAM_DATA update is due
}

func newStream(id, sendMax, recvWindow uint64) *stream {
	return &stream{id: id, sendMax: sendMax, finalSize: -1, recvMax: recvWindow, window: recvWindow}
}

func (s *stream) sendEnd() uint64 { return s.sendBase + uint64(len(s.sendBuf)) }

// bytesAt returns stream data [off, off+n) from the send buffer.
func (s *stream) bytesAt(off, n uint64) []byte {
	start := off - s.sendBase
	return s.sendBuf[start : start+n]
}

// onAcked records acknowledgement of [off, off+n) (and FIN) and frees the
// acknowledged prefix of the send buffer.
func (s *stream) onAcked(off, n uint64, fin bool) {
	if fin {
		s.finAcked = true
		s.finLost = false
	}
	if s.resetSent || n == 0 || off+n <= s.sendBase {
		return // a reset discarded the buffer; late acks are moot
	}
	lo := max(off, s.sendBase)
	s.acked.add(lo, off+n)
	s.retx.remove(lo, off+n)
	if len(s.acked) > 0 && s.acked[0].lo == s.sendBase {
		adv := min(s.acked[0].hi-s.sendBase, uint64(len(s.sendBuf)))
		s.sendBuf = s.sendBuf[adv:]
		s.sendBase += adv
		s.acked = s.acked[1:]
		s.retx.trimBelow(s.sendBase)
		if len(s.sendBuf) == 0 {
			s.sendBuf = nil // let the backing array go
		}
	}
}

// onLost queues the still-unacknowledged parts of [off, off+n) for
// retransmission.
func (s *stream) onLost(off, n uint64, fin bool) {
	if s.resetSent {
		return
	}
	if fin && !s.finAcked {
		s.finLost = true
	}
	if n == 0 || off+n <= s.sendBase {
		return
	}
	for _, g := range s.acked.gaps(max(off, s.sendBase), off+n) {
		s.retx.add(g.lo, g.hi)
	}
}

// sendDone: nothing more will ever be sent on this half.
func (s *stream) sendDone() bool {
	return s.resetSent || s.stopErr != nil || (s.finQueued && s.finAcked && len(s.sendBuf) == 0)
}

// recvDone: everything the peer will send has been consumed (or abandoned).
func (s *stream) recvDone() bool {
	if s.readClosed {
		return true
	}
	if s.resetErr != nil {
		// Keep the stream until the application has seen the reset;
		// otherwise its next read finds no stream and mistakes the abort
		// for a clean end.
		return s.resetDelivered
	}
	return s.finalSize >= 0 && s.readOff == uint64(s.finalSize) && len(s.ready) == 0
}

// receive files a STREAM frame. It returns how much new connection-level
// flow-control credit the frame consumed (growth of recvHigh), or an error
// for protocol violations.
func (s *stream) receive(off uint64, data []byte, fin bool) (uint64, error) {
	end := off + uint64(len(data))
	if end > s.recvMax {
		return 0, errFlowControl
	}
	if s.finalSize >= 0 && (end > uint64(s.finalSize) || (fin && end != uint64(s.finalSize))) {
		return 0, errFinalSize
	}
	if fin {
		if end < s.recvHigh {
			return 0, errFinalSize
		}
		s.finalSize = int64(end)
	}
	var grew uint64
	if end > s.recvHigh {
		grew = end - s.recvHigh
		s.recvHigh = end
	}
	if s.resetErr != nil || s.readClosed {
		return grew, nil // keep flow-control accounting, drop the bytes
	}
	have := s.readOff + uint64(len(s.ready))
	if end <= have {
		return grew, nil // duplicate
	}
	if off < have {
		data = data[have-off:]
		off = have
	}
	s.chunks = append(s.chunks, chunk{off: off, data: append([]byte(nil), data...)})
	sort.Slice(s.chunks, func(i, j int) bool { return s.chunks[i].off < s.chunks[j].off })
	// Pull every chunk that now touches the contiguous end into ready.
	keep := s.chunks[:0]
	for _, c := range s.chunks {
		have = s.readOff + uint64(len(s.ready))
		cend := c.off + uint64(len(c.data))
		switch {
		case cend <= have:
			// fully duplicate
		case c.off <= have:
			s.ready = append(s.ready, c.data[have-c.off:]...)
		default:
			keep = append(keep, c)
		}
	}
	s.chunks = keep
	return grew, nil
}

// read copies ready bytes into p. It reports whether the window should be
// extended (a MAX_STREAM_DATA is due).
func (s *stream) read(p []byte) (int, error) {
	if s.readClosed {
		return 0, ErrReadClosed
	}
	if s.resetErr != nil {
		s.resetDelivered = true
		return 0, s.resetErr
	}
	if len(s.ready) == 0 {
		if s.finalSize >= 0 && s.readOff == uint64(s.finalSize) {
			return 0, io.EOF
		}
		return 0, nil
	}
	n := copy(p, s.ready)
	s.ready = s.ready[n:]
	if len(s.ready) == 0 {
		s.ready = nil
	}
	s.readOff += uint64(n)
	if s.finalSize < 0 && s.recvMax-s.readOff < s.window/2 {
		s.recvMax = s.readOff + s.window
		s.needMax = true
	}
	return n, nil
}

func (s *stream) readable() bool {
	return len(s.ready) > 0 || s.resetErr != nil ||
		(s.finalSize >= 0 && s.readOff == uint64(s.finalSize))
}

// hasSendWork reports whether the stream has something to transmit given the
// connection's remaining credit for new data.
func (s *stream) hasSendWork(connCredit uint64) bool {
	if s.resetSent || s.stopErr != nil {
		return false
	}
	if !s.retx.empty() || s.finLost {
		return true
	}
	if s.sendNext < s.sendEnd() && s.sendNext < s.sendMax && connCredit > 0 {
		return true
	}
	return s.finQueued && !s.finSent && s.sendNext == s.sendEnd()
}
