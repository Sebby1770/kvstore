package vtime

import (
	"encoding/binary"
	"errors"
)

// Time-transfer messages travel as Raw datagrams tagged TagPTP:
//
//	0 tag | 1 type | 2..10 sequence | 10..18 time (ns) | 18..22 epoch
//
// SYNC carries the master's send time t1, DELAY_RESP the master's receive
// time t4 of the matching DELAY_REQ; DELAY_REQ carries nothing but its
// sequence. Epoch is the grandmaster's term: a follower ignores messages
// from a master it has not adopted, so a deposed master's stragglers cannot
// skew it.
const (
	TagPTP = 0xA1

	MsgSync      = 1
	MsgDelayReq  = 2
	MsgDelayResp = 3

	ptpLen = 22
)

// Message is one decoded time-transfer message.
type Message struct {
	Type  byte
	Seq   uint64
	Time  int64
	Epoch uint32
}

// Encode renders the message.
func (m Message) Encode() []byte {
	b := make([]byte, ptpLen)
	b[0], b[1] = TagPTP, m.Type
	binary.BigEndian.PutUint64(b[2:], m.Seq)
	binary.BigEndian.PutUint64(b[10:], uint64(m.Time))
	binary.BigEndian.PutUint32(b[18:], m.Epoch)
	return b
}

// ErrBadMessage reports a malformed time-transfer datagram.
var ErrBadMessage = errors.New("vtime: malformed time-transfer message")

// Decode parses a message.
func Decode(b []byte) (Message, error) {
	if len(b) != ptpLen || b[0] != TagPTP || b[1] < MsgSync || b[1] > MsgDelayResp {
		return Message{}, ErrBadMessage
	}
	return Message{Type: b[1], Seq: binary.BigEndian.Uint64(b[2:]), Time: int64(binary.BigEndian.Uint64(b[10:])),
		Epoch: binary.BigEndian.Uint32(b[18:])}, nil
}

// Master is the grandmaster side (sans-IO): Sync produces the next SYNC,
// HandleDelayReq the reply to a follower's request.
type Master struct {
	clock *Clock
	epoch uint32
	seq   uint64
}

// NewMaster serves time from clock under the given epoch.
func NewMaster(clock *Clock, epoch uint32) *Master {
	clock.SetMaster(true)
	return &Master{clock: clock, epoch: epoch}
}

// Sync is the next SYNC message, stamped with the send time.
func (m *Master) Sync() Message {
	m.seq++
	return Message{Type: MsgSync, Seq: m.seq, Time: m.clock.Now(), Epoch: m.epoch}
}

// HandleDelayReq answers a follower's DELAY_REQ with the receive time.
func (m *Master) HandleDelayReq(req Message) (Message, bool) {
	if req.Type != MsgDelayReq || req.Epoch != m.epoch {
		return Message{}, false
	}
	return Message{Type: MsgDelayResp, Seq: req.Seq, Time: m.clock.Now(), Epoch: m.epoch}, true
}

// Follower disciplines a clock to a master (sans-IO).
type Follower struct {
	clock  *Clock
	epoch  uint32
	window int

	// the exchange in progress
	seq        uint64
	t1, t2, t3 int64
	open       bool

	samples []sample
	last    sample
}

type sample struct{ theta, delta int64 }

// NewFollower disciplines clock, filtering over the last window samples
// (default 8).
func NewFollower(clock *Clock, window int) *Follower {
	if window <= 0 {
		window = 8
	}
	return &Follower{clock: clock, window: window}
}

// Adopt switches to a (new) grandmaster epoch, discarding samples taken
// against the old one.
func (f *Follower) Adopt(epoch uint32) {
	if epoch != f.epoch {
		f.epoch, f.samples, f.open = epoch, nil, false
		f.clock.SetMaster(false)
	}
}

// HandleSync records a SYNC's arrival and returns the DELAY_REQ to send
// immediately.
func (f *Follower) HandleSync(m Message) (Message, bool) {
	if m.Type != MsgSync || m.Epoch != f.epoch {
		return Message{}, false
	}
	f.seq, f.t1, f.t2 = m.Seq, m.Time, f.clock.Now()
	f.t3 = f.clock.Now()
	f.open = true
	return Message{Type: MsgDelayReq, Seq: m.Seq, Epoch: f.epoch}, true
}

// HandleDelayResp completes an exchange and, if it is the best recent
// sample, corrects the clock.
func (f *Follower) HandleDelayResp(m Message) bool {
	if m.Type != MsgDelayResp || m.Epoch != f.epoch || !f.open || m.Seq != f.seq {
		return false
	}
	f.open = false
	t4 := m.Time
	fwd, back := f.t2-f.t1, t4-f.t3
	s := sample{theta: (fwd - back) / 2, delta: (fwd + back) / 2}
	if s.delta < 0 {
		return false // impossible timing: a clock jumped mid-exchange
	}
	f.samples = append(f.samples, s)
	if len(f.samples) > f.window {
		f.samples = f.samples[1:]
	}
	// Lucky-packet filter: the least-delayed sample has the tightest bound.
	// Older samples were measured before earlier corrections, so only the
	// newest one may be applied; it is applied when it is (one of) the best.
	best := f.samples[0]
	for _, x := range f.samples {
		if x.delta < best.delta {
			best = x
		}
	}
	f.last = s
	if s.delta > best.delta*2+1000 && f.clock.Synced() {
		return false // a congested exchange: keep the current correction
	}
	f.clock.apply(s.theta, s.delta)
	return true
}

// Last is the most recent sample (offset, delay) in nanoseconds.
func (f *Follower) Last() (theta, delta int64) { return f.last.theta, f.last.delta }
