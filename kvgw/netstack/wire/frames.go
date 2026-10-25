package wire

import "fmt"

// Frame type codes (the leading varint of every frame).
const (
	TypePadding           = 0x00
	TypePing              = 0x01
	TypeAck               = 0x02
	TypeResetStream       = 0x04
	TypeStopSending       = 0x05
	TypeHandshake         = 0x06
	TypeStream            = 0x08 // 0x08..0x0b: | 0x02 has-length, | 0x01 FIN
	TypeMaxData           = 0x10
	TypeMaxStreamData     = 0x11
	TypeMaxStreams        = 0x12
	TypeDataBlocked       = 0x14
	TypeStreamDataBlocked = 0x15
	TypeConnectionClose   = 0x1c
	TypeHandshakeDone     = 0x1e
	TypeDatagram          = 0x30
)

// MaxAckRanges bounds the ranges in one ACK frame; a receiver tracking more
// reports the newest ones.
const MaxAckRanges = 32

// Frame is one decoded frame. Byte slices in parsed frames alias the packet
// buffer; callers that keep them must copy.
type Frame interface {
	// Append appends the frame's encoding.
	Append(b []byte) []byte
	// Len is the encoded size.
	Len() int
	// AckEliciting reports whether receiving this frame obliges an ACK.
	AckEliciting() bool
}

// Padding is n zero bytes.
type Padding struct{ N int }

// Ping elicits an ACK and nothing else (keep-alive, PTO probe).
type Ping struct{}

// AckRange is an inclusive range of acknowledged packet numbers.
type AckRange struct{ Smallest, Largest uint64 }

// Ack acknowledges packet numbers. Ranges are ordered largest-first and do
// not overlap or touch. Delay is how long (µs) the receiver held the ACK
// after receiving Ranges[0].Largest, so the sender can discount it from RTT.
type Ack struct {
	Delay  uint64
	Ranges []AckRange
}

// ResetStream abruptly terminates the sending half of a stream.
type ResetStream struct{ StreamID, Code, FinalSize uint64 }

// StopSending asks the peer to stop sending on a stream.
type StopSending struct{ StreamID, Code uint64 }

// Handshake carries one handshake message.
type Handshake struct{ Data []byte }

// Stream carries stream data at an offset; Fin marks the final byte.
type Stream struct {
	StreamID uint64
	Offset   uint64
	Data     []byte
	Fin      bool
}

// MaxData raises the connection-level flow-control limit.
type MaxData struct{ Max uint64 }

// MaxStreamData raises one stream's flow-control limit.
type MaxStreamData struct{ StreamID, Max uint64 }

// MaxStreams raises the number of streams the peer may open.
type MaxStreams struct{ Max uint64 }

// DataBlocked reports that the sender is blocked at the connection limit.
type DataBlocked struct{ Limit uint64 }

// StreamDataBlocked reports that the sender is blocked at a stream's limit.
type StreamDataBlocked struct{ StreamID, Limit uint64 }

// ConnectionClose terminates the connection.
type ConnectionClose struct {
	Code   uint64
	Reason string
}

// HandshakeDone tells the client the server has authenticated it.
type HandshakeDone struct{}

// Datagram is an unreliable application datagram.
type Datagram struct{ Data []byte }

func (f Padding) Append(b []byte) []byte {
	for i := 0; i < f.N; i++ {
		b = append(b, 0)
	}
	return b
}
func (f Padding) Len() int           { return f.N }
func (f Padding) AckEliciting() bool { return false }

func (Ping) Append(b []byte) []byte { return append(b, TypePing) }
func (Ping) Len() int               { return 1 }
func (Ping) AckEliciting() bool     { return true }

func (f Ack) Append(b []byte) []byte {
	b = append(b, TypeAck)
	if len(f.Ranges) == 0 {
		// An ACK must acknowledge something; callers never build an empty
		// one, but encode a well-formed frame (packet 0) rather than garbage.
		f.Ranges = []AckRange{{}}
	}
	first := f.Ranges[0]
	b = AppendVarint(b, first.Largest)
	b = AppendVarint(b, f.Delay)
	b = AppendVarint(b, uint64(len(f.Ranges)-1))
	b = AppendVarint(b, first.Largest-first.Smallest)
	prev := first.Smallest
	for _, r := range f.Ranges[1:] {
		b = AppendVarint(b, prev-r.Largest-2) // gap
		b = AppendVarint(b, r.Largest-r.Smallest)
		prev = r.Smallest
	}
	return b
}
func (f Ack) Len() int           { return len(f.Append(nil)) }
func (f Ack) AckEliciting() bool { return false }

// Largest is the largest acknowledged packet number.
func (f Ack) Largest() uint64 {
	if len(f.Ranges) == 0 {
		return 0
	}
	return f.Ranges[0].Largest
}

// Contains reports whether pn is acknowledged by f.
func (f Ack) Contains(pn uint64) bool {
	for _, r := range f.Ranges {
		if pn >= r.Smallest && pn <= r.Largest {
			return true
		}
	}
	return false
}

func (f ResetStream) Append(b []byte) []byte {
	b = append(b, TypeResetStream)
	b = AppendVarint(b, f.StreamID)
	b = AppendVarint(b, f.Code)
	return AppendVarint(b, f.FinalSize)
}
func (f ResetStream) Len() int {
	return 1 + VarintLen(f.StreamID) + VarintLen(f.Code) + VarintLen(f.FinalSize)
}
func (f ResetStream) AckEliciting() bool { return true }

func (f StopSending) Append(b []byte) []byte {
	b = append(b, TypeStopSending)
	b = AppendVarint(b, f.StreamID)
	return AppendVarint(b, f.Code)
}
func (f StopSending) Len() int           { return 1 + VarintLen(f.StreamID) + VarintLen(f.Code) }
func (f StopSending) AckEliciting() bool { return true }

func (f Handshake) Append(b []byte) []byte {
	b = append(b, TypeHandshake)
	b = AppendVarint(b, uint64(len(f.Data)))
	return append(b, f.Data...)
}
func (f Handshake) Len() int           { return 1 + VarintLen(uint64(len(f.Data))) + len(f.Data) }
func (f Handshake) AckEliciting() bool { return true }

func (f Stream) Append(b []byte) []byte {
	t := byte(TypeStream | 0x02)
	if f.Fin {
		t |= 0x01
	}
	b = append(b, t)
	b = AppendVarint(b, f.StreamID)
	b = AppendVarint(b, f.Offset)
	b = AppendVarint(b, uint64(len(f.Data)))
	return append(b, f.Data...)
}
func (f Stream) Len() int {
	return StreamOverhead(f.StreamID, f.Offset, len(f.Data)) + len(f.Data)
}
func (f Stream) AckEliciting() bool { return true }

// StreamOverhead is the encoded size of a STREAM frame minus its data.
func StreamOverhead(id, offset uint64, n int) int {
	return 1 + VarintLen(id) + VarintLen(offset) + VarintLen(uint64(n))
}

func (f MaxData) Append(b []byte) []byte { return AppendVarint(append(b, TypeMaxData), f.Max) }
func (f MaxData) Len() int               { return 1 + VarintLen(f.Max) }
func (f MaxData) AckEliciting() bool     { return true }

func (f MaxStreamData) Append(b []byte) []byte {
	return AppendVarint(AppendVarint(append(b, TypeMaxStreamData), f.StreamID), f.Max)
}
func (f MaxStreamData) Len() int           { return 1 + VarintLen(f.StreamID) + VarintLen(f.Max) }
func (f MaxStreamData) AckEliciting() bool { return true }

func (f MaxStreams) Append(b []byte) []byte { return AppendVarint(append(b, TypeMaxStreams), f.Max) }
func (f MaxStreams) Len() int               { return 1 + VarintLen(f.Max) }
func (f MaxStreams) AckEliciting() bool     { return true }

func (f DataBlocked) Append(b []byte) []byte {
	return AppendVarint(append(b, TypeDataBlocked), f.Limit)
}
func (f DataBlocked) Len() int           { return 1 + VarintLen(f.Limit) }
func (f DataBlocked) AckEliciting() bool { return true }

func (f StreamDataBlocked) Append(b []byte) []byte {
	return AppendVarint(AppendVarint(append(b, TypeStreamDataBlocked), f.StreamID), f.Limit)
}
func (f StreamDataBlocked) Len() int           { return 1 + VarintLen(f.StreamID) + VarintLen(f.Limit) }
func (f StreamDataBlocked) AckEliciting() bool { return true }

func (f ConnectionClose) Append(b []byte) []byte {
	b = append(b, TypeConnectionClose)
	b = AppendVarint(b, f.Code)
	b = AppendVarint(b, uint64(len(f.Reason)))
	return append(b, f.Reason...)
}
func (f ConnectionClose) Len() int {
	return 1 + VarintLen(f.Code) + VarintLen(uint64(len(f.Reason))) + len(f.Reason)
}
func (f ConnectionClose) AckEliciting() bool { return false }

func (HandshakeDone) Append(b []byte) []byte { return append(b, TypeHandshakeDone) }
func (HandshakeDone) Len() int               { return 1 }
func (HandshakeDone) AckEliciting() bool     { return true }

func (f Datagram) Append(b []byte) []byte {
	b = append(b, TypeDatagram)
	b = AppendVarint(b, uint64(len(f.Data)))
	return append(b, f.Data...)
}
func (f Datagram) Len() int           { return 1 + VarintLen(uint64(len(f.Data))) + len(f.Data) }
func (f Datagram) AckEliciting() bool { return true }

// parser walks a payload.
type parser struct {
	b   []byte
	err error
}

func (p *parser) varint() uint64 {
	if p.err != nil {
		return 0
	}
	v, n, err := ParseVarint(p.b)
	if err != nil {
		p.err = ErrFrame
		return 0
	}
	p.b = p.b[n:]
	return v
}

func (p *parser) bytes(n uint64) []byte {
	if p.err != nil {
		return nil
	}
	if n > uint64(len(p.b)) {
		p.err = ErrFrame
		return nil
	}
	out := p.b[:n:n]
	p.b = p.b[n:]
	return out
}

// ParseFrames decodes every frame in a payload. Runs of padding collapse into
// one Padding frame. Any malformed frame fails the whole payload: a packet is
// processed entirely or not at all.
func ParseFrames(payload []byte) ([]Frame, error) {
	var out []Frame
	p := &parser{b: payload}
	for len(p.b) > 0 {
		f, err := p.frame()
		if err != nil {
			return nil, err
		}
		out = append(out, f)
	}
	return out, nil
}

func (p *parser) frame() (Frame, error) {
	if p.b[0] == TypePadding {
		n := 0
		for n < len(p.b) && p.b[n] == 0 {
			n++
		}
		p.b = p.b[n:]
		return Padding{N: n}, nil
	}
	t := p.varint()
	var f Frame
	switch {
	case t == TypePing:
		f = Ping{}
	case t == TypeAck:
		f = p.ack()
	case t == TypeResetStream:
		f = ResetStream{StreamID: p.varint(), Code: p.varint(), FinalSize: p.varint()}
	case t == TypeStopSending:
		f = StopSending{StreamID: p.varint(), Code: p.varint()}
	case t == TypeHandshake:
		f = Handshake{Data: p.bytes(p.varint())}
	case t >= TypeStream && t <= TypeStream|0x03:
		s := Stream{StreamID: p.varint(), Offset: p.varint(), Fin: t&0x01 != 0}
		if t&0x02 != 0 {
			s.Data = p.bytes(p.varint())
		} else {
			s.Data = p.bytes(uint64(len(p.b)))
		}
		if p.err == nil && s.Offset+uint64(len(s.Data)) > MaxVarint {
			p.err = ErrFrame
		}
		f = s
	case t == TypeMaxData:
		f = MaxData{Max: p.varint()}
	case t == TypeMaxStreamData:
		f = MaxStreamData{StreamID: p.varint(), Max: p.varint()}
	case t == TypeMaxStreams:
		f = MaxStreams{Max: p.varint()}
	case t == TypeDataBlocked:
		f = DataBlocked{Limit: p.varint()}
	case t == TypeStreamDataBlocked:
		f = StreamDataBlocked{StreamID: p.varint(), Limit: p.varint()}
	case t == TypeConnectionClose:
		code := p.varint()
		f = ConnectionClose{Code: code, Reason: string(p.bytes(p.varint()))}
	case t == TypeHandshakeDone:
		f = HandshakeDone{}
	case t == TypeDatagram:
		f = Datagram{Data: p.bytes(p.varint())}
	default:
		if p.err != nil {
			return nil, p.err
		}
		return nil, fmt.Errorf("%w 0x%x", ErrFrameType, t)
	}
	if p.err != nil {
		return nil, p.err
	}
	return f, nil
}

func (p *parser) ack() Frame {
	largest := p.varint()
	delay := p.varint()
	extra := p.varint()
	first := p.varint()
	if p.err != nil {
		return nil
	}
	if first > largest || extra >= MaxAckRanges {
		p.err = ErrFrame
		return nil
	}
	a := Ack{Delay: delay, Ranges: make([]AckRange, 0, extra+1)}
	a.Ranges = append(a.Ranges, AckRange{Smallest: largest - first, Largest: largest})
	prev := largest - first
	for i := uint64(0); i < extra; i++ {
		gap, n := p.varint(), p.varint()
		if p.err != nil {
			return nil
		}
		if prev < gap+2 || prev-gap-2 < n {
			p.err = ErrFrame
			return nil
		}
		hi := prev - gap - 2
		a.Ranges = append(a.Ranges, AckRange{Smallest: hi - n, Largest: hi})
		prev = hi - n
	}
	return a
}
