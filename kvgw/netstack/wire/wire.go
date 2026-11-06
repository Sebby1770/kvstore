// Package wire is the byte-level codec of KTP, KVStore's transport protocol:
// the fixed packet header, QUIC-style variable-length integers, every frame
// type, and CRC32C. It knows nothing about connections, crypto, or time — it
// turns structs into bytes and back, and on hostile input it returns errors
// instead of panicking (FuzzParseHeader, FuzzParseFrames).
//
// A KTP datagram is exactly one packet:
//
//	 0  1B  type (high nibble) | reserved (low nibble, must be 0)
//	 1  1B  version (1)
//	 2  8B  destination connection ID
//	10  8B  source connection ID
//	18  8B  packet number (strictly increasing per direction, never reused)
//	26  2B  payload length (everything after the header: frames + AEAD tag or CRC)
//	28  …   payload
//
// The 28-byte header is authenticated (it is the AEAD associated data, or is
// covered by the CRC) but not encrypted: it is what an endpoint needs to route
// a packet to a connection before it can decrypt anything. The fixed-width
// packet number costs a few bytes against QUIC's truncated encoding and buys a
// codec with no reconstruction logic.
package wire

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

// Version is the only protocol version this codec speaks.
const Version = 1

// HeaderLen is the size of every packet header.
const HeaderLen = 28

// PacketType is the high nibble of the first header byte.
type PacketType uint8

const (
	// InitialPacket carries the client's HELLO. Plaintext, CRC32C-protected, and
	// padded so that a server's reply is never an amplification vector.
	InitialPacket PacketType = 1
	// HandshakePacket carries the server's SERVER_HELLO. Plaintext, CRC32C-protected.
	HandshakePacket PacketType = 2
	// OneRTTPacket carries everything else, sealed with the session keys.
	OneRTTPacket PacketType = 3
	// RawPacket is a connectionless datagram (SWIM gossip), sealed under a key
	// derived from the cluster secret, or CRC32C-only without one.
	RawPacket PacketType = 4
	// ResetPacket is a stateless reset: "I have no state for that connection".
	ResetPacket PacketType = 5
)

func (t PacketType) String() string {
	switch t {
	case InitialPacket:
		return "initial"
	case HandshakePacket:
		return "handshake"
	case OneRTTPacket:
		return "1rtt"
	case RawPacket:
		return "raw"
	case ResetPacket:
		return "reset"
	}
	return fmt.Sprintf("type(%d)", uint8(t))
}

// ConnID identifies one end of a connection. Each side picks its own and the
// peer addresses packets to it.
type ConnID [8]byte

func (c ConnID) String() string { return fmt.Sprintf("%x", c[:]) }

// Header is a decoded packet header.
type Header struct {
	Type         PacketType
	Version      uint8
	DstCID       ConnID
	SrcCID       ConnID
	PacketNumber uint64
	PayloadLen   uint16
}

// Errors returned by the parsers.
var (
	ErrShort     = errors.New("wire: truncated input")
	ErrVersion   = errors.New("wire: unsupported version")
	ErrType      = errors.New("wire: unknown packet type")
	ErrLength    = errors.New("wire: payload length does not match datagram")
	ErrFrame     = errors.New("wire: malformed frame")
	ErrFrameType = errors.New("wire: unknown frame type")
	ErrVarint    = errors.New("wire: varint out of range")
)

// AppendHeader appends the 28-byte encoding of h.
func AppendHeader(b []byte, h Header) []byte {
	b = append(b, byte(h.Type)<<4, h.Version)
	b = append(b, h.DstCID[:]...)
	b = append(b, h.SrcCID[:]...)
	b = binary.BigEndian.AppendUint64(b, h.PacketNumber)
	return binary.BigEndian.AppendUint16(b, h.PayloadLen)
}

// ParseHeader decodes the header of datagram pkt and checks that the declared
// payload length matches the datagram exactly (KTP never coalesces packets).
func ParseHeader(pkt []byte) (Header, error) {
	var h Header
	if len(pkt) < HeaderLen {
		return h, ErrShort
	}
	if pkt[0]&0x0f != 0 {
		return h, ErrType
	}
	h.Type = PacketType(pkt[0] >> 4)
	if h.Type < InitialPacket || h.Type > ResetPacket {
		return h, ErrType
	}
	h.Version = pkt[1]
	if h.Version != Version {
		return h, ErrVersion
	}
	copy(h.DstCID[:], pkt[2:10])
	copy(h.SrcCID[:], pkt[10:18])
	h.PacketNumber = binary.BigEndian.Uint64(pkt[18:26])
	h.PayloadLen = binary.BigEndian.Uint16(pkt[26:28])
	if int(h.PayloadLen) != len(pkt)-HeaderLen {
		return h, ErrLength
	}
	return h, nil
}

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// Checksum is CRC32C (Castagnoli) of b.
func Checksum(b []byte) uint32 { return crc32.Checksum(b, castagnoli) }

// AppendChecksum appends the big-endian CRC32C of b to b.
func AppendChecksum(b []byte) []byte {
	return binary.BigEndian.AppendUint32(b, Checksum(b))
}

// VerifyChecksum checks a buffer that ends in its own CRC32C and returns the
// covered bytes.
func VerifyChecksum(b []byte) ([]byte, bool) {
	if len(b) < 4 {
		return nil, false
	}
	body := b[:len(b)-4]
	return body, binary.BigEndian.Uint32(b[len(b)-4:]) == Checksum(body)
}

// ---- varints (RFC 9000 §16) ----

// MaxVarint is the largest value a varint can carry (2^62-1).
const MaxVarint = 1<<62 - 1

// VarintLen is the encoded size of v (1, 2, 4 or 8 bytes).
func VarintLen(v uint64) int {
	switch {
	case v <= 63:
		return 1
	case v <= 16383:
		return 2
	case v <= 1073741823:
		return 4
	default:
		return 8
	}
}

// AppendVarint appends the varint encoding of v. Values above MaxVarint are a
// programming error and are clamped (frames never carry them).
func AppendVarint(b []byte, v uint64) []byte {
	if v > MaxVarint {
		v = MaxVarint
	}
	switch VarintLen(v) {
	case 1:
		return append(b, byte(v))
	case 2:
		return binary.BigEndian.AppendUint16(b, uint16(v)|0x4000)
	case 4:
		return binary.BigEndian.AppendUint32(b, uint32(v)|0x80000000)
	default:
		return binary.BigEndian.AppendUint64(b, v|0xc000000000000000)
	}
}

// ParseVarint decodes one varint from the front of b, returning its value and
// encoded length.
func ParseVarint(b []byte) (uint64, int, error) {
	if len(b) == 0 {
		return 0, 0, ErrShort
	}
	n := 1 << (b[0] >> 6)
	if len(b) < n {
		return 0, 0, ErrShort
	}
	v := uint64(b[0] & 0x3f)
	for i := 1; i < n; i++ {
		v = v<<8 | uint64(b[i])
	}
	return v, n, nil
}
