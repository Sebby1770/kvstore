package ktp

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/ecdh"
	"crypto/ed25519"
	"crypto/hkdf"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/Sebby1770/kvstore/kvgw/netstack/wire"
)

// Handshake message types (first byte of a HANDSHAKE frame's data).
const (
	msgHello       = 1
	msgServerHello = 2
	msgClientAuth  = 3
)

// transportParams are the flow-control limits each side announces in its
// hello, so the peer knows how much it may send before the first update.
type transportParams struct {
	maxData, maxStreamData, maxStreams uint64
}

func (p transportParams) append(b []byte) []byte {
	b = wire.AppendVarint(b, p.maxData)
	b = wire.AppendVarint(b, p.maxStreamData)
	return wire.AppendVarint(b, p.maxStreams)
}

func parseParams(b []byte) (transportParams, []byte, error) {
	var p transportParams
	var vs [3]uint64
	for i := range vs {
		v, n, err := wire.ParseVarint(b)
		if err != nil {
			return p, nil, errMalformed
		}
		vs[i], b = v, b[n:]
	}
	p.maxData, p.maxStreamData, p.maxStreams = vs[0], vs[1], vs[2]
	return p, b, nil
}

var errMalformed = errors.New("ktp: malformed handshake message")

// hello is the client's first message: an ephemeral X25519 key, a nonce,
// and its transport parameters.
type hello struct {
	eph    []byte // 32
	nonce  []byte // 32
	params transportParams
}

func (h hello) encode() []byte {
	b := append([]byte{msgHello}, h.eph...)
	b = append(b, h.nonce...)
	return h.params.append(b)
}

func parseHello(b []byte) (hello, error) {
	if len(b) < 65 || b[0] != msgHello {
		return hello{}, errMalformed
	}
	h := hello{eph: b[1:33], nonce: b[33:65]}
	var err error
	var rest []byte
	h.params, rest, err = parseParams(b[65:])
	if err != nil || len(rest) != 0 {
		return hello{}, errMalformed
	}
	return h, nil
}

// serverHello is the server's reply. core is everything the transcript and
// signature cover; sig proves the server holds pub; confirm is an HMAC under
// a key derived from the session secret, which proves the server derived the
// same keys -- a client with the wrong cluster key finds out here, at once,
// instead of timing out.
type serverHello struct {
	eph, nonce, pub []byte
	params          transportParams
	core            []byte
	sig, confirm    []byte
}

func (s serverHello) encodeCore() []byte {
	b := append([]byte{msgServerHello}, s.eph...)
	b = append(b, s.nonce...)
	b = append(b, s.pub...)
	return s.params.append(b)
}

func parseServerHello(b []byte) (serverHello, error) {
	const fixed = 1 + 32 + 32 + 32
	if len(b) < fixed+ed25519.SignatureSize+32 || b[0] != msgServerHello {
		return serverHello{}, errMalformed
	}
	s := serverHello{eph: b[1:33], nonce: b[33:65], pub: b[65:97]}
	params, rest, err := parseParams(b[fixed:])
	if err != nil || len(rest) != ed25519.SignatureSize+32 {
		return serverHello{}, errMalformed
	}
	s.params = params
	s.core = b[:len(b)-len(rest)]
	s.sig = rest[:ed25519.SignatureSize]
	s.confirm = rest[ed25519.SignatureSize:]
	return s, nil
}

func parseClientAuth(b []byte) (pub, sig []byte, err error) {
	if len(b) != 1+ed25519.PublicKeySize+ed25519.SignatureSize || b[0] != msgClientAuth {
		return nil, nil, errMalformed
	}
	return b[1:33], b[33:], nil
}

// transcriptHash binds both hellos; every key and signature hangs off it.
func transcriptHash(helloMsg, serverCore []byte) []byte {
	h := sha256.New()
	h.Write([]byte("ktp/1 transcript"))
	h.Write(helloMsg)
	h.Write(serverCore)
	return h.Sum(nil)
}

// sessionKeys is the output of the key schedule.
type sessionKeys struct {
	c2sKey, c2sIV, s2cKey, s2cIV, confirm []byte
}

func pskSalt(clusterKey []byte) []byte {
	s := sha256.Sum256(append([]byte("ktp/1 psk"), clusterKey...))
	return s[:]
}

func deriveKeys(shared, clusterKey, th []byte) (sessionKeys, error) {
	prk, err := hkdf.Extract(sha256.New, shared, pskSalt(clusterKey))
	if err != nil {
		return sessionKeys{}, err
	}
	expand := func(label string, n int) ([]byte, error) {
		return hkdf.Expand(sha256.New, prk, label+string(th), n)
	}
	var k sessionKeys
	for _, x := range []struct {
		dst   *[]byte
		label string
		n     int
	}{
		{&k.c2sKey, "ktp/1 c2s key", 32}, {&k.c2sIV, "ktp/1 c2s iv", 12},
		{&k.s2cKey, "ktp/1 s2c key", 32}, {&k.s2cIV, "ktp/1 s2c iv", 12},
		{&k.confirm, "ktp/1 confirm", 32},
	} {
		if *x.dst, err = expand(x.label, x.n); err != nil {
			return sessionKeys{}, err
		}
	}
	return k, nil
}

func confirmTag(key, th []byte) []byte {
	m := hmac.New(sha256.New, key)
	m.Write([]byte("ktp/1 server finished"))
	m.Write(th)
	return m.Sum(nil)
}

func signedMessage(role string, th []byte) []byte {
	return append([]byte("ktp/1 "+role), th...)
}

func x25519Key(r io.Reader) (*ecdh.PrivateKey, error) {
	return ecdh.X25519().GenerateKey(r)
}

func readRand(r io.Reader, n int) ([]byte, error) {
	b := make([]byte, n)
	_, err := io.ReadFull(r, b)
	return b, err
}

// ---- packet protection ----

// protector seals or opens 1-RTT packets for one direction. A nil aead means
// insecure mode: integrity only, via CRC32C.
type protector struct {
	aead cipher.AEAD
	iv   [12]byte
}

func newProtector(key, iv []byte, insecure bool) (*protector, error) {
	p := &protector{}
	copy(p.iv[:], iv)
	if insecure {
		return p, nil
	}
	blk, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	if p.aead, err = cipher.NewGCM(blk); err != nil {
		return nil, err
	}
	return p, nil
}

func (p *protector) overhead() int {
	if p == nil || p.aead == nil {
		return 4
	}
	return p.aead.Overhead()
}

func (p *protector) nonce(pn uint64) []byte {
	n := p.iv
	var pnb [8]byte
	binary.BigEndian.PutUint64(pnb[:], pn)
	for i := 0; i < 8; i++ {
		n[4+i] ^= pnb[i]
	}
	return n[:]
}

// seal appends a full packet: header (with PayloadLen fixed up) + protected
// payload. With a nil protector (Initial/Handshake) the payload is CRC32C'd.
func (p *protector) seal(dst []byte, h wire.Header, payload []byte) []byte {
	h.PayloadLen = uint16(len(payload) + p.overhead())
	start := len(dst)
	dst = wire.AppendHeader(dst, h)
	if p == nil || p.aead == nil {
		dst = append(dst, payload...)
		return binaryAppendCRC(dst, start)
	}
	return p.aead.Seal(dst, p.nonce(h.PacketNumber), payload, dst[start:start+wire.HeaderLen])
}

func binaryAppendCRC(b []byte, start int) []byte {
	return binary.BigEndian.AppendUint32(b, wire.Checksum(b[start:]))
}

var errAuth = errors.New("ktp: packet failed authentication")

// open verifies and returns the payload of pkt (whose header is h).
func (p *protector) open(h wire.Header, pkt []byte) ([]byte, error) {
	if p == nil || p.aead == nil {
		body, ok := wire.VerifyChecksum(pkt)
		if !ok {
			return nil, errAuth
		}
		return body[wire.HeaderLen:], nil
	}
	out, err := p.aead.Open(nil, p.nonce(h.PacketNumber), pkt[wire.HeaderLen:], pkt[:wire.HeaderLen])
	if err != nil {
		return nil, errAuth
	}
	return out, nil
}

// ---- connectionless packets ----

func clusterDerived(clusterKey []byte, label string, n int) []byte {
	k, err := hkdf.Key(sha256.New, clusterKey, pskSalt(nil), label, n)
	if err != nil {
		panic("ktp: hkdf: " + err.Error()) // only fails for absurd lengths
	}
	return k
}

// MaxRawPayload is the largest payload SealRaw accepts at the default size.
const MaxRawPayload = DefaultMaxPacketSize - wire.HeaderLen - 16

// SealRaw builds a connectionless Raw packet carrying payload (SWIM gossip).
// With a cluster key it is AES-256-GCM sealed under a key derived from it,
// with a random nonce carried in the header's connection-ID and
// packet-number fields; without one it is CRC32C-protected only.
func SealRaw(cfg *Config, payload []byte) ([]byte, error) {
	if len(payload) > 65000 {
		return nil, fmt.Errorf("ktp: raw payload of %d bytes is too large", len(payload))
	}
	r := cfg.Rand
	if r == nil {
		r = rand.Reader
	}
	nonce, err := readRand(r, 12)
	if err != nil {
		return nil, err
	}
	h := wire.Header{Type: wire.RawPacket, Version: wire.Version}
	copy(h.DstCID[:4], nonce[:4])
	h.PacketNumber = binary.BigEndian.Uint64(nonce[4:])
	if len(cfg.ClusterKey) == 0 {
		return (*protector)(nil).seal(nil, h, payload), nil
	}
	// The nonce is the 4 bytes in the CID field followed by the packet
	// number: with those bytes as the IV prefix, nonce(pn) reproduces it.
	p, err := newProtector(clusterDerived(cfg.ClusterKey, "ktp/1 raw", 32), nonce[:4], false)
	if err != nil {
		return nil, err
	}
	return p.seal(nil, h, payload), nil
}

// OpenRaw verifies a Raw packet and returns its payload.
func OpenRaw(cfg *Config, pkt []byte) ([]byte, error) {
	h, err := wire.ParseHeader(pkt)
	if err != nil {
		return nil, err
	}
	if h.Type != wire.RawPacket {
		return nil, fmt.Errorf("ktp: not a raw packet (%s)", h.Type)
	}
	if len(cfg.ClusterKey) == 0 {
		return (*protector)(nil).open(h, pkt)
	}
	p, err := newProtector(clusterDerived(cfg.ClusterKey, "ktp/1 raw", 32), make([]byte, 12), false)
	if err != nil {
		return nil, err
	}
	copy(p.iv[:4], h.DstCID[:4])
	return p.open(h, pkt)
}

// resetToken is what a stateless reset for remoteCID must carry. Anyone in
// the cluster can compute it, nobody outside can forge it (without a cluster
// key resets are unauthenticated, like everything else in that mode).
func resetToken(clusterKey []byte, cid wire.ConnID) []byte {
	m := hmac.New(sha256.New, clusterDerived(clusterKey, "ktp/1 reset", 32))
	m.Write(cid[:])
	return m.Sum(nil)[:16]
}

// StatelessReset builds the reply an endpoint sends when a packet (with
// header h) arrives for a connection it has no state for -- typically because
// it restarted. The peer that owns the connection closes it immediately
// instead of waiting out its idle timeout.
func StatelessReset(cfg *Config, h wire.Header) []byte {
	out := wire.Header{Type: wire.ResetPacket, Version: wire.Version, DstCID: h.SrcCID, SrcCID: h.DstCID}
	return (*protector)(nil).seal(nil, out, resetToken(cfg.ClusterKey, h.DstCID))
}

// PeekHeader parses a datagram's header without touching any payload, for
// routing by connection ID.
func PeekHeader(pkt []byte) (wire.Header, error) { return wire.ParseHeader(pkt) }
