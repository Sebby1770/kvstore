package ktp

import (
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"time"
)

// Config configures a connection. Endpoints share one Config across all of
// their connections; it is never modified after use.
type Config struct {
	// Identity is this node's long-term Ed25519 key. Every handshake signs
	// with it, so peers learn (and can pin) who they are talking to.
	Identity ed25519.PrivateKey
	// ClusterKey is an optional pre-shared secret mixed into the key
	// schedule. Peers without it derive different keys and never connect;
	// it also authenticates Raw datagrams and stateless resets.
	ClusterKey []byte
	// VerifyPeer, if set, vets the peer's identity once it is proven (both
	// sides call it). Returning an error aborts the connection.
	VerifyPeer func(ed25519.PublicKey) error
	// Insecure replaces AEAD with CRC32C integrity on 1-RTT packets. The
	// handshake still authenticates identities; payloads travel in clear.
	// For debugging and benchmarks only.
	Insecure bool

	MaxPacketSize    int           // bytes per datagram (default 1350)
	InitialRTT       time.Duration // before the first sample (default 100ms)
	MaxAckDelay      time.Duration // how long a receiver may hold an ACK (default 25ms)
	IdleTimeout      time.Duration // silence that ends a connection (default 30s)
	HandshakeTimeout time.Duration // limit for establishing (default 10s)
	KeepAlive        time.Duration // PING after this much send silence (0 = off)

	InitialMaxData       uint64 // connection receive window (default 8 MiB)
	InitialMaxStreamData uint64 // per-stream receive window (default 2 MiB)
	MaxStreams           uint64 // concurrent peer-initiated streams (default 256)
	MaxSendBuffer        int    // unacknowledged bytes buffered per stream (default 2 MiB)
	MaxDatagramQueue     int    // queued datagrams each way (default 128)

	// Congestion selects the controller: "newreno" (default), "cubic", or
	// "bbr" (model-based; tolerates random loss).
	Congestion string
	// Rand supplies key material and connection IDs (default crypto/rand).
	// Tests pass a seeded reader to make whole connections reproducible.
	Rand io.Reader
}

// Defaults.
const (
	DefaultMaxPacketSize = 1350
	// MinInitialSize is the size every Initial packet is padded to, so a
	// server's (at most 3x) reply cannot be used to amplify a spoofed flood.
	MinInitialSize = 1200
	minPacketSize  = MinInitialSize
)

// ErrConfig reports an unusable Config.
var ErrConfig = errors.New("ktp: invalid config")

func (c *Config) resolved() (*Config, error) {
	if c == nil {
		return nil, fmt.Errorf("%w: nil config", ErrConfig)
	}
	r := *c
	if len(r.Identity) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("%w: Identity must be an Ed25519 private key", ErrConfig)
	}
	if r.MaxPacketSize == 0 {
		r.MaxPacketSize = DefaultMaxPacketSize
	}
	if r.MaxPacketSize < minPacketSize || r.MaxPacketSize > 65000 {
		return nil, fmt.Errorf("%w: MaxPacketSize %d outside [%d, 65000]", ErrConfig, r.MaxPacketSize, minPacketSize)
	}
	def := func(d *time.Duration, v time.Duration) {
		if *d <= 0 {
			*d = v
		}
	}
	def(&r.InitialRTT, 100*time.Millisecond)
	def(&r.MaxAckDelay, 25*time.Millisecond)
	def(&r.IdleTimeout, 30*time.Second)
	def(&r.HandshakeTimeout, 10*time.Second)
	if r.InitialMaxData == 0 {
		r.InitialMaxData = 8 << 20
	}
	if r.InitialMaxStreamData == 0 {
		r.InitialMaxStreamData = 2 << 20
	}
	if r.MaxStreams == 0 {
		r.MaxStreams = 256
	}
	if r.MaxSendBuffer <= 0 {
		r.MaxSendBuffer = 2 << 20
	}
	if r.MaxDatagramQueue <= 0 {
		r.MaxDatagramQueue = 128
	}
	switch r.Congestion {
	case "":
		r.Congestion = "newreno"
	case "newreno", "cubic", "bbr":
	default:
		return nil, fmt.Errorf("%w: unknown congestion controller %q", ErrConfig, r.Congestion)
	}
	if r.Rand == nil {
		r.Rand = rand.Reader
	}
	return &r, nil
}
