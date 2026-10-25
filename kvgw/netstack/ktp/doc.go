// Package ktp implements KTP, the KVStore Transport Protocol: a QUIC-shaped,
// encrypted, multiplexed, congestion-controlled transport over UDP, written
// as a sans-IO state machine.
//
// # Shape
//
// A Conn never touches a socket, a clock, or a goroutine. The driver (package
// endpoint) feeds it datagrams with HandlePacket, drains datagrams with Poll,
// calls HandleTimeout when Timeout says so, and reads notifications with
// PollEvent. Time is always a parameter, randomness comes from Config.Rand.
// That is what lets the tests run two connections through a seeded lossy
// link in virtual time -- four megabytes across eight streams at 20% loss
// with duplication, reordering and bit flips finishes in milliseconds and
// replays bit for bit from its seed.
//
// # Handshake (one round trip, mutually authenticated, forward secret)
//
//	C → S  Initial    HELLO        eph_c (X25519), nonce_c, params_c           padded to 1200 B
//	S → C  Handshake  SERVER_HELLO eph_s, nonce_s, id_s (Ed25519), params_s,
//	                               sig_s = Sign(id_s, "ktp/1 server" ‖ th),
//	                               confirm = HMAC(k_confirm, th)
//	C → S  1-RTT      CLIENT_AUTH  id_c, sig_c = Sign(id_c, "ktp/1 client" ‖ th)   (+ app data)
//	S → C  1-RTT      HANDSHAKE_DONE
//
// th is SHA-256 over both hellos. Keys come from HKDF-SHA256 with the X25519
// shared secret as input and SHA-256("ktp/1 psk" ‖ cluster key) as salt, with
// th in every label, so each key is bound to this exact exchange. A client
// holding the wrong cluster key derives a different confirm key and fails the
// handshake on the server's first message. 1-RTT packets use AES-256-GCM with
// nonce = IV ⊕ packet number and the 28-byte header as associated data;
// packet numbers are never reused, which GCM requires. Until the server has
// verified CLIENT_AUTH it drops the client's 1-RTT packets unacknowledged, so
// no stream data is ever attributed to an unproven identity. Until the client
// proves it can receive at its address, the server sends at most three times
// what it has received (anti-amplification).
//
// Raw packets (SealRaw/OpenRaw) are the connectionless channel SWIM gossips
// on, sealed under a key derived from the cluster key. A Reset packet carries
// an HMAC of the stale connection ID under another derived key: a restarted
// node tells its peers "I have no state for that connection" and they close
// at once instead of waiting out the idle timeout, while outsiders cannot
// forge resets.
//
// # Streams and flow control
//
// Bidirectional byte streams (client-opened IDs even, server-opened odd) are
// multiplexed on one connection, with out-of-order reassembly, FIN, RESET and
// STOP_SENDING, and a MAX_STREAMS limit that the receiver raises as streams
// retire. Credit is per stream and per connection: the receiver extends a
// window once the application has consumed half of it, so a slow reader
// throttles its sender instead of buffering without bound. DATAGRAM frames
// carry unreliable messages alongside.
//
// # Loss recovery and congestion control (RFC 9002)
//
// Every ack-eliciting packet is tracked with the frames it carried. ACK
// frames (up to 32 ranges) acknowledge packets; a packet is lost once three
// later packets are acknowledged or 9/8 of an RTT has passed. Lost frames,
// not packets, are retransmitted in new packets. When nothing is
// acknowledged for a probe timeout (srtt + 4·rttvar + max_ack_delay, doubling
// each time), the oldest outstanding data is re-sent as a probe. RTT is
// estimated with the RFC's smoothed/variance filter, discounting the peer's
// reported ACK delay. The congestion controller is NewReno or CUBIC (RFC
// 9438, with the Reno-friendly region and fast convergence); both use one
// reduction per recovery period, do not grow while application-limited, and
// collapse to the minimum window on persistent congestion. A token-bucket
// pacer releases the window at 1.25 × cwnd / srtt.
//
// # Lifecycle
//
// Keep-alive PINGs, an idle timeout, a handshake timeout, CONNECTION_CLOSE
// with error codes and a drain period of three PTOs, and stateless resets.
package ktp
