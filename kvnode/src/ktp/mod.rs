//! KTP, KVStore's encrypted transport, server side.
//!
//! This is the second, independent implementation of the protocol the Go
//! coordinators speak (kvgw/netstack/ktp): QUIC-shaped streams over UDP, a
//! one-round-trip X25519 + Ed25519 handshake keyed by the cluster secret,
//! AES-256-GCM packet protection, RFC 9002 loss recovery with NewReno, flow
//! control, and stateless resets. It implements only what a storage node
//! needs: it accepts connections and serves the streams clients open; it
//! never dials, opens streams, sends datagrams or gossips.
//!
//! The two implementations are held together by tests/ktp_vectors.txt,
//! generated from the Go code (see `tests`), and by the Go interop suite
//! that drives this server with the Go client over a lossy relay.

#![allow(dead_code)] // the codec covers the whole protocol; the server uses part of it

pub mod conn;
pub mod crypto;
pub mod endpoint;
pub mod ranges;
pub mod recovery;
pub mod stream;
pub mod wire;

#[cfg(test)]
mod tests;

pub use conn::Config;
pub use endpoint::{fingerprint, Endpoint};
