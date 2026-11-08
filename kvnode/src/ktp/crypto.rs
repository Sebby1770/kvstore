//! KTP/1 handshake and packet protection (server side), matching
//! kvgw/netstack/ktp/crypto.go byte for byte.
//!
//! One round trip: the client's HELLO carries an ephemeral X25519 key, a
//! nonce and its transport parameters; the server's SERVER_HELLO carries its
//! own ephemeral key, nonce, Ed25519 identity and parameters (the "core"),
//! then a signature over the transcript hash and a confirm tag that proves it
//! derived the same keys. The client answers with CLIENT_AUTH (its identity
//! and a signature) inside its first 1-RTT packet.
//!
//! ```text
//! th   = SHA-256("ktp/1 transcript" || HELLO || SERVER_HELLO core)
//! salt = SHA-256("ktp/1 psk" || cluster key)
//! prk  = HKDF-Extract(salt, X25519 shared secret)
//! keys = HKDF-Expand(prk, label || th) for c2s/s2c key (32) and iv (12), confirm (32)
//! ```
//!
//! 1-RTT packets are AES-256-GCM sealed (nonce = iv XOR packet number,
//! associated data = the 28-byte header); Initial and Handshake packets are
//! plaintext with a CRC32C.

use aes_gcm::aead::{AeadInPlace, KeyInit};
use aes_gcm::{Aes256Gcm, Nonce, Tag};
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use hkdf::Hkdf;
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use x25519_dalek::{PublicKey, StaticSecret};

use super::wire::{self, ConnId, Header, PacketType, HEADER_LEN};

pub const MSG_HELLO: u8 = 1;
pub const MSG_SERVER_HELLO: u8 = 2;
pub const MSG_CLIENT_AUTH: u8 = 3;

/// AES-GCM tag size, the per-packet overhead of a 1-RTT packet.
pub const AEAD_OVERHEAD: usize = 16;

/// The flow-control limits each side announces in its hello.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TransportParams {
    pub max_data: u64,
    pub max_stream_data: u64,
    pub max_streams: u64,
}

impl TransportParams {
    pub fn append(&self, b: &mut Vec<u8>) {
        wire::put_varint(b, self.max_data);
        wire::put_varint(b, self.max_stream_data);
        wire::put_varint(b, self.max_streams);
    }

    fn parse(mut b: &[u8]) -> Option<(TransportParams, &[u8])> {
        let mut vs = [0u64; 3];
        for v in &mut vs {
            let (x, n) = wire::get_varint(b)?;
            *v = x;
            b = &b[n..];
        }
        Some((TransportParams { max_data: vs[0], max_stream_data: vs[1], max_streams: vs[2] }, b))
    }
}

/// A parsed HELLO.
pub struct Hello {
    pub eph: [u8; 32],
    pub params: TransportParams,
}

pub fn parse_hello(b: &[u8]) -> Option<Hello> {
    if b.len() < 65 || b[0] != MSG_HELLO {
        return None;
    }
    let (params, rest) = TransportParams::parse(&b[65..])?;
    if !rest.is_empty() {
        return None;
    }
    let mut eph = [0u8; 32];
    eph.copy_from_slice(&b[1..33]);
    Some(Hello { eph, params })
}

pub fn transcript_hash(hello: &[u8], server_core: &[u8]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(b"ktp/1 transcript");
    h.update(hello);
    h.update(server_core);
    h.finalize().into()
}

pub fn psk_salt(cluster_key: &[u8]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(b"ktp/1 psk");
    h.update(cluster_key);
    h.finalize().into()
}

/// The output of the key schedule.
#[derive(Clone)]
pub struct SessionKeys {
    pub c2s_key: [u8; 32],
    pub c2s_iv: [u8; 12],
    pub s2c_key: [u8; 32],
    pub s2c_iv: [u8; 12],
    pub confirm: [u8; 32],
}

pub fn derive_keys(shared: &[u8], cluster_key: &[u8], th: &[u8; 32]) -> SessionKeys {
    let hk = Hkdf::<Sha256>::new(Some(&psk_salt(cluster_key)), shared);
    let expand = |label: &str, out: &mut [u8]| {
        let mut info = Vec::with_capacity(label.len() + th.len());
        info.extend_from_slice(label.as_bytes());
        info.extend_from_slice(th);
        hk.expand(&info, out).expect("hkdf: output length is valid");
    };
    let mut k = SessionKeys { c2s_key: [0; 32], c2s_iv: [0; 12], s2c_key: [0; 32], s2c_iv: [0; 12], confirm: [0; 32] };
    expand("ktp/1 c2s key", &mut k.c2s_key);
    expand("ktp/1 c2s iv", &mut k.c2s_iv);
    expand("ktp/1 s2c key", &mut k.s2c_key);
    expand("ktp/1 s2c iv", &mut k.s2c_iv);
    expand("ktp/1 confirm", &mut k.confirm);
    k
}

fn hmac_sha256(key: &[u8], parts: &[&[u8]]) -> [u8; 32] {
    let mut m = <Hmac<Sha256> as Mac>::new_from_slice(key).expect("hmac: any key length is valid");
    for p in parts {
        m.update(p);
    }
    m.finalize().into_bytes().into()
}

pub fn confirm_tag(key: &[u8; 32], th: &[u8; 32]) -> [u8; 32] {
    hmac_sha256(key, &[b"ktp/1 server finished", th])
}

pub fn signed_message(role: &str, th: &[u8; 32]) -> Vec<u8> {
    let mut m = format!("ktp/1 {role}").into_bytes();
    m.extend_from_slice(th);
    m
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HandshakeError {
    /// The HELLO did not parse.
    Malformed,
    /// The client's ephemeral key is a low-order point (no shared secret).
    BadKey,
}

/// Everything the server derives from a client's HELLO.
pub struct ServerHello {
    /// The full SERVER_HELLO message: core || signature || confirm tag.
    pub msg: Vec<u8>,
    pub th: [u8; 32],
    pub keys: SessionKeys,
    /// The client's transport parameters.
    pub peer: TransportParams,
}

/// Runs the server half of the handshake for hello_msg with the given
/// ephemeral secret and nonce (random in production, fixed in tests).
pub fn server_hello(
    hello_msg: &[u8],
    eph: &StaticSecret,
    nonce: &[u8; 32],
    identity: &SigningKey,
    params: TransportParams,
    cluster_key: &[u8],
) -> Result<ServerHello, HandshakeError> {
    let hello = parse_hello(hello_msg).ok_or(HandshakeError::Malformed)?;
    let shared = eph.diffie_hellman(&PublicKey::from(hello.eph));
    if !shared.was_contributory() {
        return Err(HandshakeError::BadKey);
    }
    let mut core = Vec::with_capacity(1 + 32 * 3 + 12);
    core.push(MSG_SERVER_HELLO);
    core.extend_from_slice(PublicKey::from(eph).as_bytes());
    core.extend_from_slice(nonce);
    core.extend_from_slice(identity.verifying_key().as_bytes());
    params.append(&mut core);
    let th = transcript_hash(hello_msg, &core);
    let keys = derive_keys(shared.as_bytes(), cluster_key, &th);
    let sig = identity.sign(&signed_message("server", &th));
    let mut msg = core;
    msg.extend_from_slice(&sig.to_bytes());
    msg.extend_from_slice(&confirm_tag(&keys.confirm, &th));
    Ok(ServerHello { msg, th, keys, peer: hello.params })
}

#[derive(Debug, PartialEq, Eq)]
pub enum ClientAuth {
    /// A well-formed CLIENT_AUTH with a valid signature: the client's key.
    Valid([u8; 32]),
    /// Well-formed, but the signature does not verify.
    BadSignature,
    /// Not a CLIENT_AUTH message at all.
    Malformed,
}

pub fn check_client_auth(msg: &[u8], th: &[u8; 32]) -> ClientAuth {
    if msg.len() != 1 + 32 + 64 || msg[0] != MSG_CLIENT_AUTH {
        return ClientAuth::Malformed;
    }
    let mut pk = [0u8; 32];
    pk.copy_from_slice(&msg[1..33]);
    let mut sig = [0u8; 64];
    sig.copy_from_slice(&msg[33..97]);
    let Ok(vk) = VerifyingKey::from_bytes(&pk) else {
        return ClientAuth::BadSignature;
    };
    match vk.verify(&signed_message("client", th), &Signature::from_bytes(&sig)) {
        Ok(()) => ClientAuth::Valid(pk),
        Err(_) => ClientAuth::BadSignature,
    }
}

// ---- packet protection ----

/// Seals or opens 1-RTT packets for one direction.
pub struct Protector {
    aead: Aes256Gcm,
    iv: [u8; 12],
}

impl Protector {
    pub fn new(key: &[u8; 32], iv: &[u8; 12]) -> Protector {
        Protector { aead: Aes256Gcm::new(key.into()), iv: *iv }
    }

    fn nonce(&self, pn: u64) -> [u8; 12] {
        let mut n = self.iv;
        for (i, b) in pn.to_be_bytes().iter().enumerate() {
            n[4 + i] ^= b;
        }
        n
    }

    /// Appends a full packet to out: header (payload length fixed up) and
    /// the sealed payload.
    pub fn seal(&self, out: &mut Vec<u8>, mut h: Header, payload: &[u8]) {
        h.payload_len = (payload.len() + AEAD_OVERHEAD) as u16;
        let start = out.len();
        h.append(out);
        out.extend_from_slice(payload);
        let (hdr, body) = out[start..].split_at_mut(HEADER_LEN);
        let tag = self
            .aead
            .encrypt_in_place_detached(Nonce::from_slice(&self.nonce(h.pn)), hdr, body)
            .expect("aes-gcm: payload within limits");
        out.extend_from_slice(&tag);
    }

    /// Verifies and decrypts the payload of pkt, whose header is h.
    pub fn open(&self, h: &Header, pkt: &[u8]) -> Option<Vec<u8>> {
        let body = pkt.get(HEADER_LEN..)?;
        if body.len() < AEAD_OVERHEAD {
            return None;
        }
        let (ct, tag) = body.split_at(body.len() - AEAD_OVERHEAD);
        let mut buf = ct.to_vec();
        self.aead
            .decrypt_in_place_detached(Nonce::from_slice(&self.nonce(h.pn)), &pkt[..HEADER_LEN], &mut buf, Tag::from_slice(tag))
            .ok()?;
        Some(buf)
    }
}

/// Appends a plaintext (Initial/Handshake/Reset) packet: header, payload and
/// a CRC32C over both.
pub fn seal_plain(out: &mut Vec<u8>, mut h: Header, payload: &[u8]) {
    h.payload_len = (payload.len() + 4) as u16;
    let start = out.len();
    h.append(out);
    out.extend_from_slice(payload);
    let sum = wire::crc32c(&out[start..]);
    out.extend_from_slice(&sum.to_be_bytes());
}

/// Checks a plaintext packet's CRC32C and returns its payload.
pub fn open_plain(pkt: &[u8]) -> Option<&[u8]> {
    if pkt.len() < HEADER_LEN + 4 {
        return None;
    }
    let (body, sum) = pkt.split_at(pkt.len() - 4);
    if u32::from_be_bytes([sum[0], sum[1], sum[2], sum[3]]) != wire::crc32c(body) {
        return None;
    }
    Some(&body[HEADER_LEN..])
}

// ---- stateless reset ----

/// HKDF(secret = cluster key, salt = SHA-256("ktp/1 psk"), info = label).
pub fn cluster_derived(cluster_key: &[u8], label: &str) -> [u8; 32] {
    let hk = Hkdf::<Sha256>::new(Some(&psk_salt(&[])), cluster_key);
    let mut out = [0u8; 32];
    hk.expand(label.as_bytes(), &mut out).expect("hkdf: output length is valid");
    out
}

/// The key reset tokens are computed under (derive once per endpoint).
pub fn reset_key(cluster_key: &[u8]) -> [u8; 32] {
    cluster_derived(cluster_key, "ktp/1 reset")
}

/// What a stateless reset for cid must carry. Anyone holding the cluster key
/// can compute it; nobody else can forge it.
pub fn reset_token(reset_key: &[u8; 32], cid: &ConnId) -> [u8; 16] {
    let mac = hmac_sha256(reset_key, &[cid]);
    let mut t = [0u8; 16];
    t.copy_from_slice(&mac[..16]);
    t
}

/// The reply to a packet (header h) for a connection this endpoint has no
/// state for: the owner of the connection closes it at once instead of
/// waiting out its idle timeout.
pub fn stateless_reset(reset_key: &[u8; 32], h: &Header) -> Vec<u8> {
    let mut out = Vec::with_capacity(HEADER_LEN + 16 + 4);
    seal_plain(&mut out, Header::new(PacketType::Reset, h.scid, h.dcid, 0), &reset_token(reset_key, &h.dcid));
    out
}

/// Constant-time equality for secrets of equal length.
pub fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// Fills b from the operating system's CSPRNG.
pub fn random_bytes(b: &mut [u8]) {
    getrandom::getrandom(b).expect("ktp: the OS random number generator failed");
}

/// A fresh Ed25519 identity.
pub fn generate_identity() -> SigningKey {
    let mut seed = [0u8; 32];
    random_bytes(&mut seed);
    SigningKey::from_bytes(&seed)
}
