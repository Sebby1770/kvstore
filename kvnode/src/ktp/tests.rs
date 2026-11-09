//! KTP conformance: every line of tests/ktp_vectors.txt (generated from the
//! Go implementation) is reproduced here, and the server state machine is
//! driven through a handshake that is byte-identical to Go's -- the vector
//! Initial goes in, the vector SERVER_HELLO must come out -- then through
//! requests, loss, violations, resets and timeouts with a scripted client
//! holding the vector session keys.

use std::sync::Arc;
use std::time::{Duration, Instant};

use ed25519_dalek::{Signer, SigningKey};
use x25519_dalek::{PublicKey, StaticSecret};

use super::conn::*;
use super::crypto::*;
use super::ranges::RangeSet;
use super::stream::ReadResult;
use super::wire::*;

struct Vectors {
    lines: Vec<(String, Vec<String>)>,
}

impl Vectors {
    fn load() -> Vectors {
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/ktp_vectors.txt");
        let text = std::fs::read_to_string(path).expect("tests/ktp_vectors.txt");
        let lines = text
            .lines()
            .filter(|l| !l.trim().is_empty() && !l.starts_with('#'))
            .map(|l| {
                let mut f = l.split_whitespace().map(String::from);
                let name = f.next().expect("name");
                (name, f.collect())
            })
            .collect();
        Vectors { lines }
    }

    fn all(&self, name: &str) -> Vec<&Vec<String>> {
        self.lines.iter().filter(|(n, _)| n == name).map(|(_, f)| f).collect()
    }

    fn one(&self, name: &str) -> &Vec<String> {
        let v = self.all(name);
        assert_eq!(v.len(), 1, "vector {name}");
        v[0]
    }

    fn hex(&self, name: &str) -> Vec<u8> {
        unhex(&self.one(name)[0])
    }

    fn arr<const N: usize>(&self, name: &str) -> [u8; N] {
        self.hex(name).try_into().unwrap_or_else(|_| panic!("vector {name} is not {N} bytes"))
    }

    fn params(&self, name: &str) -> TransportParams {
        let f = self.one(name);
        TransportParams {
            max_data: f[0].parse().unwrap(),
            max_stream_data: f[1].parse().unwrap(),
            max_streams: f[2].parse().unwrap(),
        }
    }
}

fn unhex(s: &str) -> Vec<u8> {
    if s == "-" {
        return Vec::new();
    }
    (0..s.len()).step_by(2).map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap()).collect()
}

fn hex(b: &[u8]) -> String {
    if b.is_empty() {
        return "-".into();
    }
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn ms(n: u64) -> Duration {
    Duration::from_millis(n)
}

// ---- codec ----

#[test]
fn varints_match_go() {
    let v = Vectors::load();
    let cases = v.all("varint");
    assert!(cases.len() >= 10);
    for f in cases {
        let value: u64 = f[0].parse().unwrap();
        let mut b = Vec::new();
        put_varint(&mut b, value);
        assert_eq!(hex(&b), f[1], "varint {value}");
        assert_eq!(get_varint(&b), Some((value, b.len())));
        assert_eq!(varint_len(value), b.len());
        assert_eq!(get_varint(&b[..b.len() - 1]).filter(|_| b.len() > 1), None, "truncated varint {value}");
    }
}

#[test]
fn crc32c_matches_go() {
    let v = Vectors::load();
    for f in v.all("crc32c") {
        assert_eq!(format!("{:08x}", crc32c(&unhex(&f[0]))), f[1]);
    }
}

#[test]
fn header_matches_go() {
    let v = Vectors::load();
    let h = Header {
        ty: PacketType::OneRtt,
        dcid: [1, 2, 3, 4, 5, 6, 7, 8],
        scid: [0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18],
        pn: 0x0102030405060708,
        payload_len: 0x1234,
    };
    let mut b = Vec::new();
    h.append(&mut b);
    assert_eq!(hex(&b), v.one("header")[0]);
    assert_eq!(Header::parse(&b), Err(WireError::Length)); // 0x1234 payload bytes are missing
    b.resize(HEADER_LEN + 0x1234, 0);
    assert_eq!(Header::parse(&b), Ok(h));
    for (at, val, err) in [(0, 0x31, WireError::Type), (0, 0x60, WireError::Type), (1, 2, WireError::Version)] {
        let mut bad = b.clone();
        bad[at] = val;
        assert_eq!(Header::parse(&bad), Err(err));
    }
    assert_eq!(Header::parse(&b[..27]), Err(WireError::Short));
}

fn fixed_frames() -> Vec<(&'static str, Frame)> {
    vec![
        ("padding", Frame::Padding(5)),
        ("ping", Frame::Ping),
        ("ack", Frame::Ack(Ack { delay: 1234, ranges: vec![(990, 1000), (900, 950), (7, 7)] })),
        ("ack_single", Frame::Ack(Ack { delay: 0, ranges: vec![(0, 0)] })),
        ("reset_stream", Frame::ResetStream { id: 4, code: 0x101, final_size: 70000 }),
        ("stop_sending", Frame::StopSending { id: 8, code: 0x102 }),
        ("handshake", Frame::Handshake(b"hs-data".to_vec())),
        ("stream", Frame::Stream { id: 12, offset: 65536, data: b"stream-data".to_vec(), fin: false }),
        ("stream_fin", Frame::Stream { id: 0, offset: 0, data: Vec::new(), fin: true }),
        ("max_data", Frame::MaxData(1 << 33)),
        ("max_stream_data", Frame::MaxStreamData { id: 16, max: 2 << 20 }),
        ("max_streams", Frame::MaxStreams(300)),
        ("data_blocked", Frame::DataBlocked(8 << 20)),
        ("stream_data_blocked", Frame::StreamDataBlocked { id: 20, limit: 2 << 20 }),
        ("connection_close", Frame::ConnectionClose { code: 0x0a, reason: b"protocol violation".to_vec() }),
        ("handshake_done", Frame::HandshakeDone),
        ("datagram", Frame::Datagram(b"dgram".to_vec())),
        ("app_pressure", Frame::AppPressure { stream: None, seq: 7, credits: 300, load: 875, queue: 12 }),
        ("app_pressure_stream", Frame::AppPressure { stream: Some(9), seq: 1 << 20, credits: 64, load: 0, queue: 0 }),
    ]
}

#[test]
fn frames_match_go() {
    let v = Vectors::load();
    let mut all = Vec::new();
    for (name, f) in fixed_frames() {
        let mut b = Vec::new();
        f.append(&mut b);
        assert_eq!(hex(&b), v.one(&format!("frame.{name}"))[0], "frame {name}");
        assert_eq!(f.encoded_len(), b.len(), "frame {name} length");
        assert_eq!(parse_frames(&b).unwrap(), vec![f.clone()], "frame {name} round trip");
        all.extend_from_slice(&b);
    }
    assert_eq!(hex(&all), v.one("frames.all")[0]);
    let want: Vec<Frame> = fixed_frames().into_iter().map(|(_, f)| f).collect();
    assert_eq!(parse_frames(&all).unwrap(), want);
    assert_eq!(
        parse_frames(&v.hex("frame.stream_nolen")).unwrap(),
        vec![Frame::Stream { id: 24, offset: 5, data: b"tail".to_vec(), fin: true }]
    );
    let bad = v.all("frames.bad");
    assert!(bad.len() >= 6);
    for f in bad {
        assert!(parse_frames(&unhex(&f[0])).is_err(), "bad payload {} parsed", f[0]);
    }
}

#[test]
fn parsers_survive_arbitrary_input() {
    // A cheap deterministic fuzz: every prefix and single-byte mutation of
    // the vector payloads must parse or fail cleanly, never panic.
    let v = Vectors::load();
    let mut seeds = vec![v.hex("frames.all"), v.hex("pkt.c2s_payload"), v.hex("pkt.initial"), v.hex("pkt.c2s")];
    seeds.push(v.hex("hs.server_hello"));
    let mut x: u32 = 0x9e37_79b9;
    for seed in &seeds {
        for n in 0..seed.len().min(400) {
            let _ = parse_frames(&seed[..n]);
            let _ = Header::parse(&seed[..n]);
            let _ = parse_hello(&seed[..n]);
        }
        for _ in 0..2000 {
            x ^= x << 13;
            x ^= x >> 17;
            x ^= x << 5;
            let mut m = seed.clone();
            let i = x as usize % m.len();
            m[i] = (x >> 8) as u8;
            let _ = parse_frames(&m);
            let _ = Header::parse(&m);
            let _ = open_plain(&m);
            let _ = check_client_auth(&m, &[0; 32]);
        }
    }
    for n in 0..HEADER_LEN + 4 {
        assert!(open_plain(&vec![0u8; n]).is_none(), "{n}-byte plaintext packet opened");
    }
}

// ---- handshake and packet protection ----

#[test]
fn handshake_matches_go() {
    let v = Vectors::load();
    let ck = v.hex("hs.cluster_key");
    let client_eph = StaticSecret::from(v.arr::<32>("hs.client_eph_secret"));
    assert_eq!(hex(PublicKey::from(&client_eph).as_bytes()), v.one("hs.client_eph_public")[0]);
    let mut hello = vec![MSG_HELLO];
    hello.extend_from_slice(PublicKey::from(&client_eph).as_bytes());
    hello.extend_from_slice(&v.hex("hs.client_nonce"));
    v.params("hs.client_params").append(&mut hello);
    assert_eq!(hex(&hello), v.one("hs.hello")[0]);

    let server_eph = StaticSecret::from(v.arr::<32>("hs.server_eph_secret"));
    assert_eq!(hex(PublicKey::from(&server_eph).as_bytes()), v.one("hs.server_eph_public")[0]);
    let server_id = SigningKey::from_bytes(&v.arr("hs.server_identity_seed"));
    assert_eq!(hex(server_id.verifying_key().as_bytes()), v.one("hs.server_identity_public")[0]);
    let shared = server_eph.diffie_hellman(&PublicKey::from(&client_eph));
    assert_eq!(hex(shared.as_bytes()), v.one("hs.shared")[0]);

    let sh = server_hello(&hello, &server_eph, &v.arr("hs.server_nonce"), &server_id, v.params("hs.server_params"), &ck)
        .expect("server hello");
    let core_len = sh.msg.len() - 64 - 32;
    assert_eq!(hex(&sh.msg[..core_len]), v.one("hs.server_core")[0]);
    assert_eq!(hex(&sh.th), v.one("hs.transcript")[0]);
    assert_eq!(hex(&sh.keys.c2s_key), v.one("hs.c2s_key")[0]);
    assert_eq!(hex(&sh.keys.c2s_iv), v.one("hs.c2s_iv")[0]);
    assert_eq!(hex(&sh.keys.s2c_key), v.one("hs.s2c_key")[0]);
    assert_eq!(hex(&sh.keys.s2c_iv), v.one("hs.s2c_iv")[0]);
    assert_eq!(hex(&sh.keys.confirm), v.one("hs.confirm_key")[0]);
    assert_eq!(hex(&sh.msg[core_len..core_len + 64]), v.one("hs.server_sig")[0]);
    assert_eq!(hex(&sh.msg[core_len + 64..]), v.one("hs.confirm_tag")[0]);
    assert_eq!(hex(&sh.msg), v.one("hs.server_hello")[0]);
    assert_eq!(sh.peer, v.params("hs.client_params"));

    // CLIENT_AUTH: Ed25519 is deterministic, so ours is Go's.
    let client_id = SigningKey::from_bytes(&v.arr("hs.client_identity_seed"));
    let mut auth = vec![MSG_CLIENT_AUTH];
    auth.extend_from_slice(client_id.verifying_key().as_bytes());
    auth.extend_from_slice(&client_id.sign(&signed_message("client", &sh.th)).to_bytes());
    assert_eq!(hex(&auth), v.one("hs.client_auth")[0]);
    assert_eq!(check_client_auth(&auth, &sh.th), ClientAuth::Valid(client_id.verifying_key().to_bytes()));
    let mut forged = auth.clone();
    forged[50] ^= 1;
    assert_eq!(check_client_auth(&forged, &sh.th), ClientAuth::BadSignature);
    assert_eq!(check_client_auth(&auth[..96], &sh.th), ClientAuth::Malformed);
    assert_eq!(check_client_auth(&auth, &[0; 32]), ClientAuth::BadSignature); // another transcript

    // Without a cluster key the salt is SHA-256 of the label alone.
    let nk = derive_keys(shared.as_bytes(), &[], &sh.th);
    assert_eq!(hex(&nk.c2s_key), v.one("hs.nokey.c2s_key")[0]);
    assert_eq!(hex(&nk.s2c_iv), v.one("hs.nokey.s2c_iv")[0]);
    assert_eq!(hex(&confirm_tag(&nk.confirm, &sh.th)), v.one("hs.nokey.confirm_tag")[0]);

    // A low-order client key yields no shared secret and is refused.
    let mut weak = hello.clone();
    weak[1..33].fill(0);
    assert!(matches!(
        server_hello(&weak, &server_eph, &[0; 32], &server_id, TransportParams::default(), &ck),
        Err(HandshakeError::BadKey)
    ));
    assert!(server_hello(&hello[..70], &server_eph, &[0; 32], &server_id, TransportParams::default(), &ck).is_err());
}

#[test]
fn packet_protection_matches_go() {
    let v = Vectors::load();
    let c2s = Protector::new(&v.arr("hs.c2s_key"), &v.arr("hs.c2s_iv"));
    let s2c = Protector::new(&v.arr("hs.s2c_key"), &v.arr("hs.s2c_iv"));
    let client_cid: ConnId = v.arr("pkt.client_cid");
    let server_cid: ConnId = v.arr("pkt.server_cid");

    let mut out = Vec::new();
    s2c.seal(&mut out, Header::new(PacketType::OneRtt, client_cid, server_cid, 0x0102030405), &v.hex("pkt.s2c_payload"));
    assert_eq!(hex(&out), v.one("pkt.s2c")[0]);

    let pkt = v.hex("pkt.c2s");
    let h = Header::parse(&pkt).unwrap();
    assert_eq!(c2s.open(&h, &pkt).expect("c2s opens"), v.hex("pkt.c2s_payload"));
    assert!(s2c.open(&h, &pkt).is_none(), "wrong direction's keys");
    for at in [0, 18, HEADER_LEN, pkt.len() - 1] {
        let mut bad = pkt.clone();
        bad[at] ^= 0x40;
        if let Ok(h) = Header::parse(&bad) {
            assert!(c2s.open(&h, &bad).is_none(), "tampered byte {at} opened");
        }
    }

    out.clear();
    seal_plain(&mut out, Header::new(PacketType::Handshake, client_cid, server_cid, 0), &v.hex("pkt.server_hello_payload"));
    assert_eq!(hex(&out), v.one("pkt.server_hello")[0]);
    let initial = v.hex("pkt.initial");
    let frames = parse_frames(open_plain(&initial).expect("initial opens")).unwrap();
    assert_eq!(frames[0], Frame::Handshake(v.hex("hs.hello")));
}

#[test]
fn stateless_reset_matches_go() {
    let v = Vectors::load();
    let rk = reset_key(&v.hex("hs.cluster_key"));
    let server_cid: ConnId = v.arr("pkt.server_cid");
    let client_cid: ConnId = v.arr("pkt.client_cid");
    assert_eq!(hex(&reset_token(&rk, &server_cid)), v.one("reset.token")[0]);
    assert_eq!(hex(&reset_token(&reset_key(&[]), &server_cid)), v.one("reset.token_nokey")[0]);
    let trigger = Header::new(PacketType::OneRtt, server_cid, client_cid, 9);
    assert_eq!(hex(&stateless_reset(&rk, &trigger)), v.one("reset.packet")[0]);
}

// ---- the server state machine against a scripted client ----

fn server_config(v: &Vectors) -> Arc<Config> {
    let mut cfg = Config::new(SigningKey::from_bytes(&v.arr("hs.server_identity_seed")), v.hex("hs.cluster_key"));
    // The vector server announced these parameters; they are in its transcript.
    let p = v.params("hs.server_params");
    cfg.initial_max_data = p.max_data;
    cfg.initial_max_stream_data = p.max_stream_data;
    cfg.max_streams = p.max_streams;
    Arc::new(cfg)
}

/// A client holding the vector session keys.
struct Client {
    c2s: Protector,
    s2c: Protector,
    cid: ConnId,
    server: ConnId,
    pn: u64,
    auth: Vec<u8>,
    received: RangeSet, // server packet numbers seen
}

impl Client {
    fn send(&mut self, conn: &mut Conn, frames: &[Frame], now: Instant) {
        let mut payload = Vec::new();
        for f in frames {
            f.append(&mut payload);
        }
        let mut pkt = Vec::new();
        self.c2s.seal(&mut pkt, Header::new(PacketType::OneRtt, self.server, self.cid, self.pn), &payload);
        self.pn += 1;
        conn.handle_packet(&pkt, now);
    }

    /// Everything the server sends now, decrypted.
    fn recv(&mut self, conn: &mut Conn, now: Instant) -> Vec<(Header, Vec<Frame>)> {
        let mut out = Vec::new();
        let mut buf = Vec::new();
        while conn.poll(&mut buf, now) {
            assert!(buf.len() <= 1350, "{}-byte datagram", buf.len());
            let h = Header::parse(&buf).unwrap();
            assert_eq!((h.dcid, h.scid), (self.cid, self.server));
            let payload = match h.ty {
                PacketType::OneRtt => self.s2c.open(&h, &buf).expect("server packet opens"),
                _ => open_plain(&buf).expect("plaintext packet").to_vec(),
            };
            out.push((h, parse_frames(&payload).unwrap()));
        }
        out
    }

    /// An ACK for every server packet received so far.
    fn ack(&self) -> Frame {
        Frame::Ack(Ack { delay: 0, ranges: self.received.iter().rev().map(|&(lo, hi)| (lo, hi - 1)).collect() })
    }
}

/// A connection accepted from the vector Initial with the vector server's
/// randomness: the server's first datagram must be Go's SERVER_HELLO, byte
/// for byte, and the session keys are the vector keys.
fn handshake(v: &Vectors, t0: Instant) -> (Conn, Client) {
    let initial = v.hex("pkt.initial");
    let mut conn = Conn::accept_with(
        server_config(v),
        &initial,
        t0,
        StaticSecret::from(v.arr::<32>("hs.server_eph_secret")),
        v.arr("hs.server_nonce"),
        v.arr("pkt.server_cid"),
    )
    .expect("accept the Go initial");
    assert_eq!(conn.state(), State::Handshake);
    let mut buf = Vec::new();
    assert!(conn.poll(&mut buf, t0));
    assert_eq!(hex(&buf), v.one("pkt.server_hello")[0], "SERVER_HELLO datagram differs from Go's");
    assert!(!conn.poll(&mut buf, t0));
    let mut received = RangeSet::new();
    received.add(0, 1);
    let client = Client {
        c2s: Protector::new(&v.arr("hs.c2s_key"), &v.arr("hs.c2s_iv")),
        s2c: Protector::new(&v.arr("hs.s2c_key"), &v.arr("hs.s2c_iv")),
        cid: v.arr("pkt.client_cid"),
        server: v.arr("pkt.server_cid"),
        pn: 1,
        auth: v.hex("hs.client_auth"),
        received,
    };
    (conn, client)
}

/// handshake(), then Go's first 1-RTT packet (CLIENT_AUTH, ACK, a one-byte
/// request on stream 0), with the request read.
fn established(v: &Vectors, t0: Instant) -> (Conn, Client) {
    let (mut conn, mut cl) = handshake(v, t0);
    conn.handle_packet(&v.hex("pkt.c2s"), t0 + ms(1));
    cl.pn = 2;
    assert!(conn.is_established());
    assert_eq!(conn.accept_stream(), Some(0));
    let mut buf = [0u8; 8];
    assert_eq!(conn.stream_read(0, &mut buf), Ok(ReadResult::Data(1)));
    assert_eq!(buf[0], 5);
    (conn, cl)
}

fn stream_ranges(packets: &[(Header, Vec<Frame>)], got: &mut RangeSet) -> u64 {
    let mut n = 0;
    for (_, frames) in packets {
        for f in frames {
            if let Frame::Stream { offset, data, .. } = f {
                got.add(*offset, offset + data.len() as u64);
                n += data.len() as u64;
            }
        }
    }
    n
}

#[test]
fn serves_a_request_over_the_go_handshake() {
    let v = Vectors::load();
    let t0 = Instant::now();
    let (mut conn, mut cl) = established(&v, t0);
    assert_eq!(conn.peer_identity().map(|k| hex(&k)), Some(v.one("hs.client_identity_public")[0].clone()));
    assert_eq!(conn.stream_write(0, &[0]), Ok(1));
    let sent = cl.recv(&mut conn, t0 + ms(2));
    assert_eq!(sent.len(), 1);
    let frames = &sent[0].1;
    assert!(frames.contains(&Frame::HandshakeDone), "{frames:?}");
    assert!(frames.iter().any(|f| matches!(f, Frame::Ack(a) if a.contains(1))), "{frames:?}");
    assert!(frames.contains(&Frame::Stream { id: 0, offset: 0, data: vec![0], fin: false }), "{frames:?}");
    // The client half-closes; the server answers with its FIN and, once
    // that is acknowledged, retires the stream and grants a new slot.
    cl.received.add(sent[0].0.pn, sent[0].0.pn + 1);
    let ack = cl.ack();
    cl.send(&mut conn, &[ack, Frame::Stream { id: 0, offset: 1, data: Vec::new(), fin: true }], t0 + ms(3));
    let mut buf = [0u8; 8];
    assert_eq!(conn.stream_read(0, &mut buf), Ok(ReadResult::Eof));
    conn.stream_close_write(0);
    let sent = cl.recv(&mut conn, t0 + ms(4));
    assert!(sent[0].1.contains(&Frame::Stream { id: 0, offset: 1, data: Vec::new(), fin: true }), "{sent:?}");
    cl.received.add(sent[0].0.pn, sent[0].0.pn + 1);
    let ack = cl.ack();
    cl.send(&mut conn, &[ack], t0 + ms(5));
    assert!(!conn.has_stream(0));
    let sent = cl.recv(&mut conn, t0 + ms(40)); // ACK delay elapsed
    let frames: Vec<&Frame> = sent.iter().flat_map(|(_, f)| f).collect();
    assert!(frames.contains(&&Frame::MaxStreams(101)), "{frames:?}");
    // Late frames for the retired stream are harmless.
    cl.send(&mut conn, &[Frame::Stream { id: 0, offset: 0, data: vec![5], fin: false }], t0 + ms(41));
    assert!(conn.is_established());
}

#[test]
fn ignores_application_data_until_the_client_authenticates() {
    let v = Vectors::load();
    let t0 = Instant::now();
    let (mut conn, mut cl) = handshake(&v, t0);
    let req = Frame::Stream { id: 0, offset: 0, data: vec![5], fin: false };
    cl.send(&mut conn, &[req.clone()], t0 + ms(1));
    assert_eq!(conn.state(), State::Handshake);
    assert_eq!(conn.accept_stream(), None);
    assert!(cl.recv(&mut conn, t0 + ms(30)).is_empty(), "nothing acknowledged before CLIENT_AUTH");
    let auth = Frame::Handshake(cl.auth.clone());
    cl.send(&mut conn, &[auth, req], t0 + ms(2));
    assert!(conn.is_established());
    assert_eq!(conn.accept_stream(), Some(0));
}

#[test]
fn bad_client_signature_closes_the_connection() {
    let v = Vectors::load();
    let t0 = Instant::now();
    let (mut conn, mut cl) = handshake(&v, t0);
    let mut auth = cl.auth.clone();
    auth[40] ^= 1;
    cl.send(&mut conn, &[Frame::Handshake(auth)], t0 + ms(1));
    assert_eq!(conn.state(), State::Closing);
    let sent = cl.recv(&mut conn, t0 + ms(1));
    assert!(matches!(&sent[0].1[0], Frame::ConnectionClose { code: CODE_HANDSHAKE_FAILED, .. }), "{sent:?}");
}

#[test]
fn allow_list_rejects_unknown_clients() {
    let v = Vectors::load();
    let t0 = Instant::now();
    let mut cfg = Config::new(SigningKey::from_bytes(&v.arr("hs.server_identity_seed")), v.hex("hs.cluster_key"));
    let p = v.params("hs.server_params");
    cfg.initial_max_data = p.max_data;
    cfg.initial_max_stream_data = p.max_stream_data;
    cfg.max_streams = p.max_streams;
    cfg.allowed_peers = Some(vec![[7; 32]]);
    let mut conn = Conn::accept_with(
        Arc::new(cfg),
        &v.hex("pkt.initial"),
        t0,
        StaticSecret::from(v.arr::<32>("hs.server_eph_secret")),
        v.arr("hs.server_nonce"),
        v.arr("pkt.server_cid"),
    )
    .unwrap();
    conn.handle_packet(&v.hex("pkt.c2s"), t0);
    assert_eq!(conn.state(), State::Closing);
    assert!(matches!(conn.error(), Some(ConnError::Closed(e)) if e.code == CODE_PEER_REJECTED));
}

#[test]
fn a_retransmitted_initial_gets_the_server_hello_again() {
    let v = Vectors::load();
    let t0 = Instant::now();
    let (mut conn, mut cl) = handshake(&v, t0);
    let mut payload = Vec::new();
    append_handshake(&mut payload, &v.hex("hs.hello"));
    payload.resize(MIN_INITIAL_SIZE - HEADER_LEN - 4, 0);
    let mut pkt = Vec::new();
    seal_plain(&mut pkt, Header::new(PacketType::Initial, v.arr("pkt.orig_dcid"), cl.cid, 1), &payload);
    conn.handle_packet(&pkt, t0 + ms(5));
    let sent = cl.recv(&mut conn, t0 + ms(5));
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].0.ty, PacketType::Handshake);
    assert!(sent[0].1.contains(&Frame::Handshake(v.hex("hs.server_hello"))));
    // A duplicate (same packet number) is dropped.
    conn.handle_packet(&pkt, t0 + ms(6));
    assert!(cl.recv(&mut conn, t0 + ms(6)).is_empty());
}

#[test]
fn retransmits_lost_data_until_everything_is_delivered() {
    let v = Vectors::load();
    let t0 = Instant::now();
    let (mut conn, mut cl) = established(&v, t0);
    let data: Vec<u8> = (0..60_000u32).map(|i| (i * 7) as u8).collect();
    assert_eq!(conn.stream_write(0, &data), Ok(data.len()));
    conn.stream_close_write(0);

    // The first flight is lost entirely: nothing else goes out until the PTO.
    let mut now = t0 + ms(2);
    let lost = cl.recv(&mut conn, now);
    let first_flight = stream_ranges(&lost, &mut RangeSet::new());
    assert!(first_flight > 0 && first_flight < data.len() as u64, "cwnd bounds the first flight: {first_flight}");
    assert!(cl.recv(&mut conn, now + ms(1)).is_empty());
    let pto = conn.timeout().expect("PTO armed");
    assert!(pto > now);
    now = pto;
    conn.handle_timeout(now);
    let probes = cl.recv(&mut conn, now);
    assert!(!probes.is_empty() && probes.len() <= 2, "{} probes", probes.len());

    // From here on every packet arrives and is acknowledged.
    let mut got = RangeSet::new();
    let mut fin = false;
    let mut packets = probes;
    for _ in 0..500 {
        stream_ranges(&packets, &mut got);
        for (h, frames) in &packets {
            cl.received.add(h.pn, h.pn + 1);
            fin |= frames.iter().any(|f| matches!(f, Frame::Stream { fin: true, .. }));
        }
        if fin && got.gaps(0, data.len() as u64).is_empty() && conn.stats().bytes_in_flight == 0 {
            break;
        }
        now += ms(5);
        let ack = cl.ack();
        cl.send(&mut conn, &[ack], now);
        packets = cl.recv(&mut conn, now);
        if packets.is_empty() {
            if let Some(t) = conn.timeout() {
                now = now.max(t);
                conn.handle_timeout(now);
                packets = cl.recv(&mut conn, now);
            }
        }
    }
    assert!(got.gaps(0, data.len() as u64).is_empty(), "missing {:?}", got.gaps(0, data.len() as u64));
    assert!(fin, "FIN never arrived");
    let s = conn.stats();
    assert!(s.packets_lost > 0 && s.stream_bytes_retransmitted >= first_flight, "{s:?}");
    assert_eq!(s.bytes_in_flight, 0);
}

#[test]
fn protocol_violations_close_the_connection() {
    let v = Vectors::load();
    let t0 = Instant::now();
    let cases = [
        (Frame::Stream { id: 0, offset: 1 << 20, data: vec![1], fin: false }, CODE_FLOW_CONTROL),
        (Frame::Stream { id: 2 * 100, offset: 0, data: vec![1], fin: false }, CODE_STREAM_LIMIT),
        (Frame::Stream { id: 1, offset: 0, data: vec![1], fin: false }, CODE_STREAM_STATE),
        (Frame::HandshakeDone, CODE_PROTOCOL_VIOLATION),
        (Frame::Ack(Ack { delay: 0, ranges: vec![(0, 50)] }), CODE_PROTOCOL_VIOLATION),
        (Frame::ResetStream { id: 0, code: 1, final_size: 0 }, CODE_FINAL_SIZE), // below the byte already received
    ];
    for (frame, code) in cases {
        let (mut conn, mut cl) = established(&v, t0);
        cl.send(&mut conn, &[frame.clone()], t0 + ms(2));
        assert_eq!(conn.state(), State::Closing, "{frame:?}");
        let sent = cl.recv(&mut conn, t0 + ms(2));
        assert!(
            matches!(&sent[0].1[0], Frame::ConnectionClose { code: c, .. } if *c == code),
            "{frame:?}: {sent:?}"
        );
        // Another packet while closing is answered with another CLOSE...
        cl.send(&mut conn, &[Frame::Ping], t0 + ms(3));
        assert_eq!(cl.recv(&mut conn, t0 + ms(3)).len(), 1);
        // ...and after three PTOs the state can go.
        let drain = conn.timeout().expect("drain timer");
        conn.handle_timeout(drain);
        assert!(conn.is_closed());
    }
}

#[test]
fn stop_sending_is_answered_with_a_reset() {
    let v = Vectors::load();
    let t0 = Instant::now();
    let (mut conn, mut cl) = established(&v, t0);
    conn.stream_write(0, b"partial").unwrap();
    let _ = cl.recv(&mut conn, t0 + ms(2));
    cl.send(&mut conn, &[Frame::StopSending { id: 0, code: 0x123 }], t0 + ms(3));
    let frames: Vec<Frame> = cl.recv(&mut conn, t0 + ms(3)).into_iter().flat_map(|(_, f)| f).collect();
    assert!(frames.contains(&Frame::ResetStream { id: 0, code: 0x123, final_size: 7 }), "{frames:?}");
    assert_eq!(conn.stream_write(0, b"more"), Err(StreamError::Stopped(0x123)));
}

#[test]
fn a_peer_reset_is_reported_and_releases_credit() {
    let v = Vectors::load();
    let t0 = Instant::now();
    let (mut conn, mut cl) = established(&v, t0);
    cl.send(&mut conn, &[Frame::Stream { id: 0, offset: 1, data: vec![9; 100], fin: false }], t0 + ms(2));
    cl.send(&mut conn, &[Frame::ResetStream { id: 0, code: 0x77, final_size: 5000 }], t0 + ms(3));
    let mut buf = [0u8; 64];
    assert_eq!(conn.stream_read(0, &mut buf), Ok(ReadResult::Reset(0x77)));
    assert!(conn.is_established());
}

#[test]
fn stateless_reset_from_the_client_closes_the_connection() {
    let v = Vectors::load();
    let t0 = Instant::now();
    let (mut conn, cl) = established(&v, t0);
    // A reset forged without the cluster key is ignored...
    let forged = stateless_reset(&reset_key(b"not the cluster key"), &Header::new(PacketType::OneRtt, cl.cid, cl.server, 3));
    conn.handle_packet(&forged, t0 + ms(2));
    assert!(conn.is_established());
    // ...the real one (the client lost its state for our packets) closes.
    let real = stateless_reset(&reset_key(&v.hex("hs.cluster_key")), &Header::new(PacketType::OneRtt, cl.cid, cl.server, 3));
    conn.handle_packet(&real, t0 + ms(3));
    assert!(conn.is_closed());
    assert_eq!(conn.error(), Some(&ConnError::StatelessReset));
}

#[test]
fn a_vanished_client_times_out() {
    let v = Vectors::load();
    let t0 = Instant::now();
    let (mut conn, mut cl) = established(&v, t0);
    conn.stream_write(0, &[0; 3000]).unwrap(); // unacknowledged data keeps PTOs firing
    let _ = cl.recv(&mut conn, t0 + ms(2));
    let mut now = t0 + ms(2);
    for _ in 0..10_000 {
        let Some(t) = conn.timeout() else { break };
        now = t;
        conn.handle_timeout(now);
        let _ = cl.recv(&mut conn, now);
        if conn.is_closed() {
            break;
        }
    }
    assert_eq!(conn.error(), Some(&ConnError::IdleTimeout));
    let after = now.saturating_duration_since(t0);
    assert!(after >= Duration::from_secs(30) && after < Duration::from_secs(40), "closed after {after:?}");
}

#[test]
fn a_client_close_drains_silently() {
    let v = Vectors::load();
    let t0 = Instant::now();
    let (mut conn, mut cl) = established(&v, t0);
    cl.send(&mut conn, &[Frame::ConnectionClose { code: 0, reason: b"bye".to_vec() }], t0 + ms(2));
    assert_eq!(conn.state(), State::Draining);
    assert!(matches!(conn.error(), Some(ConnError::Closed(e)) if e.remote && e.reason == "bye"));
    assert!(cl.recv(&mut conn, t0 + ms(2)).is_empty());
    assert_eq!(conn.stream_write(0, b"x"), Err(StreamError::NotEstablished));
    let drain = conn.timeout().unwrap();
    conn.handle_timeout(drain);
    assert!(conn.is_closed());
}

#[test]
fn initials_that_are_not_valid_hellos_are_refused() {
    let v = Vectors::load();
    let t0 = Instant::now();
    let initial = v.hex("pkt.initial");
    let cfg = server_config(&v);
    let accept = |pkt: &[u8]| Conn::accept(Arc::clone(&cfg), pkt, t0, [1; 8]);
    assert!(accept(&initial).is_ok());
    assert!(matches!(accept(&initial[..1100]), Err(AcceptError::Wire(_)))); // length mismatch
    let mut short = Vec::new();
    let mut payload = Vec::new();
    append_handshake(&mut payload, &v.hex("hs.hello"));
    seal_plain(&mut short, Header::new(PacketType::Initial, [1; 8], [2; 8], 0), &payload);
    assert!(matches!(accept(&short), Err(AcceptError::TooSmall(_))));
    let mut corrupt = initial.clone();
    corrupt[100] ^= 1;
    assert!(matches!(accept(&corrupt), Err(AcceptError::Checksum)));
    let mut not_hello = Vec::new();
    seal_plain(&mut not_hello, Header::new(PacketType::Initial, [1; 8], [2; 8], 0), &vec![0; MIN_INITIAL_SIZE - 32]);
    assert!(matches!(accept(&not_hello), Err(AcceptError::NoHello)));
}

// ---- robustness: an authenticated client sending nonsense ----

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
    fn bytes(&mut self, max: u64) -> Vec<u8> {
        (0..self.below(max + 1)).map(|_| self.next() as u8).collect()
    }
}

fn random_frame(r: &mut Rng) -> Frame {
    let id = r.below(8); // mostly valid client streams, some server-side (odd) IDs
    match r.below(14) {
        0 => Frame::Ping,
        1 | 2 => {
            let hi = r.below(40);
            let lo = hi.saturating_sub(r.below(5));
            Frame::Ack(Ack { delay: r.below(100_000), ranges: vec![(lo, hi)] })
        }
        3..=5 => Frame::Stream { id: id & !1, offset: r.below(6000), data: r.bytes(400), fin: r.below(4) == 0 },
        6 => Frame::ResetStream { id, code: r.below(1000), final_size: r.below(7000) },
        7 => Frame::StopSending { id, code: r.below(1000) },
        8 => Frame::MaxData(r.below(1 << 40)),
        9 => Frame::MaxStreamData { id, max: r.below(1 << 30) },
        10 => Frame::MaxStreams(r.below(1 << 20)),
        11 => Frame::StreamDataBlocked { id, limit: r.below(1 << 20) },
        12 => Frame::Handshake(r.bytes(120)),
        _ => {
            if r.below(20) == 0 {
                Frame::ConnectionClose { code: r.below(20), reason: r.bytes(10) }
            } else {
                Frame::Datagram(r.bytes(50))
            }
        }
    }
}

#[test]
fn survives_random_frames_and_application_calls() {
    // KTP_FUZZ_SEEDS=N runs N seeds (default 1, which keeps `cargo test` fast).
    let seeds: u64 = std::env::var("KTP_FUZZ_SEEDS").ok().and_then(|s| s.parse().ok()).unwrap_or(1);
    for seed in 0..seeds {
        torture(0x2545_f491_4f6c_dd1d ^ seed.wrapping_mul(0x9e37_79b9_7f4a_7c15));
    }
}

fn torture(seed: u64) {
    let v = Vectors::load();
    let t0 = Instant::now();
    let mut r = Rng(seed);
    let mut connections = 0;
    let mut now = t0;
    let (mut conn, mut cl) = established(&v, now);
    let mut buf = vec![0u8; 2048];
    for step in 0..20_000 {
        if conn.is_closed() {
            // Each "new" connection reuses the vector keys: fine for a
            // single-connection state machine.
            connections += 1;
            now += ms(1);
            (conn, cl) = established(&v, now);
        }
        match r.below(10) {
            0..=5 => {
                let frames: Vec<Frame> = (0..1 + r.below(4)).map(|_| random_frame(&mut r)).collect();
                cl.send(&mut conn, &frames, now);
            }
            6 => {
                // Random (sometimes nonsensical) application calls.
                let id = r.below(8) & !1;
                match r.below(5) {
                    0 => {
                        let _ = conn.stream_read(id, &mut buf);
                    }
                    1 => {
                        let data = r.bytes(3000);
                        let _ = conn.stream_write(id, &data);
                    }
                    2 => conn.stream_close_write(id),
                    3 => conn.stream_reset(id, CODE_APPLICATION),
                    _ => conn.stream_stop_sending(id, CODE_APPLICATION),
                }
                while conn.accept_stream().is_some() {}
            }
            7 => {
                if let Some(t) = conn.timeout() {
                    now = now.max(t.min(now + ms(r.below(200))));
                    conn.handle_timeout(now);
                }
            }
            _ => {
                // Garbage datagrams: random bytes, truncations, bit flips.
                let mut pkt = v.hex("pkt.c2s");
                match r.below(3) {
                    0 => pkt.truncate(r.below(pkt.len() as u64) as usize),
                    1 => {
                        let i = r.below(pkt.len() as u64) as usize;
                        pkt[i] ^= 1 << r.below(8);
                    }
                    _ => pkt = r.bytes(200),
                }
                conn.handle_packet(&pkt, now);
            }
        }
        // Whatever the server sends must be well-formed and decryptable.
        for (h, _) in cl.recv(&mut conn, now) {
            assert!(h.ty == PacketType::OneRtt || h.ty == PacketType::Handshake, "seed {seed:x} step {step}");
        }
        now += Duration::from_micros(r.below(3000));
    }
    assert!(connections > 10, "seed {seed:x}: the fuzzer rarely closed a connection ({connections})");
}
