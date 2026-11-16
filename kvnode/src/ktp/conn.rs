//! One server-side KTP connection as a pure state machine, the server half of
//! kvgw/netstack/ktp/conn*.go: feed it datagrams with `handle_packet`, drain
//! datagrams to send with `poll`, honour `timeout` with `handle_timeout`. It
//! does no I/O and reads no clock; the endpoint drives it.
//!
//! kvnode only ever accepts connections and never opens streams, so this is
//! the server role alone: it answers a HELLO with SERVER_HELLO, requires
//! CLIENT_AUTH before it processes anything else, then serves
//! client-initiated bidirectional streams with flow control, loss recovery
//! and an orderly close.

use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use ed25519_dalek::SigningKey;
use x25519_dalek::StaticSecret;

use super::crypto::{self, ClientAuth, Protector, TransportParams, AEAD_OVERHEAD};
use super::recovery::{AckTracker, Outcome, Recovery, SentFrame, SentPacket};
use super::stream::{ReadResult, Stream};
use super::wire::{self, ConnId, Frame, Header, PacketType, WireError, HEADER_LEN};

/// Every Initial is padded to this, so the server's reply (at most 3x what it
/// received until the client proves its address) cannot amplify a spoofed
/// flood.
pub const MIN_INITIAL_SIZE: usize = 1200;

// Transport error codes carried in CONNECTION_CLOSE (same as Go).
pub const CODE_NO_ERROR: u64 = 0x00;
pub const CODE_INTERNAL: u64 = 0x01;
pub const CODE_FLOW_CONTROL: u64 = 0x03;
pub const CODE_STREAM_LIMIT: u64 = 0x04;
pub const CODE_STREAM_STATE: u64 = 0x05;
pub const CODE_FINAL_SIZE: u64 = 0x06;
pub const CODE_PROTOCOL_VIOLATION: u64 = 0x0a;
pub const CODE_HANDSHAKE_FAILED: u64 = 0x10;
pub const CODE_PEER_REJECTED: u64 = 0x11;
/// First application error code (stream resets, STOP_SENDING).
pub const CODE_APPLICATION: u64 = 0x100;

/// Connection parameters, shared by every connection of an endpoint.
pub struct Config {
    /// The node's long-term Ed25519 identity; every handshake signs with it.
    pub identity: SigningKey,
    /// Pre-shared secret mixed into the key schedule: clients without it
    /// derive different keys and never connect. It also authenticates
    /// stateless resets.
    pub cluster_key: Vec<u8>,
    /// If set, only clients with one of these identities may connect.
    pub allowed_peers: Option<Vec<[u8; 32]>>,
    pub max_packet_size: usize,
    pub initial_rtt: Duration,
    pub max_ack_delay: Duration,
    pub idle_timeout: Duration,
    pub handshake_timeout: Duration,
    pub initial_max_data: u64,
    pub initial_max_stream_data: u64,
    pub max_streams: u64,
    pub max_send_buffer: usize,
    reset_key: [u8; 32],
}

impl Config {
    /// The same defaults as the Go implementation.
    pub fn new(identity: SigningKey, cluster_key: Vec<u8>) -> Config {
        let reset_key = crypto::reset_key(&cluster_key);
        Config {
            identity,
            cluster_key,
            allowed_peers: None,
            max_packet_size: 1350,
            initial_rtt: Duration::from_millis(100),
            max_ack_delay: Duration::from_millis(25),
            idle_timeout: Duration::from_secs(30),
            handshake_timeout: Duration::from_secs(10),
            initial_max_data: 8 << 20,
            initial_max_stream_data: 2 << 20,
            max_streams: 256,
            max_send_buffer: 2 << 20,
            reset_key,
        }
    }

    pub fn reset_key(&self) -> &[u8; 32] {
        &self.reset_key
    }

    fn local_params(&self) -> TransportParams {
        TransportParams {
            max_data: self.initial_max_data,
            max_stream_data: self.initial_max_stream_data,
            max_streams: self.max_streams,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum State {
    Handshake,   // the client has not proven its identity yet
    Established, // streams flow
    Closing,     // we sent CONNECTION_CLOSE; waiting out the drain period
    Draining,    // the peer closed; waiting out the drain period silently
    Closed,      // all state may be discarded
}

/// A CONNECTION_CLOSE, sent or received.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CloseError {
    pub code: u64,
    pub reason: String,
    pub remote: bool,
}

impl CloseError {
    pub fn local(code: u64, reason: &str) -> CloseError {
        CloseError { code, reason: reason.to_string(), remote: false }
    }
}

/// Why a connection ended.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConnError {
    Closed(CloseError),
    IdleTimeout,
    HandshakeTimeout,
    StatelessReset,
}

impl fmt::Display for ConnError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConnError::Closed(e) => {
                let who = if e.remote { "by peer" } else { "locally" };
                if e.reason.is_empty() {
                    write!(f, "closed {who} (code 0x{:x})", e.code)
                } else {
                    write!(f, "closed {who} (code 0x{:x}): {}", e.code, e.reason)
                }
            }
            ConnError::IdleTimeout => write!(f, "idle timeout"),
            ConnError::HandshakeTimeout => write!(f, "handshake timeout"),
            ConnError::StatelessReset => write!(f, "peer has no state for this connection (stateless reset)"),
        }
    }
}

#[derive(Debug)]
pub enum AcceptError {
    Wire(WireError),
    NotInitial,
    TooSmall(usize),
    Checksum,
    NoHello,
    Handshake(crypto::HandshakeError),
}

impl fmt::Display for AcceptError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AcceptError::Wire(e) => write!(f, "{e}"),
            AcceptError::NotInitial => write!(f, "not an initial packet"),
            AcceptError::TooSmall(n) => write!(f, "initial of {n} bytes is under the {MIN_INITIAL_SIZE}-byte minimum"),
            AcceptError::Checksum => write!(f, "initial failed its checksum"),
            AcceptError::NoHello => write!(f, "initial carries no valid HELLO"),
            AcceptError::Handshake(e) => write!(f, "handshake: {e:?}"),
        }
    }
}

/// Errors from the stream API.
#[derive(Debug, PartialEq, Eq)]
pub enum StreamError {
    /// No such stream (never opened, or retired).
    Unknown,
    /// The connection is not established (closed or closing).
    NotEstablished,
    /// This side already finished or reset its send half.
    WriteClosed,
    /// The peer sent STOP_SENDING with this code.
    Stopped(u64),
    /// This side abandoned the receive half.
    ReadClosed,
}

/// A snapshot of the transport counters.
#[derive(Clone, Debug, Default)]
pub struct Stats {
    pub srtt: Duration,
    pub min_rtt: Duration,
    pub cwnd: usize,
    pub bytes_in_flight: usize,
    pub packets_sent: u64,
    pub packets_received: u64,
    pub packets_lost: u64,
    pub packets_dropped: u64,
    pub bytes_sent: u64,
    pub bytes_received: u64,
    pub stream_bytes_retransmitted: u64,
    pub pto_count: u64,
    pub congestion_events: u64,
    pub streams_open: usize,
    pub handshake: Option<Duration>,
}

pub struct Conn {
    cfg: Arc<Config>,
    state: State,
    err: Option<ConnError>,

    local_cid: ConnId,
    remote_cid: ConnId,
    orig_dcid: ConnId,

    // handshake
    th: [u8; 32],
    server_hello: Vec<u8>,
    send_prot: Protector,
    recv_prot: Protector,
    peer_id: Option<[u8; 32]>,
    authed: bool,         // CLIENT_AUTH verified (also: handshake confirmed)
    addr_validated: bool, // the client proved it receives at its address
    pending_shello: bool, // SERVER_HELLO must be (re)sent
    peer_params: TransportParams,
    created: Instant,
    established_at: Option<Instant>,

    // packet numbers and acknowledgements
    next_pn: u64,
    acks: AckTracker,
    rec: Recovery,
    probes: u32,

    // streams (all client-initiated, so even IDs)
    streams: BTreeMap<u64, Stream>,
    rr: usize,
    peer_opened_idx: u64, // client streams opened so far
    local_max_idx: u64,   // client streams we allow
    accept_q: VecDeque<u64>,

    // connection flow control
    send_max_data: u64, // the client's limit on our new stream bytes
    sent_data: u64,
    recv_max_data: u64, // our advertised limit
    recv_data: u64,     // sum of every stream's highest offset
    consumed: u64,      // bytes released by the application (read or abandoned)

    // queued control frames
    ctrl: VecDeque<Frame>,
    need_max_data: bool,
    need_max_streams: bool,

    // close
    close_frame: Option<Frame>,
    close_sent: bool,
    drain_until: Option<Instant>,

    // timers / accounting
    last_recv: Instant,
    // The first ack-eliciting packet sent since we last received one: the
    // idle timer restarts then, and not on every later probe (RFC 9000
    // §10.1), so a vanished client times out instead of being probed forever.
    first_elicit_since_recv: Option<Instant>,
    ping_queued: bool,
    bytes_sent: u64,
    bytes_recv: u64,
    pkts_sent: u64,
    pkts_recv: u64,
    pkts_dropped: u64,
    retx_bytes: u64,
}

impl Conn {
    /// Accepts a connection from a client's Initial with fresh randomness.
    pub fn accept(cfg: Arc<Config>, initial: &[u8], now: Instant, local_cid: ConnId) -> Result<Conn, AcceptError> {
        let mut eph = [0u8; 32];
        let mut nonce = [0u8; 32];
        crypto::random_bytes(&mut eph);
        crypto::random_bytes(&mut nonce);
        Conn::accept_with(cfg, initial, now, StaticSecret::from(eph), nonce, local_cid)
    }

    /// Accepts with a given ephemeral key and nonce (deterministic in tests).
    pub fn accept_with(
        cfg: Arc<Config>,
        initial: &[u8],
        now: Instant,
        eph: StaticSecret,
        nonce: [u8; 32],
        local_cid: ConnId,
    ) -> Result<Conn, AcceptError> {
        let h = Header::parse(initial).map_err(AcceptError::Wire)?;
        if h.ty != PacketType::Initial {
            return Err(AcceptError::NotInitial);
        }
        if initial.len() < MIN_INITIAL_SIZE {
            return Err(AcceptError::TooSmall(initial.len()));
        }
        let payload = crypto::open_plain(initial).ok_or(AcceptError::Checksum)?;
        let frames = wire::parse_frames(payload).map_err(AcceptError::Wire)?;
        // As in Go, the last HANDSHAKE frame is the hello.
        let hello = frames
            .iter()
            .rev()
            .find_map(|f| if let Frame::Handshake(d) = f { Some(d) } else { None })
            .ok_or(AcceptError::NoHello)?;
        let sh = crypto::server_hello(hello, &eph, &nonce, &cfg.identity, cfg.local_params(), &cfg.cluster_key)
            .map_err(|e| match e {
                crypto::HandshakeError::Malformed => AcceptError::NoHello,
                e => AcceptError::Handshake(e),
            })?;
        let mss = cfg.max_packet_size;
        let mut c = Conn {
            state: State::Handshake,
            err: None,
            local_cid,
            remote_cid: h.scid,
            orig_dcid: h.dcid,
            th: sh.th,
            server_hello: sh.msg,
            send_prot: Protector::new(&sh.keys.s2c_key, &sh.keys.s2c_iv),
            recv_prot: Protector::new(&sh.keys.c2s_key, &sh.keys.c2s_iv),
            peer_id: None,
            authed: false,
            addr_validated: false,
            pending_shello: true,
            peer_params: sh.peer,
            created: now,
            established_at: None,
            next_pn: 0,
            acks: AckTracker::default(),
            rec: Recovery::new(mss),
            probes: 0,
            streams: BTreeMap::new(),
            rr: 0,
            peer_opened_idx: 0,
            local_max_idx: cfg.max_streams,
            accept_q: VecDeque::new(),
            send_max_data: sh.peer.max_data,
            sent_data: 0,
            recv_max_data: cfg.initial_max_data,
            recv_data: 0,
            consumed: 0,
            ctrl: VecDeque::new(),
            need_max_data: false,
            need_max_streams: false,
            close_frame: None,
            close_sent: false,
            drain_until: None,
            last_recv: now,
            first_elicit_since_recv: None,
            ping_queued: false,
            bytes_sent: 0,
            bytes_recv: initial.len() as u64,
            pkts_sent: 0,
            pkts_recv: 1,
            pkts_dropped: 0,
            retx_bytes: 0,
            cfg,
        };
        c.acks.record(now, h.pn, true);
        Ok(c)
    }

    // ---- accessors ----

    pub fn state(&self) -> State {
        self.state
    }

    pub fn is_closed(&self) -> bool {
        self.state == State::Closed
    }

    pub fn is_established(&self) -> bool {
        self.state == State::Established
    }

    pub fn error(&self) -> Option<&ConnError> {
        self.err.as_ref()
    }

    pub fn local_cid(&self) -> ConnId {
        self.local_cid
    }

    pub fn orig_dcid(&self) -> ConnId {
        self.orig_dcid
    }

    /// The client's proven Ed25519 key (None until authenticated).
    pub fn peer_identity(&self) -> Option<[u8; 32]> {
        self.peer_id
    }

    pub fn has_stream(&self, id: u64) -> bool {
        self.streams.contains_key(&id)
    }

    pub fn stats(&self) -> Stats {
        Stats {
            srtt: self.rec.rtt.smoothed,
            min_rtt: self.rec.rtt.min,
            cwnd: self.rec.cc.window(),
            bytes_in_flight: self.rec.bytes_in_flight,
            packets_sent: self.pkts_sent,
            packets_received: self.pkts_recv,
            packets_lost: self.rec.lost,
            packets_dropped: self.pkts_dropped,
            bytes_sent: self.bytes_sent,
            bytes_received: self.bytes_recv,
            stream_bytes_retransmitted: self.retx_bytes,
            pto_count: self.rec.ptos,
            congestion_events: self.rec.congestion_events,
            streams_open: self.streams.len(),
            handshake: self.established_at.map(|t| t.saturating_duration_since(self.created)),
        }
    }

    fn drain_period(&self) -> Duration {
        self.rec.rtt.pto(self.cfg.initial_rtt, self.cfg.max_ack_delay) * 3
    }

    fn max_ack_delay_for_pto(&self) -> Duration {
        if self.authed {
            self.cfg.max_ack_delay
        } else {
            Duration::ZERO
        }
    }

    // ---- receiving ----

    /// Processes one datagram. Undecryptable, duplicate or out-of-place
    /// packets are counted and dropped; protocol violations close the
    /// connection.
    pub fn handle_packet(&mut self, pkt: &[u8], now: Instant) {
        if self.state == State::Closed {
            return;
        }
        let Ok(h) = Header::parse(pkt) else {
            self.pkts_dropped += 1;
            return;
        };
        if h.ty == PacketType::Reset {
            self.handle_reset(&h, pkt);
            return;
        }
        if h.dcid != self.local_cid && !(h.ty == PacketType::Initial && h.dcid == self.orig_dcid) {
            self.pkts_dropped += 1;
            return;
        }
        self.bytes_recv += pkt.len() as u64;
        if self.acks.seen(h.pn) {
            self.pkts_dropped += 1;
            return;
        }
        let payload = match h.ty {
            PacketType::Initial if !self.authed => crypto::open_plain(pkt).map(<[u8]>::to_vec),
            PacketType::OneRtt => self.recv_prot.open(&h, pkt),
            _ => None, // Handshake packets only travel server to client
        };
        let Some(payload) = payload else {
            self.pkts_dropped += 1;
            return;
        };
        let frames = match wire::parse_frames(&payload) {
            Ok(f) => f,
            Err(_) => {
                self.pkts_dropped += 1;
                self.close_with(CloseError::local(CODE_PROTOCOL_VIOLATION, "protocol violation"), now);
                return;
            }
        };

        if self.state >= State::Closing {
            // Closing: answer with another CONNECTION_CLOSE (one per received
            // packet); draining: stay silent. A CLOSE from the peer moves us
            // from closing to draining.
            if frames.iter().any(|f| matches!(f, Frame::ConnectionClose { .. })) {
                self.state = State::Draining;
                if self.drain_until.is_none() {
                    self.drain_until = Some(now + self.drain_period());
                }
            }
            if self.state == State::Closing {
                self.close_sent = false;
            }
            return;
        }

        match h.ty {
            // A retransmitted HELLO: the client has not seen our reply.
            PacketType::Initial => self.pending_shello = true,
            PacketType::OneRtt => {
                if !self.authed && !self.server_auth(&frames, now) {
                    return; // dropped unacknowledged; the client retransmits
                }
                self.addr_validated = true;
            }
            _ => {}
        }

        let mut eliciting = false;
        for f in &frames {
            eliciting |= f.ack_eliciting();
            if let Err(e) = self.handle_frame(f, h.ty, now) {
                self.close_with(e, now);
                return;
            }
            if self.state >= State::Closing {
                break;
            }
        }
        self.acks.record(now, h.pn, eliciting);
        self.pkts_recv += 1;
        self.last_recv = now;
        self.first_elicit_since_recv = None;
    }

    fn handle_reset(&mut self, h: &Header, pkt: &[u8]) {
        if h.dcid != self.local_cid {
            return;
        }
        let token = crypto::reset_token(self.cfg.reset_key(), &self.remote_cid);
        match crypto::open_plain(pkt) {
            Some(body) if crypto::ct_eq(body, &token) => self.close_now(ConnError::StatelessReset),
            _ => self.pkts_dropped += 1,
        }
    }

    /// Requires the client's CLIENT_AUTH before anything else in its first
    /// 1-RTT packets is processed.
    fn server_auth(&mut self, frames: &[Frame], now: Instant) -> bool {
        for f in frames {
            let Frame::Handshake(msg) = f else { continue };
            match crypto::check_client_auth(msg, &self.th) {
                ClientAuth::Malformed => continue,
                ClientAuth::BadSignature => {
                    self.close_with(CloseError::local(CODE_HANDSHAKE_FAILED, "bad client signature"), now);
                    return false;
                }
                ClientAuth::Valid(pk) => {
                    if let Some(allowed) = &self.cfg.allowed_peers {
                        if !allowed.contains(&pk) {
                            self.close_with(CloseError::local(CODE_PEER_REJECTED, "client identity rejected"), now);
                            return false;
                        }
                    }
                    self.peer_id = Some(pk);
                    self.authed = true;
                    self.pending_shello = false;
                    self.ctrl.push_back(Frame::HandshakeDone);
                    self.state = State::Established;
                    self.established_at = Some(now);
                    return true;
                }
            }
        }
        self.pkts_dropped += 1;
        false
    }

    fn handle_frame(&mut self, f: &Frame, pt: PacketType, now: Instant) -> Result<(), CloseError> {
        match f {
            Frame::Padding(_) | Frame::Ping | Frame::Handshake(_) => Ok(()),
            Frame::Ack(a) => self.handle_ack(a, now),
            Frame::ConnectionClose { code, reason } => {
                let reason = String::from_utf8_lossy(reason).into_owned();
                self.err = Some(ConnError::Closed(CloseError { code: *code, reason, remote: true }));
                self.state = State::Draining;
                self.drain_until = Some(now + self.drain_period());
                Ok(())
            }
            // Only handshake, ACK and close frames travel in clear.
            _ if pt != PacketType::OneRtt => Err(CloseError::local(CODE_PROTOCOL_VIOLATION, "protocol violation")),
            _ => self.handle_app_frame(f),
        }
    }

    fn handle_app_frame(&mut self, f: &Frame) -> Result<(), CloseError> {
        match f {
            Frame::Stream { id, offset, data, fin } => {
                let Some(id) = self.peer_stream(*id)? else { return Ok(()) };
                let s = self.streams.get_mut(&id).expect("peer_stream returned it");
                let grew = s.receive(*offset, data, *fin)?;
                let released = if s.read_closed || s.reset_err.is_some() { s.release_to(s.recv_high) } else { 0 };
                self.recv_data += grew;
                if self.recv_data > self.recv_max_data {
                    return Err(CloseError::local(CODE_FLOW_CONTROL, "flow control limit exceeded"));
                }
                self.consume(released);
            }
            Frame::ResetStream { id, code, final_size } => {
                let Some(id) = self.peer_stream(*id)? else { return Ok(()) };
                let s = self.streams.get_mut(&id).expect("peer_stream returned it");
                if s.reset_err.is_some() {
                    return Ok(());
                }
                if *final_size < s.recv_high || s.final_size.is_some_and(|fs| fs != *final_size) {
                    return Err(CloseError::local(CODE_FINAL_SIZE, "data beyond final size"));
                }
                if *final_size > s.recv_max {
                    return Err(CloseError::local(CODE_FLOW_CONTROL, "flow control limit exceeded"));
                }
                let grew = final_size - s.recv_high;
                s.recv_high = *final_size;
                s.final_size = Some(*final_size);
                s.drop_recv_data();
                s.reset_err = Some(*code);
                // Everything the peer sent but the application never read is
                // abandoned: release its connection-level credit.
                let released = s.release_to(*final_size);
                self.recv_data += grew;
                if self.recv_data > self.recv_max_data {
                    return Err(CloseError::local(CODE_FLOW_CONTROL, "flow control limit exceeded"));
                }
                self.consume(released);
                self.maybe_retire(id);
            }
            Frame::StopSending { id, code } => {
                let Some(id) = self.peer_stream(*id)? else { return Ok(()) };
                let s = self.streams.get_mut(&id).expect("peer_stream returned it");
                if s.stop_err.is_none() && !s.fin_acked {
                    s.stop_err = Some(*code);
                    if !s.reset_sent {
                        s.reset_sent = true;
                        let final_size = s.send_next;
                        s.drop_send_data();
                        self.ctrl.push_back(Frame::ResetStream { id, code: *code, final_size });
                    }
                    self.maybe_retire(id);
                }
            }
            Frame::MaxData(m) => self.send_max_data = self.send_max_data.max(*m),
            Frame::MaxStreamData { id, max } => {
                let Some(id) = self.peer_stream(*id)? else { return Ok(()) };
                let s = self.streams.get_mut(&id).expect("peer_stream returned it");
                s.send_max = s.send_max.max(*max);
            }
            // The server never opens streams; credit hints are informational.
            Frame::MaxStreams(_) | Frame::DataBlocked(_) | Frame::StreamDataBlocked { .. } => {}
            Frame::HandshakeDone => {
                return Err(CloseError::local(CODE_PROTOCOL_VIOLATION, "HANDSHAKE_DONE sent to a server"))
            }
            Frame::Datagram(_) => {} // kvnode has no use for datagrams
            // Application backpressure describes a peer's worker pool; kvnode
            // serves each stream as it comes and has nothing to throttle.
            Frame::AppPressure { .. } => {}
            _ => return Err(CloseError::local(CODE_PROTOCOL_VIOLATION, "protocol violation")),
        }
        Ok(())
    }

    /// Resolves a stream ID from a received frame, implicitly opening client
    /// streams (and any lower-numbered ones). None for a stream that existed
    /// and has been retired: late frames for it are harmless.
    fn peer_stream(&mut self, id: u64) -> Result<Option<u64>, CloseError> {
        if self.streams.contains_key(&id) {
            return Ok(Some(id));
        }
        if id & 1 == 1 {
            return Err(CloseError::local(CODE_STREAM_STATE, "frame for a stream in the wrong state"));
        }
        let idx = id >> 1;
        if idx < self.peer_opened_idx {
            return Ok(None);
        }
        if idx >= self.local_max_idx {
            return Err(CloseError::local(CODE_STREAM_LIMIT, "stream limit exceeded"));
        }
        for i in self.peer_opened_idx..=idx {
            let sid = i << 1;
            self.streams.insert(
                sid,
                Stream::new(sid, self.peer_params.max_stream_data, self.cfg.initial_max_stream_data),
            );
            self.accept_q.push_back(sid);
        }
        self.peer_opened_idx = idx + 1;
        Ok(Some(id))
    }

    fn handle_ack(&mut self, a: &wire::Ack, now: Instant) -> Result<(), CloseError> {
        if a.largest() >= self.next_pn {
            return Err(CloseError::local(CODE_PROTOCOL_VIOLATION, "ACK for a packet never sent"));
        }
        let delay = Duration::from_micros(a.delay);
        let mad = self.max_ack_delay_for_pto();
        let Outcome { acked, lost } = self.rec.on_ack(now, a, delay, mad);
        for frames in acked {
            for sf in frames {
                if let SentFrame::Stream { id, offset, len, fin } = sf {
                    if let Some(s) = self.streams.get_mut(&id) {
                        s.on_acked(offset, len, fin);
                        self.maybe_retire(id);
                    }
                }
            }
        }
        for frames in lost {
            self.requeue(frames);
        }
        Ok(())
    }

    /// Puts whatever lost (or probed) packets carried that still matters
    /// back into the send queues.
    fn requeue(&mut self, frames: Vec<SentFrame>) {
        for sf in frames {
            match sf {
                SentFrame::Stream { id, offset, len, fin } => {
                    if let Some(s) = self.streams.get_mut(&id) {
                        s.on_lost(offset, len, fin);
                    }
                }
                SentFrame::ServerHello => {
                    if !self.authed {
                        self.pending_shello = true;
                    }
                }
                SentFrame::MaxData => self.need_max_data = true,
                SentFrame::MaxStreamData(id) => {
                    if let Some(s) = self.streams.get_mut(&id) {
                        if s.final_size.is_none() && !s.read_closed {
                            s.need_max = true;
                        }
                    }
                }
                SentFrame::MaxStreams => self.need_max_streams = true,
                SentFrame::Ctrl(f) => self.ctrl.push_back(f),
                SentFrame::Ping => {} // a PTO probes again if needed
            }
        }
    }

    /// Returns connection-level credit for n bytes the application released.
    fn consume(&mut self, n: u64) {
        if n == 0 {
            return;
        }
        self.consumed += n;
        let w = self.cfg.initial_max_data;
        if self.recv_max_data.saturating_sub(self.consumed) < w / 2 {
            self.recv_max_data = self.consumed + w;
            self.need_max_data = true;
        }
    }

    /// Forgets a stream once both halves are finished, granting the client a
    /// new stream slot.
    fn maybe_retire(&mut self, id: u64) {
        let Some(s) = self.streams.get(&id) else { return };
        if !s.send_done() || !s.recv_done() {
            return;
        }
        self.streams.remove(&id);
        self.local_max_idx += 1;
        self.need_max_streams = true;
    }

    // ---- the stream API ----

    /// The next stream the client opened, if any.
    pub fn accept_stream(&mut self) -> Option<u64> {
        self.accept_q.pop_front()
    }

    pub fn stream_read(&mut self, id: u64, buf: &mut [u8]) -> Result<ReadResult, StreamError> {
        let s = self.streams.get_mut(&id).ok_or(StreamError::Unknown)?;
        if s.read_closed {
            return Err(StreamError::ReadClosed);
        }
        let r = s.read(buf);
        let released = if let ReadResult::Data(_) = r { s.release_to(s.read_off) } else { 0 };
        self.consume(released);
        self.maybe_retire(id);
        Ok(r)
    }

    /// Queues data; returns how much was accepted (0 when the send buffer is
    /// full: wait for acknowledgements and try again).
    pub fn stream_write(&mut self, id: u64, data: &[u8]) -> Result<usize, StreamError> {
        if self.state != State::Established {
            return Err(StreamError::NotEstablished);
        }
        let s = self.streams.get_mut(&id).ok_or(StreamError::Unknown)?;
        if let Some(code) = s.stop_err {
            return Err(StreamError::Stopped(code));
        }
        if s.fin_queued || s.reset_sent {
            return Err(StreamError::WriteClosed);
        }
        let n = self.cfg.max_send_buffer.saturating_sub(s.buffered()).min(data.len());
        s.push(&data[..n]);
        Ok(n)
    }

    /// Whether stream_write would accept at least one byte (or fail).
    pub fn stream_writable(&self, id: u64) -> bool {
        match self.streams.get(&id) {
            Some(s) => s.stop_err.is_some() || s.fin_queued || s.reset_sent || s.buffered() < self.cfg.max_send_buffer,
            None => true,
        }
    }

    /// Half-closes the stream: queued data is delivered, then FIN.
    pub fn stream_close_write(&mut self, id: u64) {
        if let Some(s) = self.streams.get_mut(&id) {
            s.fin_queued = true;
        }
        self.maybe_retire(id);
    }

    /// Abandons the send half with an application error code.
    pub fn stream_reset(&mut self, id: u64, code: u64) {
        let Some(s) = self.streams.get_mut(&id) else { return };
        if s.reset_sent || s.fin_acked {
            return;
        }
        s.reset_sent = true;
        let final_size = s.send_next;
        s.drop_send_data();
        self.ctrl.push_back(Frame::ResetStream { id, code, final_size });
        self.maybe_retire(id);
    }

    /// Abandons the receive half: buffered data is dropped and the client is
    /// asked to stop sending.
    pub fn stream_stop_sending(&mut self, id: u64, code: u64) {
        let Some(s) = self.streams.get_mut(&id) else { return };
        if s.read_closed {
            return;
        }
        s.close_read();
        let released = s.release_to(s.recv_high);
        let ask = s.final_size.is_none();
        self.consume(released);
        if ask {
            self.ctrl.push_back(Frame::StopSending { id, code });
        }
        self.maybe_retire(id);
    }

    /// Starts an orderly close: the next poll sends CONNECTION_CLOSE, then
    /// the connection drains for three PTOs.
    pub fn close(&mut self, code: u64, reason: &str, now: Instant) {
        let mut reason = reason.to_string();
        reason.truncate(256);
        self.close_with(CloseError { code, reason, remote: false }, now);
    }

    fn close_with(&mut self, e: CloseError, _now: Instant) {
        if self.state >= State::Closing {
            return;
        }
        self.state = State::Closing;
        self.close_frame = Some(Frame::ConnectionClose { code: e.code, reason: e.reason.clone().into_bytes() });
        self.close_sent = false;
        self.err = Some(ConnError::Closed(e));
    }

    /// Discards the connection without telling the peer.
    fn close_now(&mut self, err: ConnError) {
        if self.state == State::Closed {
            return;
        }
        self.state = State::Closed;
        if self.err.is_none() {
            self.err = Some(err);
        }
    }

    // ---- sending ----

    /// Writes the next datagram to send into out (cleared first) and returns
    /// true, or false when there is nothing to send right now. Call it until
    /// it returns false after every handle_packet, handle_timeout, or stream
    /// call that queues data.
    pub fn poll(&mut self, out: &mut Vec<u8>, now: Instant) -> bool {
        out.clear();
        match self.state {
            State::Closed | State::Draining => return false,
            State::Closing => return self.poll_close(out, now),
            _ => {}
        }
        let mut max_size = self.cfg.max_packet_size;
        if !self.addr_validated {
            // Anti-amplification: until the client proves it can receive at
            // its address, send at most 3x what it sent us.
            let budget = 3 * self.bytes_recv as i64 - self.bytes_sent as i64;
            if budget < (HEADER_LEN + 64) as i64 {
                return false;
            }
            max_size = max_size.min(budget as usize);
        }
        if self.pending_shello {
            return self.poll_server_hello(out, now, max_size);
        }
        self.poll_one_rtt(out, now, max_size)
    }

    fn poll_server_hello(&mut self, out: &mut Vec<u8>, now: Instant, max_size: usize) -> bool {
        let mut payload = Vec::with_capacity(self.server_hello.len() + 32);
        wire::append_handshake(&mut payload, &self.server_hello);
        let ack = self.acks.any_new;
        if ack {
            self.acks.frame(now).append(&mut payload);
        }
        if HEADER_LEN + payload.len() + 4 > max_size {
            return false; // wait for more anti-amplification budget
        }
        self.pending_shello = false;
        self.finish(out, PacketType::Handshake, &payload, vec![SentFrame::ServerHello], true, ack, now);
        true
    }

    fn poll_close(&mut self, out: &mut Vec<u8>, now: Instant) -> bool {
        if self.close_sent {
            return false;
        }
        let Some(f) = self.close_frame.clone() else { return false };
        self.close_sent = true;
        if self.drain_until.is_none() {
            self.drain_until = Some(now + self.drain_period());
        }
        let mut payload = Vec::new();
        f.append(&mut payload);
        self.finish(out, PacketType::OneRtt, &payload, Vec::new(), false, false, now);
        true
    }

    /// Assembles an application packet in priority order: ACK, control
    /// frames, credit updates, retransmitted then new stream data.
    /// Ack-eliciting content is gated by the congestion window, except for
    /// PTO probes.
    fn poll_one_rtt(&mut self, out: &mut Vec<u8>, now: Instant, max_size: usize) -> bool {
        let room = max_size.saturating_sub(HEADER_LEN + AEAD_OVERHEAD);
        let mut payload = Vec::with_capacity(room);
        let ack_included = self.acks.any_new;
        if ack_included {
            self.acks.frame(now).append(&mut payload);
        }
        let probe = self.probes > 0;
        let can_send = probe || self.rec.bytes_in_flight + max_size <= self.rec.cc.window();
        let mut frames: Vec<SentFrame> = Vec::new();
        if can_send {
            while let Some(f) = self.ctrl.front() {
                if payload.len() + f.encoded_len() > room {
                    break;
                }
                let f = self.ctrl.pop_front().expect("front exists");
                f.append(&mut payload);
                frames.push(SentFrame::Ctrl(f));
            }
            if self.need_max_data {
                let f = Frame::MaxData(self.recv_max_data);
                if payload.len() + f.encoded_len() <= room {
                    f.append(&mut payload);
                    frames.push(SentFrame::MaxData);
                    self.need_max_data = false;
                }
            }
            if self.need_max_streams {
                let f = Frame::MaxStreams(self.local_max_idx);
                if payload.len() + f.encoded_len() <= room {
                    f.append(&mut payload);
                    frames.push(SentFrame::MaxStreams);
                    self.need_max_streams = false;
                }
            }
            for (id, s) in self.streams.iter_mut() {
                if s.need_max && s.final_size.is_none() && !s.read_closed {
                    let f = Frame::MaxStreamData { id: *id, max: s.recv_max };
                    if payload.len() + f.encoded_len() > room {
                        break;
                    }
                    f.append(&mut payload);
                    frames.push(SentFrame::MaxStreamData(*id));
                    s.need_max = false;
                }
            }
            self.fill_streams(&mut payload, &mut frames, room);
            if (self.ping_queued || (probe && frames.is_empty())) && payload.len() < room {
                payload.push(wire::T_PING as u8);
                frames.push(SentFrame::Ping);
                self.ping_queued = false;
            }
        }
        let eliciting = !frames.is_empty();
        if !eliciting && !(ack_included && self.acks.due(now, self.cfg.max_ack_delay)) {
            return false;
        }
        if probe && eliciting {
            self.probes -= 1;
        }
        self.finish(out, PacketType::OneRtt, &payload, frames, eliciting, ack_included, now);
        true
    }

    /// Adds STREAM frames round-robin across streams: retransmissions before
    /// new data within each stream, new data bounded by stream and
    /// connection credit.
    fn fill_streams(&mut self, payload: &mut Vec<u8>, frames: &mut Vec<SentFrame>, room: usize) {
        let ids: Vec<u64> = self.streams.keys().copied().collect();
        if ids.is_empty() {
            return;
        }
        let start = self.rr % ids.len();
        for k in 0..ids.len() {
            let id = ids[(start + k) % ids.len()];
            loop {
                let left = room.saturating_sub(payload.len());
                if left < 24 {
                    self.rr = (start + k) % ids.len();
                    return;
                }
                match self.next_stream_frame(id, left, payload) {
                    Some(sf) => frames.push(sf),
                    None => break,
                }
            }
        }
        self.rr = start + 1;
    }

    fn next_stream_frame(&mut self, id: u64, left: usize, payload: &mut Vec<u8>) -> Option<SentFrame> {
        let credit = self.send_max_data.saturating_sub(self.sent_data);
        let s = self.streams.get_mut(&id)?;
        if s.reset_sent || s.stop_err.is_some() {
            return None;
        }
        let send_end = s.send_end();
        // Retransmission first.
        if let Some((lo, hi)) = s.retx.first() {
            let over = wire::stream_overhead(id, lo, (hi - lo) as usize);
            if left <= over {
                return None;
            }
            let n = (hi - lo).min((left - over) as u64);
            let fin = s.fin_queued && lo + n == send_end;
            s.retx.remove(lo, lo + n);
            if fin {
                s.fin_sent = true;
                s.fin_lost = false;
            }
            self.retx_bytes += n;
            wire::append_stream(payload, id, lo, s.bytes_at(lo, n), fin);
            return Some(SentFrame::Stream { id, offset: lo, len: n, fin });
        }
        // New data.
        let limit = send_end.min(s.send_max).min(s.send_next + credit);
        if limit > s.send_next {
            let off = s.send_next;
            let over = wire::stream_overhead(id, off, (limit - off) as usize);
            if left <= over {
                return None;
            }
            let n = (limit - off).min((left - over) as u64);
            let fin = s.fin_queued && off + n == send_end;
            s.send_next += n;
            self.sent_data += n;
            if fin {
                s.fin_sent = true;
                s.fin_lost = false;
            }
            wire::append_stream(payload, id, off, s.bytes_at(off, n), fin);
            return Some(SentFrame::Stream { id, offset: off, len: n, fin });
        }
        // A bare FIN (all data already sent), or a lost FIN to repeat.
        if s.fin_queued && s.send_next == send_end && (!s.fin_sent || s.fin_lost) && !s.fin_acked {
            s.fin_sent = true;
            s.fin_lost = false;
            wire::append_stream(payload, id, send_end, &[], true);
            return Some(SentFrame::Stream { id, offset: send_end, len: 0, fin: true });
        }
        None
    }

    /// Whether anything is queued that a poll would send if the window
    /// allowed.
    pub fn has_pending_work(&self) -> bool {
        if !self.ctrl.is_empty() || self.need_max_data || self.need_max_streams || self.ping_queued || self.pending_shello {
            return true;
        }
        let credit = self.send_max_data.saturating_sub(self.sent_data);
        self.streams.values().any(|s| s.has_send_work(credit) || s.need_max)
    }

    /// Seals the payload into out and records the packet.
    #[allow(clippy::too_many_arguments)]
    fn finish(
        &mut self,
        out: &mut Vec<u8>,
        pt: PacketType,
        payload: &[u8],
        frames: Vec<SentFrame>,
        eliciting: bool,
        ack_included: bool,
        now: Instant,
    ) {
        let h = Header::new(pt, self.remote_cid, self.local_cid, self.next_pn);
        self.next_pn += 1;
        out.clear();
        if pt == PacketType::OneRtt {
            self.send_prot.seal(out, h, payload);
        } else {
            crypto::seal_plain(out, h, payload);
        }
        let n = out.len();
        self.rec.on_sent(SentPacket::new(h.pn, now, n, eliciting, frames));
        if ack_included {
            self.acks.sent();
        }
        self.bytes_sent += n as u64;
        self.pkts_sent += 1;
        if eliciting && self.first_elicit_since_recv.is_none() {
            self.first_elicit_since_recv = Some(now);
        }
    }

    // ---- timers ----

    fn idle_deadline(&self) -> Instant {
        let last = match self.first_elicit_since_recv {
            Some(t) if t > self.last_recv => t,
            _ => self.last_recv,
        };
        last + self.cfg.idle_timeout.max(self.drain_period())
    }

    /// When handle_timeout must next be called.
    pub fn timeout(&self) -> Option<Instant> {
        if self.state == State::Closed {
            return None;
        }
        if self.state >= State::Closing {
            return self.drain_until;
        }
        let mut t = self.idle_deadline();
        let mut consider = |x: Option<Instant>| {
            if let Some(x) = x {
                t = t.min(x);
            }
        };
        if self.state == State::Handshake {
            consider(Some(self.created + self.cfg.handshake_timeout));
        }
        consider(self.rec.timer(self.cfg.initial_rtt, self.max_ack_delay_for_pto()));
        consider(self.acks.deadline(self.cfg.max_ack_delay));
        Some(t)
    }

    /// Runs whatever timers have expired at now.
    pub fn handle_timeout(&mut self, now: Instant) {
        if self.state == State::Closed {
            return;
        }
        if self.state >= State::Closing {
            if self.drain_until.is_some_and(|d| now >= d) {
                self.state = State::Closed;
            }
            return;
        }
        if self.state == State::Handshake && now >= self.created + self.cfg.handshake_timeout {
            self.close_now(ConnError::HandshakeTimeout);
            return;
        }
        if now >= self.idle_deadline() {
            self.close_now(ConnError::IdleTimeout);
            return;
        }
        let mad = self.max_ack_delay_for_pto();
        if let Some(rt) = self.rec.timer(self.cfg.initial_rtt, mad) {
            if now >= rt {
                let (lost, probe) = self.rec.on_timeout(now, mad);
                for frames in lost {
                    self.requeue(frames);
                }
                if probe {
                    // Probe with the oldest outstanding frames (probably what
                    // was lost).
                    self.probes = 2;
                    match self.rec.oldest_unacked_frames() {
                        Some(frames) => self.requeue(frames),
                        None => self.ping_queued = true,
                    }
                }
            }
        }
    }
}
