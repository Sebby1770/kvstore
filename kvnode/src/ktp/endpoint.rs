//! A KTP server endpoint on one UDP socket: a receive thread, a timer thread,
//! and blocking stream handles (`Read + Write`) for application threads.
//!
//! All connections live behind one mutex. Datagrams are routed by
//! destination connection ID (retransmitted Initials by the client's
//! original one); a packet for a connection we have no state for -- the
//! client is still talking to a previous incarnation of this process -- is
//! answered with a stateless reset, so the client redials at once instead of
//! waiting out its idle timeout.

use std::collections::{HashMap, VecDeque};
use std::io::{self, ErrorKind, Read, Write};
use std::net::{SocketAddr, UdpSocket};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread;
use std::time::{Duration, Instant};

use super::conn::{Config, Conn, StreamError, CODE_APPLICATION, CODE_NO_ERROR};
use super::crypto;
use super::stream::ReadResult;
use super::wire::{ConnId, Header, PacketType};

/// Concurrent connections; further Initials are dropped.
const MAX_CONNS: usize = 1024;
/// Stateless resets sent per second, at most.
const RESET_RATE: u32 = 64;

/// Endpoint counters.
#[derive(Clone, Debug, Default)]
pub struct EndpointStats {
    pub datagrams: u64,
    pub malformed: u64,
    pub accepted: u64,
    pub rejected: u64,
    pub unroutable: u64,
    pub resets_sent: u64,
    pub closed: u64,
}

struct Entry {
    conn: Conn,
    addr: SocketAddr,
    opened: Instant,
    announced: bool,
}

struct EpState {
    conns: HashMap<ConnId, Entry>,
    initial: HashMap<ConnId, ConnId>, // client's original DCID -> our CID
    accept_q: VecDeque<(ConnId, u64)>,
    out: Vec<u8>,
    closed: bool,
    reset_window: Instant,
    resets: u32,
    stats: EndpointStats,
}

struct Inner {
    socket: UdpSocket,
    cfg: Arc<Config>,
    state: Mutex<EpState>,
    app_cv: Condvar,   // accept() and stream reads/writes wait here
    timer_cv: Condvar, // the timer thread waits here
    log: bool,
}

impl Inner {
    fn lock(&self) -> MutexGuard<'_, EpState> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn changed(&self) {
        self.app_cv.notify_all();
        self.timer_cv.notify_one();
    }
}

/// A KTP server bound to one UDP socket.
pub struct Endpoint {
    inner: Arc<Inner>,
}

impl Endpoint {
    /// Binds addr and starts the endpoint's threads. `log` prints a line per
    /// connection established and closed.
    pub fn bind(addr: &str, cfg: Config, log: bool) -> io::Result<Endpoint> {
        let socket = UdpSocket::bind(addr)?;
        socket.set_read_timeout(Some(Duration::from_millis(250)))?;
        let inner = Arc::new(Inner {
            socket,
            cfg: Arc::new(cfg),
            state: Mutex::new(EpState {
                conns: HashMap::new(),
                initial: HashMap::new(),
                accept_q: VecDeque::new(),
                out: Vec::with_capacity(2048),
                closed: false,
                reset_window: Instant::now(),
                resets: 0,
                stats: EndpointStats::default(),
            }),
            app_cv: Condvar::new(),
            timer_cv: Condvar::new(),
            log,
        });
        let rx = Arc::clone(&inner);
        thread::Builder::new().name("ktp-recv".into()).spawn(move || recv_loop(rx))?;
        let tm = Arc::clone(&inner);
        thread::Builder::new().name("ktp-timer".into()).spawn(move || timer_loop(tm))?;
        Ok(Endpoint { inner })
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.socket.local_addr()
    }

    pub fn stats(&self) -> EndpointStats {
        self.inner.lock().stats.clone()
    }

    /// Blocks until a client opens a stream.
    pub fn accept(&self) -> io::Result<KtpStream> {
        let inner = &self.inner;
        let mut st = inner.lock();
        loop {
            if st.closed {
                return Err(io::Error::new(ErrorKind::NotConnected, "ktp: endpoint closed"));
            }
            while let Some((cid, id)) = st.accept_q.pop_front() {
                if let Some(e) = st.conns.get(&cid) {
                    if e.conn.has_stream(id) {
                        let handle = Handle {
                            inner: Arc::clone(inner),
                            cid,
                            id,
                            peer: e.addr,
                            identity: e.conn.peer_identity(),
                            eof: AtomicBool::new(false),
                        };
                        return Ok(KtpStream { h: Arc::new(handle) });
                    }
                }
            }
            st = inner.app_cv.wait(st).unwrap_or_else(|e| e.into_inner());
        }
    }

    /// Closes every connection (CONNECTION_CLOSE) and stops the threads.
    pub fn close(&self) {
        let inner = &self.inner;
        let mut st = inner.lock();
        let now = Instant::now();
        let cids: Vec<ConnId> = st.conns.keys().copied().collect();
        for cid in cids {
            if let Some(e) = st.conns.get_mut(&cid) {
                e.conn.close(CODE_NO_ERROR, "endpoint closing", now);
            }
            flush(inner, &mut st, cid, now);
        }
        st.closed = true;
        drop(st);
        inner.changed();
    }
}

impl Drop for Endpoint {
    fn drop(&mut self) {
        self.close();
    }
}

fn recv_loop(inner: Arc<Inner>) {
    let mut buf = vec![0u8; 65536];
    loop {
        match inner.socket.recv_from(&mut buf) {
            Ok((n, from)) => {
                let mut st = inner.lock();
                if st.closed {
                    return;
                }
                handle_datagram(&inner, &mut st, &buf[..n], from, Instant::now());
                drop(st);
                inner.changed();
            }
            Err(e) if matches!(e.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut | ErrorKind::Interrupted) => {
                if inner.lock().closed {
                    return;
                }
            }
            Err(_) => {
                // e.g. an ICMP error surfaced by the OS: nothing to do but go on.
                if inner.lock().closed {
                    return;
                }
                thread::sleep(Duration::from_millis(1));
            }
        }
    }
}

fn timer_loop(inner: Arc<Inner>) {
    let mut st = inner.lock();
    loop {
        if st.closed {
            return;
        }
        let now = Instant::now();
        let due: Vec<ConnId> = st
            .conns
            .iter()
            .filter(|(_, e)| e.conn.timeout().is_some_and(|t| t <= now))
            .map(|(cid, _)| *cid)
            .collect();
        let fired = !due.is_empty();
        for cid in due {
            if let Some(e) = st.conns.get_mut(&cid) {
                e.conn.handle_timeout(now);
            }
            flush(&inner, &mut st, cid, now);
        }
        if fired {
            inner.app_cv.notify_all();
        }
        let next = st.conns.values().filter_map(|e| e.conn.timeout()).min();
        // Never spin: a deadline that stays in the past (say, an ACK held
        // back by the anti-amplification limit) is retried each millisecond.
        let wait = match next {
            Some(t) => t.saturating_duration_since(Instant::now()).max(Duration::from_millis(1)),
            None => Duration::from_secs(1),
        };
        st = inner.timer_cv.wait_timeout(st, wait).unwrap_or_else(|e| e.into_inner()).0;
    }
}

fn handle_datagram(inner: &Inner, st: &mut EpState, pkt: &[u8], from: SocketAddr, now: Instant) {
    st.stats.datagrams += 1;
    let Ok(h) = Header::parse(pkt) else {
        st.stats.malformed += 1;
        return;
    };
    if h.ty == PacketType::Raw {
        return; // gossip is for mesh nodes; storage nodes do not take part
    }
    let mut cid = st.conns.contains_key(&h.dcid).then_some(h.dcid);
    if cid.is_none() && h.ty == PacketType::Initial {
        cid = st.initial.get(&h.dcid).copied();
        if cid.is_none() {
            accept_conn(inner, st, pkt, from, now);
            return;
        }
    }
    let Some(cid) = cid else {
        st.stats.unroutable += 1;
        if h.ty != PacketType::Reset {
            send_reset(inner, st, &h, from, now);
        }
        return;
    };
    if let Some(e) = st.conns.get_mut(&cid) {
        e.conn.handle_packet(pkt, now);
    }
    flush(inner, st, cid, now);
}

fn accept_conn(inner: &Inner, st: &mut EpState, pkt: &[u8], from: SocketAddr, now: Instant) {
    if st.conns.len() >= MAX_CONNS {
        st.stats.rejected += 1;
        return;
    }
    let mut cid: ConnId = [0; 8];
    loop {
        crypto::random_bytes(&mut cid);
        if !st.conns.contains_key(&cid) {
            break;
        }
    }
    match Conn::accept(Arc::clone(&inner.cfg), pkt, now, cid) {
        Ok(conn) => {
            st.initial.insert(conn.orig_dcid(), cid);
            st.conns.insert(cid, Entry { conn, addr: from, opened: now, announced: false });
            st.stats.accepted += 1;
            flush(inner, st, cid, now);
        }
        Err(_) => st.stats.rejected += 1,
    }
}

fn send_reset(inner: &Inner, st: &mut EpState, h: &Header, to: SocketAddr, now: Instant) {
    if now.saturating_duration_since(st.reset_window) >= Duration::from_secs(1) {
        st.reset_window = now;
        st.resets = 0;
    }
    if st.resets >= RESET_RATE {
        return;
    }
    st.resets += 1;
    st.stats.resets_sent += 1;
    let _ = inner.socket.send_to(&crypto::stateless_reset(inner.cfg.reset_key(), h), to);
}

/// Sends everything a connection has queued, hands its new streams to
/// accept(), and forgets it once it is closed.
fn flush(inner: &Inner, st: &mut EpState, cid: ConnId, now: Instant) {
    let EpState { conns, initial, accept_q, out, stats, .. } = st;
    let Some(e) = conns.get_mut(&cid) else { return };
    while e.conn.poll(out, now) {
        // A failed send (say ENOBUFS during a burst) is just a lost packet.
        let _ = inner.socket.send_to(out, e.addr);
    }
    while let Some(id) = e.conn.accept_stream() {
        accept_q.push_back((cid, id));
    }
    if !e.announced && e.conn.is_established() {
        e.announced = true;
        if inner.log {
            let who = e.conn.peer_identity().map(|k| fingerprint(&k)).unwrap_or_default();
            eprintln!("kvnode: ktp: {} connected (peer {who})", e.addr);
        }
    }
    if e.conn.is_closed() {
        let e = conns.remove(&cid).expect("entry exists");
        if initial.get(&e.conn.orig_dcid()) == Some(&cid) {
            initial.remove(&e.conn.orig_dcid());
        }
        stats.closed += 1;
        if inner.log && e.announced {
            let s = e.conn.stats();
            let why = e.conn.error().map(|e| e.to_string()).unwrap_or_else(|| "closed".into());
            eprintln!(
                "kvnode: ktp: {} {why} after {:.1}s (packets sent {}, received {}, lost {}; srtt {:.2}ms)",
                e.addr,
                now.saturating_duration_since(e.opened).as_secs_f64(),
                s.packets_sent,
                s.packets_received,
                s.packets_lost,
                s.srtt.as_secs_f64() * 1e3
            );
        }
    }
}

/// A short, human-comparable form of an Ed25519 public key.
pub fn fingerprint(key: &[u8; 32]) -> String {
    key[..8].iter().map(|b| format!("{b:02x}")).collect()
}

struct Handle {
    inner: Arc<Inner>,
    cid: ConnId,
    id: u64,
    peer: SocketAddr,
    identity: Option<[u8; 32]>,
    eof: AtomicBool,
}

impl Drop for Handle {
    fn drop(&mut self) {
        // The application is done with the stream: deliver what it wrote,
        // then FIN; if it stopped reading before the client's FIN, ask the
        // client to stop sending.
        let inner = &self.inner;
        let mut st = inner.lock();
        if let Some(e) = st.conns.get_mut(&self.cid) {
            e.conn.stream_close_write(self.id);
            if !self.eof.load(Ordering::Relaxed) {
                e.conn.stream_stop_sending(self.id, CODE_APPLICATION);
            }
            flush(inner, &mut st, self.cid, Instant::now());
        }
        drop(st);
        inner.changed();
    }
}

/// One stream of a KTP connection, used exactly like a TCP connection.
/// Clones share the stream; it is finished when the last clone is dropped.
pub struct KtpStream {
    h: Arc<Handle>,
}

fn closed_err(kind: ErrorKind, what: &str) -> io::Error {
    io::Error::new(kind, format!("ktp: {what}"))
}

impl KtpStream {
    pub fn try_clone(&self) -> io::Result<KtpStream> {
        Ok(KtpStream { h: Arc::clone(&self.h) })
    }

    pub fn peer_addr(&self) -> SocketAddr {
        self.h.peer
    }

    /// The client's authenticated Ed25519 identity.
    pub fn peer_identity(&self) -> Option<[u8; 32]> {
        self.h.identity
    }
}

impl Read for KtpStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() || self.h.eof.load(Ordering::Relaxed) {
            return Ok(0);
        }
        let inner = &self.h.inner;
        let mut st = inner.lock();
        loop {
            let now = Instant::now();
            let Some(e) = st.conns.get_mut(&self.h.cid) else {
                return Err(closed_err(ErrorKind::ConnectionAborted, "connection closed"));
            };
            match e.conn.stream_read(self.h.id, buf) {
                Ok(ReadResult::Data(n)) => {
                    // Reading may have opened the flow-control window.
                    flush(inner, &mut st, self.h.cid, now);
                    inner.timer_cv.notify_one();
                    return Ok(n);
                }
                Ok(ReadResult::Eof) => {
                    self.h.eof.store(true, Ordering::Relaxed);
                    return Ok(0);
                }
                Ok(ReadResult::Reset(code)) => {
                    return Err(closed_err(ErrorKind::ConnectionReset, &format!("stream reset by peer (code 0x{code:x})")));
                }
                Ok(ReadResult::WouldBlock) => {
                    if !e.conn.is_established() {
                        let why = e.conn.error().map(|e| e.to_string()).unwrap_or_else(|| "connection closing".into());
                        return Err(closed_err(ErrorKind::ConnectionAborted, &why));
                    }
                    st = inner.app_cv.wait(st).unwrap_or_else(|e| e.into_inner());
                }
                Err(StreamError::Unknown) => return Err(closed_err(ErrorKind::ConnectionAborted, "stream closed")),
                Err(e) => return Err(closed_err(ErrorKind::Other, &format!("{e:?}"))),
            }
        }
    }
}

impl Write for KtpStream {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        if data.is_empty() {
            return Ok(0);
        }
        let inner = &self.h.inner;
        let mut st = inner.lock();
        loop {
            let now = Instant::now();
            let Some(e) = st.conns.get_mut(&self.h.cid) else {
                return Err(closed_err(ErrorKind::BrokenPipe, "connection closed"));
            };
            match e.conn.stream_write(self.h.id, data) {
                Ok(0) => st = inner.app_cv.wait(st).unwrap_or_else(|e| e.into_inner()),
                Ok(n) => {
                    flush(inner, &mut st, self.h.cid, now);
                    inner.timer_cv.notify_one();
                    return Ok(n);
                }
                Err(StreamError::Stopped(code)) => {
                    return Err(closed_err(ErrorKind::BrokenPipe, &format!("peer stopped reading (code 0x{code:x})")));
                }
                Err(e) => return Err(closed_err(ErrorKind::BrokenPipe, &format!("{e:?}"))),
            }
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(()) // every write is handed to the connection (and sent) at once
    }
}
