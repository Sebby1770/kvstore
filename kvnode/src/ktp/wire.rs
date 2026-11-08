//! KTP/1 byte-level codec: the fixed 28-byte packet header, QUIC-style
//! varints, every frame type, and CRC32C. Byte-for-byte compatible with the
//! Go implementation in kvgw/netstack/wire (tests/ktp_vectors.txt pins it).
//! Parsers return errors on hostile input; they never panic.
//!
//! ```text
//!  0  1B  type (high nibble) | reserved (low nibble, must be 0)
//!  1  1B  version (1)
//!  2  8B  destination connection ID
//! 10  8B  source connection ID
//! 18  8B  packet number
//! 26  2B  payload length (frames + AEAD tag or CRC)
//! 28  …   payload
//! ```

use std::fmt;

pub const VERSION: u8 = 1;
pub const HEADER_LEN: usize = 28;
pub const MAX_VARINT: u64 = (1 << 62) - 1;
/// Bound on the ranges in one ACK frame.
pub const MAX_ACK_RANGES: usize = 32;

pub type ConnId = [u8; 8];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PacketType {
    Initial = 1,
    Handshake = 2,
    OneRtt = 3,
    Raw = 4,
    Reset = 5,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Header {
    pub ty: PacketType,
    pub dcid: ConnId,
    pub scid: ConnId,
    pub pn: u64,
    pub payload_len: u16,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WireError {
    Short,
    Type,
    Version,
    Length,
    Frame,
    FrameType(u64),
}

impl fmt::Display for WireError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WireError::Short => write!(f, "wire: truncated input"),
            WireError::Type => write!(f, "wire: unknown packet type"),
            WireError::Version => write!(f, "wire: unsupported version"),
            WireError::Length => write!(f, "wire: payload length does not match datagram"),
            WireError::Frame => write!(f, "wire: malformed frame"),
            WireError::FrameType(t) => write!(f, "wire: unknown frame type 0x{t:x}"),
        }
    }
}

impl std::error::Error for WireError {}

impl Header {
    pub fn new(ty: PacketType, dcid: ConnId, scid: ConnId, pn: u64) -> Header {
        Header { ty, dcid, scid, pn, payload_len: 0 }
    }

    pub fn append(&self, b: &mut Vec<u8>) {
        b.push((self.ty as u8) << 4);
        b.push(VERSION);
        b.extend_from_slice(&self.dcid);
        b.extend_from_slice(&self.scid);
        b.extend_from_slice(&self.pn.to_be_bytes());
        b.extend_from_slice(&self.payload_len.to_be_bytes());
    }

    /// Decodes a datagram's header and checks that the declared payload
    /// length matches the datagram exactly (KTP never coalesces packets).
    pub fn parse(pkt: &[u8]) -> Result<Header, WireError> {
        if pkt.len() < HEADER_LEN {
            return Err(WireError::Short);
        }
        if pkt[0] & 0x0f != 0 {
            return Err(WireError::Type);
        }
        let ty = match pkt[0] >> 4 {
            1 => PacketType::Initial,
            2 => PacketType::Handshake,
            3 => PacketType::OneRtt,
            4 => PacketType::Raw,
            5 => PacketType::Reset,
            _ => return Err(WireError::Type),
        };
        if pkt[1] != VERSION {
            return Err(WireError::Version);
        }
        let mut dcid = [0u8; 8];
        dcid.copy_from_slice(&pkt[2..10]);
        let mut scid = [0u8; 8];
        scid.copy_from_slice(&pkt[10..18]);
        let mut pn = [0u8; 8];
        pn.copy_from_slice(&pkt[18..26]);
        let payload_len = u16::from_be_bytes([pkt[26], pkt[27]]);
        if payload_len as usize != pkt.len() - HEADER_LEN {
            return Err(WireError::Length);
        }
        Ok(Header { ty, dcid, scid, pn: u64::from_be_bytes(pn), payload_len })
    }
}

// ---- CRC32C (Castagnoli, reflected polynomial 0x82F63B78) ----

const fn crc_table() -> [u32; 256] {
    let mut t = [0u32; 256];
    let mut i = 0;
    while i < 256 {
        let mut c = i as u32;
        let mut k = 0;
        while k < 8 {
            c = if c & 1 != 0 { (c >> 1) ^ 0x82F6_3B78 } else { c >> 1 };
            k += 1;
        }
        t[i] = c;
        i += 1;
    }
    t
}

static CRC_TABLE: [u32; 256] = crc_table();

pub fn crc32c(b: &[u8]) -> u32 {
    let mut c = !0u32;
    for &x in b {
        c = CRC_TABLE[((c ^ x as u32) & 0xff) as usize] ^ (c >> 8);
    }
    !c
}

// ---- varints (RFC 9000 §16) ----

pub fn varint_len(v: u64) -> usize {
    match v {
        0..=63 => 1,
        64..=16383 => 2,
        16384..=1_073_741_823 => 4,
        _ => 8,
    }
}

/// Appends the varint encoding of v (values above MAX_VARINT are clamped,
/// like the Go codec; frames never carry them).
pub fn put_varint(b: &mut Vec<u8>, v: u64) {
    let v = v.min(MAX_VARINT);
    match varint_len(v) {
        1 => b.push(v as u8),
        2 => b.extend_from_slice(&((v as u16) | 0x4000).to_be_bytes()),
        4 => b.extend_from_slice(&((v as u32) | 0x8000_0000).to_be_bytes()),
        _ => b.extend_from_slice(&(v | 0xc000_0000_0000_0000).to_be_bytes()),
    }
}

/// Decodes one varint from the front of b: (value, encoded length).
pub fn get_varint(b: &[u8]) -> Option<(u64, usize)> {
    let first = *b.first()?;
    let n = 1usize << (first >> 6);
    if b.len() < n {
        return None;
    }
    let mut v = (first & 0x3f) as u64;
    for &x in &b[1..n] {
        v = (v << 8) | x as u64;
    }
    Some((v, n))
}

// ---- frames ----

pub const T_PADDING: u64 = 0x00;
pub const T_PING: u64 = 0x01;
pub const T_ACK: u64 = 0x02;
pub const T_RESET_STREAM: u64 = 0x04;
pub const T_STOP_SENDING: u64 = 0x05;
pub const T_HANDSHAKE: u64 = 0x06;
pub const T_STREAM: u64 = 0x08; // 0x08..0x0b: | 0x02 has-length, | 0x01 FIN
pub const T_MAX_DATA: u64 = 0x10;
pub const T_MAX_STREAM_DATA: u64 = 0x11;
pub const T_MAX_STREAMS: u64 = 0x12;
pub const T_DATA_BLOCKED: u64 = 0x14;
pub const T_STREAM_DATA_BLOCKED: u64 = 0x15;
pub const T_CONNECTION_CLOSE: u64 = 0x1c;
pub const T_HANDSHAKE_DONE: u64 = 0x1e;
pub const T_DATAGRAM: u64 = 0x30;

/// Acknowledged packet numbers: inclusive (smallest, largest) ranges,
/// largest first, neither overlapping nor touching. `delay` is how long (µs)
/// the receiver held the ACK after receiving the largest.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct Ack {
    pub delay: u64,
    pub ranges: Vec<(u64, u64)>,
}

impl Ack {
    pub fn largest(&self) -> u64 {
        self.ranges.first().map_or(0, |r| r.1)
    }

    pub fn contains(&self, pn: u64) -> bool {
        self.ranges.iter().any(|&(lo, hi)| pn >= lo && pn <= hi)
    }

    pub fn append(&self, b: &mut Vec<u8>) {
        b.push(T_ACK as u8);
        // An ACK must acknowledge something; encode packet 0 rather than
        // garbage if a caller ever builds an empty one (as Go does).
        let (s0, l0) = self.ranges.first().copied().unwrap_or((0, 0));
        put_varint(b, l0);
        put_varint(b, self.delay);
        put_varint(b, self.ranges.len().max(1) as u64 - 1);
        put_varint(b, l0 - s0);
        let mut prev = s0;
        for &(s, l) in self.ranges.iter().skip(1) {
            put_varint(b, prev - l - 2); // gap
            put_varint(b, l - s);
            prev = s;
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Frame {
    Padding(usize),
    Ping,
    Ack(Ack),
    ResetStream { id: u64, code: u64, final_size: u64 },
    StopSending { id: u64, code: u64 },
    Handshake(Vec<u8>),
    Stream { id: u64, offset: u64, data: Vec<u8>, fin: bool },
    MaxData(u64),
    MaxStreamData { id: u64, max: u64 },
    MaxStreams(u64),
    DataBlocked(u64),
    StreamDataBlocked { id: u64, limit: u64 },
    ConnectionClose { code: u64, reason: Vec<u8> },
    HandshakeDone,
    Datagram(Vec<u8>),
}

/// The encoded size of a STREAM frame minus its data.
pub fn stream_overhead(id: u64, offset: u64, n: usize) -> usize {
    1 + varint_len(id) + varint_len(offset) + varint_len(n as u64)
}

/// Appends a STREAM frame (always with the length flag, as Go does).
pub fn append_stream(b: &mut Vec<u8>, id: u64, offset: u64, data: &[u8], fin: bool) {
    b.push((T_STREAM | 0x02 | fin as u64) as u8);
    put_varint(b, id);
    put_varint(b, offset);
    put_varint(b, data.len() as u64);
    b.extend_from_slice(data);
}

/// Appends a HANDSHAKE frame carrying msg.
pub fn append_handshake(b: &mut Vec<u8>, msg: &[u8]) {
    b.push(T_HANDSHAKE as u8);
    put_varint(b, msg.len() as u64);
    b.extend_from_slice(msg);
}

impl Frame {
    pub fn append(&self, b: &mut Vec<u8>) {
        match self {
            Frame::Padding(n) => b.resize(b.len() + n, 0),
            Frame::Ping => b.push(T_PING as u8),
            Frame::Ack(a) => a.append(b),
            Frame::ResetStream { id, code, final_size } => {
                b.push(T_RESET_STREAM as u8);
                put_varint(b, *id);
                put_varint(b, *code);
                put_varint(b, *final_size);
            }
            Frame::StopSending { id, code } => {
                b.push(T_STOP_SENDING as u8);
                put_varint(b, *id);
                put_varint(b, *code);
            }
            Frame::Handshake(d) => append_handshake(b, d),
            Frame::Stream { id, offset, data, fin } => append_stream(b, *id, *offset, data, *fin),
            Frame::MaxData(m) => {
                b.push(T_MAX_DATA as u8);
                put_varint(b, *m);
            }
            Frame::MaxStreamData { id, max } => {
                b.push(T_MAX_STREAM_DATA as u8);
                put_varint(b, *id);
                put_varint(b, *max);
            }
            Frame::MaxStreams(m) => {
                b.push(T_MAX_STREAMS as u8);
                put_varint(b, *m);
            }
            Frame::DataBlocked(l) => {
                b.push(T_DATA_BLOCKED as u8);
                put_varint(b, *l);
            }
            Frame::StreamDataBlocked { id, limit } => {
                b.push(T_STREAM_DATA_BLOCKED as u8);
                put_varint(b, *id);
                put_varint(b, *limit);
            }
            Frame::ConnectionClose { code, reason } => {
                b.push(T_CONNECTION_CLOSE as u8);
                put_varint(b, *code);
                put_varint(b, reason.len() as u64);
                b.extend_from_slice(reason);
            }
            Frame::HandshakeDone => b.push(T_HANDSHAKE_DONE as u8),
            Frame::Datagram(d) => {
                b.push(T_DATAGRAM as u8);
                put_varint(b, d.len() as u64);
                b.extend_from_slice(d);
            }
        }
    }

    pub fn encoded_len(&self) -> usize {
        match self {
            Frame::Padding(n) => *n,
            Frame::Ping | Frame::HandshakeDone => 1,
            Frame::Handshake(d) | Frame::Datagram(d) => 1 + varint_len(d.len() as u64) + d.len(),
            Frame::Stream { id, offset, data, .. } => stream_overhead(*id, *offset, data.len()) + data.len(),
            Frame::ResetStream { id, code, final_size } => {
                1 + varint_len(*id) + varint_len(*code) + varint_len(*final_size)
            }
            Frame::StopSending { id, code } => 1 + varint_len(*id) + varint_len(*code),
            Frame::MaxData(v) | Frame::MaxStreams(v) | Frame::DataBlocked(v) => 1 + varint_len(*v),
            Frame::MaxStreamData { id, max: v } | Frame::StreamDataBlocked { id, limit: v } => {
                1 + varint_len(*id) + varint_len(*v)
            }
            Frame::ConnectionClose { code, reason } => {
                1 + varint_len(*code) + varint_len(reason.len() as u64) + reason.len()
            }
            Frame::Ack(_) => {
                let mut b = Vec::new();
                self.append(&mut b);
                b.len()
            }
        }
    }

    /// Whether receiving this frame obliges the receiver to acknowledge.
    pub fn ack_eliciting(&self) -> bool {
        !matches!(self, Frame::Padding(_) | Frame::Ack(_) | Frame::ConnectionClose { .. })
    }
}

struct Parser<'a> {
    b: &'a [u8],
    err: bool,
}

impl<'a> Parser<'a> {
    fn varint(&mut self) -> u64 {
        if self.err {
            return 0;
        }
        match get_varint(self.b) {
            Some((v, n)) => {
                self.b = &self.b[n..];
                v
            }
            None => {
                self.err = true;
                0
            }
        }
    }

    fn bytes(&mut self, n: u64) -> &'a [u8] {
        if self.err {
            return &[];
        }
        if n > self.b.len() as u64 {
            self.err = true;
            return &[];
        }
        let (x, rest) = self.b.split_at(n as usize);
        self.b = rest;
        x
    }

    fn frame(&mut self) -> Result<Frame, WireError> {
        if self.b[0] == 0 {
            let n = self.b.iter().take_while(|&&x| x == 0).count();
            self.b = &self.b[n..];
            return Ok(Frame::Padding(n));
        }
        let t = self.varint();
        let f = match t {
            T_PING => Frame::Ping,
            T_ACK => Frame::Ack(self.ack()?),
            T_RESET_STREAM => Frame::ResetStream { id: self.varint(), code: self.varint(), final_size: self.varint() },
            T_STOP_SENDING => Frame::StopSending { id: self.varint(), code: self.varint() },
            T_HANDSHAKE => {
                let n = self.varint();
                Frame::Handshake(self.bytes(n).to_vec())
            }
            0x08..=0x0b => {
                let id = self.varint();
                let offset = self.varint();
                let data = if t & 0x02 != 0 {
                    let n = self.varint();
                    self.bytes(n)
                } else {
                    let n = self.b.len() as u64;
                    self.bytes(n)
                };
                if !self.err && offset.checked_add(data.len() as u64).map_or(true, |end| end > MAX_VARINT) {
                    self.err = true;
                }
                Frame::Stream { id, offset, data: data.to_vec(), fin: t & 0x01 != 0 }
            }
            T_MAX_DATA => Frame::MaxData(self.varint()),
            T_MAX_STREAM_DATA => Frame::MaxStreamData { id: self.varint(), max: self.varint() },
            T_MAX_STREAMS => Frame::MaxStreams(self.varint()),
            T_DATA_BLOCKED => Frame::DataBlocked(self.varint()),
            T_STREAM_DATA_BLOCKED => Frame::StreamDataBlocked { id: self.varint(), limit: self.varint() },
            T_CONNECTION_CLOSE => {
                let code = self.varint();
                let n = self.varint();
                Frame::ConnectionClose { code, reason: self.bytes(n).to_vec() }
            }
            T_HANDSHAKE_DONE => Frame::HandshakeDone,
            T_DATAGRAM => {
                let n = self.varint();
                Frame::Datagram(self.bytes(n).to_vec())
            }
            _ => {
                if self.err {
                    return Err(WireError::Frame);
                }
                return Err(WireError::FrameType(t));
            }
        };
        if self.err {
            return Err(WireError::Frame);
        }
        Ok(f)
    }

    fn ack(&mut self) -> Result<Ack, WireError> {
        let largest = self.varint();
        let delay = self.varint();
        let extra = self.varint();
        let first = self.varint();
        if self.err || first > largest || extra >= MAX_ACK_RANGES as u64 {
            return Err(WireError::Frame);
        }
        let mut ranges = Vec::with_capacity(extra as usize + 1);
        ranges.push((largest - first, largest));
        let mut prev = largest - first;
        for _ in 0..extra {
            let gap = self.varint();
            let n = self.varint();
            // gap <= MAX_VARINT, so gap + 2 cannot overflow.
            if self.err || prev < gap + 2 || prev - gap - 2 < n {
                return Err(WireError::Frame);
            }
            let hi = prev - gap - 2;
            ranges.push((hi - n, hi));
            prev = hi - n;
        }
        Ok(Ack { delay, ranges })
    }
}

/// Decodes every frame in a payload. Runs of padding collapse into one
/// Padding frame. Any malformed frame fails the whole payload: a packet is
/// processed entirely or not at all.
pub fn parse_frames(payload: &[u8]) -> Result<Vec<Frame>, WireError> {
    let mut out = Vec::new();
    let mut p = Parser { b: payload, err: false };
    while !p.b.is_empty() {
        out.push(p.frame()?);
    }
    Ok(out)
}
