//! Both halves of one bidirectional stream, as in kvgw/netstack/ktp/stream.go.

use std::collections::BTreeMap;

use super::conn::{CloseError, CODE_FINAL_SIZE, CODE_FLOW_CONTROL};
use super::ranges::RangeSet;

/// Result of reading a stream.
#[derive(Debug, PartialEq, Eq)]
pub enum ReadResult {
    Data(usize),
    /// Nothing ready yet.
    WouldBlock,
    /// The peer's FIN: everything has been read.
    Eof,
    /// The peer reset its send half with this application code.
    Reset(u64),
}

#[derive(Debug)]
pub struct Stream {
    pub id: u64,

    // ---- send half ----
    send_buf: Vec<u8>, // bytes from send_base (at index buf_head): unacknowledged or unsent
    buf_head: usize,
    pub send_base: u64, // every offset below is acknowledged
    pub send_next: u64, // first never-transmitted offset
    pub send_max: u64,  // peer's MAX_STREAM_DATA
    acked: RangeSet,    // acknowledged offsets >= send_base
    pub retx: RangeSet, // lost ranges awaiting retransmission
    pub fin_queued: bool,
    pub fin_sent: bool,
    pub fin_acked: bool,
    pub fin_lost: bool,
    pub reset_sent: bool,
    pub stop_err: Option<u64>,

    // ---- receive half ----
    ready: Vec<u8>,                // contiguous bytes from read_off
    pub read_off: u64,             // the application has consumed everything below
    chunks: BTreeMap<u64, Vec<u8>>, // out-of-order data beyond the ready bytes, non-overlapping
    pub recv_high: u64,            // highest offset+1 seen (connection flow control)
    pub final_size: Option<u64>,   // known after FIN or RESET_STREAM
    pub recv_max: u64,             // our advertised MAX_STREAM_DATA
    window: u64,                   // receive window kept ahead of read_off
    pub reset_err: Option<u64>,
    reset_delivered: bool,
    pub read_closed: bool,
    pub need_max: bool,
    released: u64, // bytes whose connection-level credit has been returned
}

impl Stream {
    pub fn new(id: u64, send_max: u64, recv_window: u64) -> Stream {
        Stream {
            id,
            send_buf: Vec::new(),
            buf_head: 0,
            send_base: 0,
            send_next: 0,
            send_max,
            acked: RangeSet::new(),
            retx: RangeSet::new(),
            fin_queued: false,
            fin_sent: false,
            fin_acked: false,
            fin_lost: false,
            reset_sent: false,
            stop_err: None,
            ready: Vec::new(),
            read_off: 0,
            chunks: BTreeMap::new(),
            recv_high: 0,
            final_size: None,
            recv_max: recv_window,
            window: recv_window,
            reset_err: None,
            reset_delivered: false,
            read_closed: false,
            need_max: false,
            released: 0,
        }
    }

    // ---- send half ----

    /// Unacknowledged or unsent bytes held.
    pub fn buffered(&self) -> usize {
        self.send_buf.len() - self.buf_head
    }

    pub fn send_end(&self) -> u64 {
        self.send_base + self.buffered() as u64
    }

    pub fn push(&mut self, data: &[u8]) {
        self.send_buf.extend_from_slice(data);
    }

    /// Stream data [off, off+n) from the send buffer.
    pub fn bytes_at(&self, off: u64, n: u64) -> &[u8] {
        let start = self.buf_head + (off - self.send_base) as usize;
        &self.send_buf[start..start + n as usize]
    }

    /// Abandons the send half's data (reset).
    pub fn drop_send_data(&mut self) {
        self.send_buf = Vec::new();
        self.buf_head = 0;
        self.retx.clear();
        self.acked.clear();
    }

    /// Records acknowledgement of [off, off+n) (and FIN) and frees the
    /// acknowledged prefix of the send buffer.
    pub fn on_acked(&mut self, off: u64, n: u64, fin: bool) {
        if fin {
            self.fin_acked = true;
            self.fin_lost = false;
        }
        if self.reset_sent || n == 0 || off + n <= self.send_base {
            return; // a reset discarded the buffer; late acks are moot
        }
        let lo = off.max(self.send_base);
        self.acked.add(lo, off + n);
        self.retx.remove(lo, off + n);
        if let Some((a, b)) = self.acked.first() {
            if a == self.send_base {
                let adv = (b - self.send_base).min(self.buffered() as u64);
                self.buf_head += adv as usize;
                self.send_base += adv;
                self.acked.pop_first();
                self.retx.trim_below(self.send_base);
                if self.buf_head == self.send_buf.len() {
                    self.send_buf = Vec::new();
                    self.buf_head = 0;
                } else if self.buf_head >= 64 * 1024 && self.buf_head * 2 >= self.send_buf.len() {
                    self.send_buf.drain(..self.buf_head);
                    self.buf_head = 0;
                }
            }
        }
    }

    /// Queues the still-unacknowledged parts of [off, off+n) for
    /// retransmission.
    pub fn on_lost(&mut self, off: u64, n: u64, fin: bool) {
        if self.reset_sent {
            return;
        }
        if fin && !self.fin_acked {
            self.fin_lost = true;
        }
        if n == 0 || off + n <= self.send_base {
            return;
        }
        for (lo, hi) in self.acked.gaps(off.max(self.send_base), off + n) {
            self.retx.add(lo, hi);
        }
    }

    /// Nothing more will ever be sent on this half.
    pub fn send_done(&self) -> bool {
        self.reset_sent || self.stop_err.is_some() || (self.fin_queued && self.fin_acked && self.buffered() == 0)
    }

    /// Whether the stream has something to transmit given the connection's
    /// remaining credit for new data.
    pub fn has_send_work(&self, conn_credit: u64) -> bool {
        if self.reset_sent || self.stop_err.is_some() {
            return false;
        }
        if !self.retx.is_empty() || self.fin_lost {
            return true;
        }
        if self.send_next < self.send_end() && self.send_next < self.send_max && conn_credit > 0 {
            return true;
        }
        self.fin_queued && !self.fin_sent && self.send_next == self.send_end()
    }

    // ---- receive half ----

    /// Everything the peer will send has been consumed (or abandoned).
    pub fn recv_done(&self) -> bool {
        if self.read_closed {
            return true;
        }
        if self.reset_err.is_some() {
            // Keep the stream until the application has seen the reset;
            // otherwise its next read finds no stream and mistakes the
            // abort for a clean end.
            return self.reset_delivered;
        }
        self.final_size == Some(self.read_off) && self.ready.is_empty()
    }

    /// Returns the connection-level credit freed by releasing every byte
    /// below upto.
    pub fn release_to(&mut self, upto: u64) -> u64 {
        let d = upto.saturating_sub(self.released);
        self.released = self.released.max(upto);
        d
    }

    /// Files a STREAM frame. Returns how much new connection-level
    /// flow-control credit it consumed (growth of recv_high).
    pub fn receive(&mut self, off: u64, data: &[u8], fin: bool) -> Result<u64, CloseError> {
        let end = off + data.len() as u64; // the parser bounds this by 2^62
        if end > self.recv_max {
            return Err(CloseError::local(CODE_FLOW_CONTROL, "flow control limit exceeded"));
        }
        if let Some(fs) = self.final_size {
            if end > fs || (fin && end != fs) {
                return Err(CloseError::local(CODE_FINAL_SIZE, "data beyond final size"));
            }
        }
        if fin {
            if end < self.recv_high {
                return Err(CloseError::local(CODE_FINAL_SIZE, "data beyond final size"));
            }
            self.final_size = Some(end);
        }
        let grew = end.saturating_sub(self.recv_high);
        self.recv_high = self.recv_high.max(end);
        if self.reset_err.is_some() || self.read_closed {
            return Ok(grew); // keep the flow-control accounting, drop the bytes
        }
        let have = self.read_off + self.ready.len() as u64;
        if end <= have {
            return Ok(grew); // duplicate
        }
        let (off, data) = if off < have { (have, &data[(have - off) as usize..]) } else { (off, data) };
        // Store only the parts no buffered chunk covers yet, so the chunks
        // never overlap and never hold more than the window.
        let mut pieces = Vec::new();
        let mut cur = off;
        for (&co, cd) in self.chunks.range(..end) {
            let ce = co + cd.len() as u64;
            if ce <= cur {
                continue;
            }
            if co > cur {
                pieces.push((cur, co.min(end)));
            }
            cur = cur.max(ce);
            if cur >= end {
                break;
            }
        }
        if cur < end {
            pieces.push((cur, end));
        }
        for (a, b) in pieces {
            self.chunks.insert(a, data[(a - off) as usize..(b - off) as usize].to_vec());
        }
        // Pull every chunk that now touches the contiguous end into ready.
        while let Some((&co, _)) = self.chunks.iter().next() {
            let have = self.read_off + self.ready.len() as u64;
            if co > have {
                break;
            }
            let c = self.chunks.remove(&co).expect("key exists");
            let ce = co + c.len() as u64;
            if ce > have {
                self.ready.extend_from_slice(&c[(have - co) as usize..]);
            }
        }
        Ok(grew)
    }

    /// Copies ready bytes into p.
    pub fn read(&mut self, p: &mut [u8]) -> ReadResult {
        if let Some(code) = self.reset_err {
            self.reset_delivered = true;
            return ReadResult::Reset(code);
        }
        if self.ready.is_empty() {
            if self.final_size == Some(self.read_off) {
                return ReadResult::Eof;
            }
            return ReadResult::WouldBlock;
        }
        let n = p.len().min(self.ready.len());
        p[..n].copy_from_slice(&self.ready[..n]);
        self.ready.drain(..n);
        self.read_off += n as u64;
        if self.final_size.is_none() && self.recv_max - self.read_off < self.window / 2 {
            self.recv_max = self.read_off + self.window;
            self.need_max = true;
        }
        ReadResult::Data(n)
    }

    /// Abandons the receive half: buffered data is dropped.
    pub fn close_read(&mut self) {
        self.read_closed = true;
        self.ready = Vec::new();
        self.chunks.clear();
    }

    /// Abandons buffered data after a peer reset.
    pub fn drop_recv_data(&mut self) {
        self.ready = Vec::new();
        self.chunks.clear();
    }

    pub fn readable(&self) -> bool {
        !self.ready.is_empty() || self.reset_err.is_some() || self.final_size == Some(self.read_off)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reassembles_out_of_order_and_overlapping_data() {
        let mut s = Stream::new(0, 1 << 20, 1 << 20);
        assert_eq!(s.receive(6, b"world", true).unwrap(), 11);
        assert_eq!(s.receive(3, b"lo wor", false).unwrap(), 0); // overlaps the buffered chunk
        assert!(!s.readable());
        assert_eq!(s.receive(0, b"hel", false).unwrap(), 0);
        let mut buf = [0u8; 32];
        assert_eq!(s.read(&mut buf), ReadResult::Data(11));
        assert_eq!(&buf[..11], b"hello world");
        assert_eq!(s.read(&mut buf), ReadResult::Eof);
        assert!(s.recv_done());
    }

    #[test]
    fn rejects_flow_control_and_final_size_violations() {
        let mut s = Stream::new(0, 0, 10);
        assert_eq!(s.receive(5, b"123456", false).unwrap_err().code, CODE_FLOW_CONTROL);
        s.receive(0, b"abc", true).unwrap();
        assert_eq!(s.receive(2, b"cd", false).unwrap_err().code, CODE_FINAL_SIZE);
        assert_eq!(s.receive(0, b"ab", true).unwrap_err().code, CODE_FINAL_SIZE);
    }

    #[test]
    fn acknowledgements_free_the_send_buffer_and_losses_requeue_gaps() {
        let mut s = Stream::new(0, 1 << 20, 1 << 20);
        s.push(&[7u8; 100]);
        s.send_next = 100;
        s.on_acked(50, 25, false); // out of order: nothing freed yet
        assert_eq!(s.buffered(), 100);
        s.on_lost(0, 100, false);
        assert_eq!(s.retx.iter().copied().collect::<Vec<_>>(), vec![(0, 50), (75, 100)]);
        s.on_acked(0, 50, false);
        assert_eq!((s.send_base, s.buffered()), (75, 25));
        assert_eq!(s.retx.iter().copied().collect::<Vec<_>>(), vec![(75, 100)]);
        assert_eq!(s.bytes_at(80, 3), &[7, 7, 7]);
    }
}
