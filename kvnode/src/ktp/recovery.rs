//! Loss recovery and congestion control after RFC 9002, as in
//! kvgw/netstack/ktp/recovery.go. Packet numbers are never reused: a lost
//! packet is never resent; the frames it carried that still matter go back
//! into the send queues and travel in new packets.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use super::ranges::RangeSet;
use super::wire::{Ack, Frame, MAX_ACK_RANGES};

const PACKET_THRESHOLD: u64 = 3; // reordering tolerated before declaring loss
const GRANULARITY: Duration = Duration::from_millis(1);
const PERSISTENT_MULT: u32 = 3;
const MAX_RTT_SAMPLE: Duration = Duration::from_secs(60);

/// The RFC 9002 §5 RTT estimator.
#[derive(Clone, Debug, Default)]
pub struct Rtt {
    pub latest: Duration,
    pub smoothed: Duration,
    pub variance: Duration,
    pub min: Duration,
    pub has_sample: bool,
    first_sample: Option<Instant>,
}

impl Rtt {
    fn update(&mut self, now: Instant, sample: Duration, ack_delay: Duration) {
        if sample.is_zero() || sample > MAX_RTT_SAMPLE {
            return;
        }
        self.latest = sample;
        if !self.has_sample {
            self.has_sample = true;
            self.first_sample = Some(now);
            self.min = sample;
            self.smoothed = sample;
            self.variance = sample / 2;
            return;
        }
        self.min = self.min.min(sample);
        let adjusted = if sample >= self.min + ack_delay { sample - ack_delay } else { sample };
        let diff = if self.smoothed > adjusted { self.smoothed - adjusted } else { adjusted - self.smoothed };
        self.variance = (self.variance * 3 + diff) / 4;
        self.smoothed = (self.smoothed * 7 + adjusted) / 8;
    }

    pub fn pto(&self, initial: Duration, max_ack_delay: Duration) -> Duration {
        let (srtt, rv) = if self.has_sample { (self.smoothed, self.variance) } else { (initial, initial / 2) };
        srtt + (rv * 4).max(GRANULARITY) + max_ack_delay
    }
}

fn initial_window(mss: usize) -> usize {
    (10 * mss).min(14720.max(2 * mss))
}

fn minimum_window(mss: usize) -> usize {
    2 * mss
}

/// NewReno (RFC 9002 §7): slow start, then one MSS per window acknowledged;
/// halve on loss, at most once per recovery period.
#[derive(Debug)]
pub struct NewReno {
    mss: usize,
    cwnd: usize,
    ssthresh: usize,
    recovery_start: Option<Instant>,
    acked_in_ca: usize,
}

impl NewReno {
    pub fn new(mss: usize) -> NewReno {
        NewReno { mss, cwnd: initial_window(mss), ssthresh: usize::MAX, recovery_start: None, acked_in_ca: 0 }
    }

    pub fn window(&self) -> usize {
        self.cwnd
    }

    pub fn ssthresh(&self) -> usize {
        self.ssthresh
    }

    fn in_recovery(&self, sent_at: Instant) -> bool {
        self.recovery_start.is_some_and(|r| sent_at <= r)
    }

    fn on_ack(&mut self, sent_at: Instant, bytes: usize, limited: bool) {
        if self.in_recovery(sent_at) || !limited {
            return;
        }
        if self.cwnd < self.ssthresh {
            self.cwnd += bytes;
            return;
        }
        self.acked_in_ca += bytes;
        if self.acked_in_ca >= self.cwnd {
            self.acked_in_ca -= self.cwnd;
            self.cwnd += self.mss;
        }
    }

    fn on_loss(&mut self, now: Instant, sent_at: Instant) {
        if self.in_recovery(sent_at) {
            return;
        }
        self.recovery_start = Some(now);
        self.ssthresh = (self.cwnd / 2).max(minimum_window(self.mss));
        self.cwnd = self.ssthresh;
        self.acked_in_ca = 0;
    }

    fn on_persistent_congestion(&mut self) {
        self.cwnd = minimum_window(self.mss);
        self.recovery_start = None;
        self.acked_in_ca = 0;
    }
}

/// What a packet carried, so that on loss we know what to queue again.
/// Stream data is recorded by range (the bytes stay in the stream's send
/// buffer); credit updates by kind (a lost MAX_DATA is replaced by the
/// current value, not the stale one); other control frames whole.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SentFrame {
    Stream { id: u64, offset: u64, len: u64, fin: bool },
    ServerHello,
    Ctrl(Frame),
    MaxData,
    MaxStreamData(u64),
    MaxStreams,
    Ping,
}

#[derive(Debug)]
pub struct SentPacket {
    pub pn: u64,
    pub sent_at: Instant,
    pub size: usize,
    pub ack_eliciting: bool,
    pub frames: Vec<SentFrame>,
    acked: bool,
    lost: bool,
}

impl SentPacket {
    pub fn new(pn: u64, sent_at: Instant, size: usize, ack_eliciting: bool, frames: Vec<SentFrame>) -> SentPacket {
        SentPacket { pn, sent_at, size, ack_eliciting, frames, acked: false, lost: false }
    }
}

/// Tracks ack-eliciting packets in flight, the RTT, loss and PTO timers.
#[derive(Debug)]
pub struct Recovery {
    sent: VecDeque<SentPacket>, // pn order; acked/lost entries pruned from the front
    pub bytes_in_flight: usize,
    largest_acked: Option<u64>,
    loss_time: Option<Instant>,
    pto_count: u32,
    last_ack_elicit: Option<Instant>,
    pub rtt: Rtt,
    pub cc: NewReno,
    recovery_start: Option<Instant>,
    pub lost: u64,
    pub ptos: u64,
    pub congestion_events: u64,
}

/// Frames of packets settled by an ACK or a timer.
#[derive(Default)]
pub struct Outcome {
    pub acked: Vec<Vec<SentFrame>>,
    pub lost: Vec<Vec<SentFrame>>,
}

impl Recovery {
    pub fn new(mss: usize) -> Recovery {
        Recovery {
            sent: VecDeque::new(),
            bytes_in_flight: 0,
            largest_acked: None,
            loss_time: None,
            pto_count: 0,
            last_ack_elicit: None,
            rtt: Rtt::default(),
            cc: NewReno::new(mss),
            recovery_start: None,
            lost: 0,
            ptos: 0,
            congestion_events: 0,
        }
    }

    pub fn on_sent(&mut self, p: SentPacket) {
        if !p.ack_eliciting {
            return;
        }
        self.bytes_in_flight += p.size;
        self.last_ack_elicit = Some(p.sent_at);
        self.sent.push_back(p);
    }

    fn prune(&mut self) {
        while self.sent.front().is_some_and(|p| p.acked || p.lost) {
            self.sent.pop_front();
        }
    }

    /// Processes an ACK frame: the frames of newly acknowledged and newly
    /// lost packets go back to the caller.
    pub fn on_ack(&mut self, now: Instant, a: &Ack, ack_delay: Duration, max_ack_delay: Duration) -> Outcome {
        let largest = a.largest();
        let cwnd_before = self.cc.window();
        let mut out = Outcome::default();
        let mut largest_new_sent: Option<Instant> = None;
        let mut acked_bytes = 0;
        let mut acked_sent_at = Vec::new();
        for p in self.sent.iter_mut() {
            if p.acked || p.lost || !a.contains(p.pn) {
                continue;
            }
            p.acked = true;
            self.bytes_in_flight -= p.size;
            acked_bytes += p.size;
            acked_sent_at.push((p.sent_at, p.size));
            if p.pn == largest {
                largest_new_sent = Some(p.sent_at);
            }
            out.acked.push(std::mem::take(&mut p.frames));
        }
        if self.largest_acked.map_or(true, |l| largest > l) {
            self.largest_acked = Some(largest);
        }
        if out.acked.is_empty() {
            return out;
        }
        if let Some(sent_at) = largest_new_sent {
            self.rtt.update(now, now.saturating_duration_since(sent_at), ack_delay.min(max_ack_delay));
        }
        // The window may only grow if the sender was actually using it.
        let limited = self.bytes_in_flight + acked_bytes >= cwnd_before / 2;
        for (sent_at, size) in acked_sent_at {
            self.cc.on_ack(sent_at, size, limited);
        }
        self.pto_count = 0;
        out.lost = self.detect_loss(now, max_ack_delay);
        self.prune();
        out
    }

    fn loss_delay(&self) -> Duration {
        let d = if self.rtt.has_sample { self.rtt.latest.max(self.rtt.smoothed) } else { Duration::ZERO };
        (d * 9 / 8).max(GRANULARITY)
    }

    /// Declares packets lost by the packet and time thresholds and arms the
    /// loss timer for the earliest packet that could still be lost.
    fn detect_loss(&mut self, now: Instant, max_ack_delay: Duration) -> Vec<Vec<SentFrame>> {
        self.loss_time = None;
        let Some(largest_acked) = self.largest_acked else { return Vec::new() };
        let delay = self.loss_delay();
        let mut lost = Vec::new();
        let mut lost_pns = Vec::new();
        let mut last_lost_sent: Option<Instant> = None;
        for p in self.sent.iter_mut() {
            if p.acked || p.lost || p.pn > largest_acked {
                continue;
            }
            if largest_acked - p.pn >= PACKET_THRESHOLD || p.sent_at + delay <= now {
                p.lost = true;
                self.bytes_in_flight -= p.size;
                lost_pns.push(p.pn);
                last_lost_sent = Some(p.sent_at);
                lost.push(std::mem::take(&mut p.frames));
                continue;
            }
            let t = p.sent_at + delay;
            if self.loss_time.map_or(true, |lt| t < lt) {
                self.loss_time = Some(t);
            }
        }
        if let Some(last) = last_lost_sent {
            self.lost += lost.len() as u64;
            self.cc.on_loss(now, last);
            if self.recovery_start.map_or(true, |r| last > r) {
                self.congestion_events += 1;
                self.recovery_start = Some(now);
            }
            if self.persistent(&lost_pns, max_ack_delay) {
                self.cc.on_persistent_congestion();
            }
        }
        lost
    }

    /// RFC 9002 §7.6: the lost packets include two, sent more than three
    /// PTO periods apart, with nothing in between acknowledged.
    fn persistent(&self, lost: &[u64], max_ack_delay: Duration) -> bool {
        let Some(first_sample) = self.rtt.first_sample else { return false };
        if lost.len() < 2 {
            return false;
        }
        let period =
            (self.rtt.smoothed + (self.rtt.variance * 4).max(GRANULARITY) + max_ack_delay) * PERSISTENT_MULT;
        let mut run_start: Option<Instant> = None;
        for p in &self.sent {
            if lost.binary_search(&p.pn).is_err() || p.sent_at < first_sample {
                run_start = None;
                continue;
            }
            let start = *run_start.get_or_insert(p.sent_at);
            if p.sent_at.saturating_duration_since(start) > period {
                return true;
            }
        }
        false
    }

    /// When the loss or PTO timer fires.
    pub fn timer(&self, initial_rtt: Duration, max_ack_delay: Duration) -> Option<Instant> {
        if self.loss_time.is_some() {
            return self.loss_time;
        }
        if self.bytes_in_flight == 0 {
            return None;
        }
        let backoff = 1u32 << self.pto_count.min(4);
        Some(self.last_ack_elicit? + self.rtt.pto(initial_rtt, max_ack_delay) * backoff)
    }

    /// Handles the loss/PTO timer: lost packets (loss timer) or a request to
    /// probe (PTO).
    pub fn on_timeout(&mut self, now: Instant, max_ack_delay: Duration) -> (Vec<Vec<SentFrame>>, bool) {
        if self.loss_time.is_some_and(|t| t <= now) {
            let lost = self.detect_loss(now, max_ack_delay);
            self.prune();
            return (lost, false);
        }
        self.pto_count += 1;
        self.ptos += 1;
        (Vec::new(), true)
    }

    /// The frames of the oldest packet still awaiting acknowledgement, which
    /// a PTO probe re-sends (the packet stays tracked; whichever copy is
    /// acknowledged first wins).
    pub fn oldest_unacked_frames(&self) -> Option<Vec<SentFrame>> {
        self.sent.iter().find(|p| !p.acked && !p.lost).map(|p| p.frames.clone())
    }
}

/// Remembers which packet numbers arrived, to build ACK frames and drop
/// duplicates. It keeps the newest MAX_ACK_RANGES ranges; anything below the
/// oldest kept range counts as already seen.
#[derive(Debug, Default)]
pub struct AckTracker {
    ranges: RangeSet,
    floor: u64,
    largest_at: Option<Instant>,
    pending: u32,
    unacked_since: Option<Instant>,
    ack_now: bool,
    pub any_new: bool,
    largest: Option<u64>,
}

impl AckTracker {
    pub fn seen(&self, pn: u64) -> bool {
        pn < self.floor || self.ranges.contains(pn)
    }

    pub fn record(&mut self, now: Instant, pn: u64, ack_eliciting: bool) {
        if let Some(l) = self.largest {
            if pn < l || pn > l + 1 {
                self.ack_now = true; // reordering or a gap: tell the sender quickly
            }
        }
        if self.largest.map_or(true, |l| pn > l) {
            self.largest = Some(pn);
            self.largest_at = Some(now);
        }
        self.ranges.add(pn, pn + 1);
        while self.ranges.len() > MAX_ACK_RANGES {
            if let Some((_, hi)) = self.ranges.pop_first() {
                self.floor = hi;
            }
        }
        self.any_new = true;
        if ack_eliciting {
            if self.pending == 0 {
                self.unacked_since = Some(now);
            }
            self.pending += 1;
        }
    }

    /// Whether an ACK must go out now rather than ride along later.
    pub fn due(&self, now: Instant, max_ack_delay: Duration) -> bool {
        if self.pending == 0 {
            return false;
        }
        self.pending >= 2 || self.ack_now || self.unacked_since.is_some_and(|t| now >= t + max_ack_delay)
    }

    pub fn deadline(&self, max_ack_delay: Duration) -> Option<Instant> {
        if self.pending == 0 {
            return None;
        }
        self.unacked_since.map(|t| t + max_ack_delay)
    }

    pub fn frame(&self, now: Instant) -> Ack {
        let delay = self.largest_at.map_or(0, |t| now.saturating_duration_since(t).as_micros() as u64);
        Ack { delay, ranges: self.ranges.iter().rev().map(|&(lo, hi)| (lo, hi - 1)).collect() }
    }

    pub fn sent(&mut self) {
        self.pending = 0;
        self.ack_now = false;
        self.any_new = false;
    }
}
