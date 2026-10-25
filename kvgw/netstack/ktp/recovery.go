package ktp

import (
	"time"

	"github.com/Sebby1770/kvstore/kvgw/netstack/wire"
)

// Loss recovery after RFC 9002. Packet numbers are never reused: a lost
// packet is never "resent"; the frames it carried that still matter are put
// back in the send queues and travel in new packets.

const (
	packetThreshold = 3                 // reordering tolerated before declaring loss
	timeThresholdN  = 9                 // time threshold = 9/8 × max(srtt, latest_rtt)
	timeThresholdD  = 8                 //
	granularity     = time.Millisecond  // timer granularity
	maxPTOBackoff   = 16                // cap on 2^pto_count
	persistentMult  = 3                 // persistent congestion = 3 × PTO period
	maxRTTSample    = 60 * time.Second  // ignore absurd samples (clock jumps)
	ackRangesKept   = wire.MaxAckRanges // received-PN ranges tracked for ACKs
	initialRTTVarFr = 2                 // rttvar starts at initial_rtt / 2
)

// rttStats is the RFC 9002 §5 estimator.
type rttStats struct {
	latest, smoothed, variance, minRTT time.Duration
	hasSample                          bool
	firstSample                        time.Time
}

func (r *rttStats) smoothedOr(d time.Duration) time.Duration {
	if !r.hasSample {
		return d
	}
	return r.smoothed
}

// update folds in a sample measured from a newly acknowledged largest packet;
// ackDelay is the peer's reported hold time, capped at maxAckDelay once the
// handshake is confirmed.
func (r *rttStats) update(now time.Time, sample, ackDelay time.Duration) {
	if sample <= 0 || sample > maxRTTSample {
		return
	}
	r.latest = sample
	if !r.hasSample {
		r.hasSample = true
		r.firstSample = now
		r.minRTT = sample
		r.smoothed = sample
		r.variance = sample / 2
		return
	}
	r.minRTT = min(r.minRTT, sample)
	adjusted := sample
	if sample >= r.minRTT+ackDelay {
		adjusted = sample - ackDelay
	}
	diff := r.smoothed - adjusted
	if diff < 0 {
		diff = -diff
	}
	r.variance = (3*r.variance + diff) / 4
	r.smoothed = (7*r.smoothed + adjusted) / 8
}

func (r *rttStats) pto(initial, maxAckDelay time.Duration) time.Duration {
	srtt, rv := initial, initial/initialRTTVarFr
	if r.hasSample {
		srtt, rv = r.smoothed, r.variance
	}
	return srtt + max(4*rv, granularity) + maxAckDelay
}

// sentFrame records what a packet carried, so that on loss we know what to
// queue again. Stream data is recorded by range (the bytes stay in the
// stream's send buffer); credit updates by kind (a lost MAX_DATA is replaced
// by the current value, not the stale one); other control frames whole.
type sentFrame struct {
	kind           frameKind
	streamID       uint64
	offset, length uint64
	fin            bool
	ctrl           wire.Frame
}

type frameKind uint8

const (
	fkStream frameKind = iota + 1
	fkCtrl
	fkMaxData
	fkMaxStreamData
	fkMaxStreams
)

type sentPacket struct {
	pn           uint64
	sentAt       time.Time
	size         int
	ackEliciting bool
	frames       []sentFrame
	acked, lost  bool
}

// recovery tracks ack-eliciting packets in flight.
type recovery struct {
	sent          []*sentPacket // pn order; acked/lost entries pruned from the front
	bytesInFlight int
	largestAcked  uint64
	hasAcked      bool
	lossTime      time.Time
	ptoCount      int
	lastAckElicit time.Time
	rtt           rttStats
	cc            CongestionController

	// stats
	lost, ptos, congestionEvents uint64
}

func (r *recovery) onSent(p *sentPacket) {
	if !p.ackEliciting {
		return
	}
	r.sent = append(r.sent, p)
	r.bytesInFlight += p.size
	r.lastAckElicit = p.sentAt
}

func (r *recovery) find(pn uint64) *sentPacket {
	lo, hi := 0, len(r.sent)
	for lo < hi {
		m := (lo + hi) / 2
		if r.sent[m].pn < pn {
			lo = m + 1
		} else {
			hi = m
		}
	}
	if lo < len(r.sent) && r.sent[lo].pn == pn {
		return r.sent[lo]
	}
	return nil
}

func (r *recovery) prune() {
	i := 0
	for i < len(r.sent) && (r.sent[i].acked || r.sent[i].lost) {
		i++
	}
	if i > 0 {
		r.sent = append(r.sent[:0:0], r.sent[i:]...)
	}
}

// onAck processes an ACK frame. It returns the newly acknowledged and the
// newly lost packets; the caller applies their frames.
func (r *recovery) onAck(now time.Time, a wire.Ack, ackDelay, maxAckDelay time.Duration, cwndBefore int) (acked, lost []*sentPacket) {
	largest := a.Largest()
	var largestNew *sentPacket
	for _, p := range r.sent {
		if p.acked || p.lost || !a.Contains(p.pn) {
			continue
		}
		p.acked = true
		r.bytesInFlight -= p.size
		acked = append(acked, p)
		if p.pn == largest {
			largestNew = p
		}
	}
	if !r.hasAcked || largest > r.largestAcked {
		r.largestAcked, r.hasAcked = largest, true
	}
	if len(acked) == 0 {
		return nil, nil
	}
	if largestNew != nil {
		r.rtt.update(now, now.Sub(largestNew.sentAt), min(ackDelay, maxAckDelay))
	}
	// The window may only grow if the sender was actually using it.
	limited := r.bytesInFlight+sumSize(acked) >= cwndBefore/2
	for _, p := range acked {
		r.cc.OnAck(now, p.sentAt, p.size, &r.rtt, limited)
	}
	r.ptoCount = 0
	lost = r.detectLoss(now, maxAckDelay)
	r.prune()
	return acked, lost
}

func sumSize(ps []*sentPacket) int {
	n := 0
	for _, p := range ps {
		n += p.size
	}
	return n
}

func (r *recovery) lossDelay() time.Duration {
	d := max(r.rtt.latest, r.rtt.smoothedOr(0))
	if !r.rtt.hasSample {
		d = 0
	}
	return max(d*timeThresholdN/timeThresholdD, granularity)
}

// detectLoss declares packets lost by the packet and time thresholds and arms
// the loss timer for the earliest packet that could still be declared lost.
func (r *recovery) detectLoss(now time.Time, maxAckDelay time.Duration) []*sentPacket {
	r.lossTime = time.Time{}
	if !r.hasAcked {
		return nil
	}
	delay := r.lossDelay()
	var lost []*sentPacket
	for _, p := range r.sent {
		if p.acked || p.lost || p.pn > r.largestAcked {
			continue
		}
		if r.largestAcked-p.pn >= packetThreshold || !p.sentAt.After(now.Add(-delay)) {
			p.lost = true
			r.bytesInFlight -= p.size
			lost = append(lost, p)
			continue
		}
		t := p.sentAt.Add(delay)
		if r.lossTime.IsZero() || t.Before(r.lossTime) {
			r.lossTime = t
		}
	}
	if len(lost) > 0 {
		r.lost += uint64(len(lost))
		r.cc.OnLoss(now, lost[len(lost)-1].sentAt)
		r.congestionEvents++
		if r.persistent(lost, maxAckDelay) {
			r.cc.OnPersistentCongestion()
		}
	}
	return lost
}

// persistent reports RFC 9002 §7.6 persistent congestion: the lost packets
// include two, sent more than 3 PTOs apart, with nothing in between
// acknowledged -- the path was black for a long time, so the window is reset.
func (r *recovery) persistent(lost []*sentPacket, maxAckDelay time.Duration) bool {
	if !r.rtt.hasSample || len(lost) < 2 {
		return false
	}
	period := persistentMult * (r.rtt.smoothed + max(4*r.rtt.variance, granularity) + maxAckDelay)
	isLost := make(map[uint64]bool, len(lost))
	for _, p := range lost {
		isLost[p.pn] = true
	}
	// Walk the tracked packets in order looking for a run of lost packets
	// (sent after the first RTT sample) whose span exceeds the period.
	var runStart *sentPacket
	for _, p := range r.sent {
		if !isLost[p.pn] || p.sentAt.Before(r.rtt.firstSample) {
			runStart = nil
			continue
		}
		if runStart == nil {
			runStart = p
		}
		if p.sentAt.Sub(runStart.sentAt) > period {
			return true
		}
	}
	return false
}

// timer is when the loss or PTO timer fires.
func (r *recovery) timer(initialRTT, maxAckDelay time.Duration) (time.Time, bool) {
	if !r.lossTime.IsZero() {
		return r.lossTime, true
	}
	if r.bytesInFlight <= 0 {
		return time.Time{}, false
	}
	backoff := time.Duration(1) << min(r.ptoCount, 4)
	return r.lastAckElicit.Add(r.rtt.pto(initialRTT, maxAckDelay) * backoff), true
}

// onTimeout handles the loss/PTO timer. It returns lost packets (loss timer)
// or reports that probes are needed (PTO).
func (r *recovery) onTimeout(now time.Time, maxAckDelay time.Duration) (lost []*sentPacket, probe bool) {
	if !r.lossTime.IsZero() && !r.lossTime.After(now) {
		lost = r.detectLoss(now, maxAckDelay)
		r.prune()
		return lost, false
	}
	r.ptoCount++
	r.ptos++
	return nil, true
}

// oldestUnacked returns the oldest packet still awaiting acknowledgement,
// whose frames a PTO probe re-sends when there is no new data.
func (r *recovery) oldestUnacked() *sentPacket {
	for _, p := range r.sent {
		if !p.acked && !p.lost {
			return p
		}
	}
	return nil
}

// ---- received packet numbers ----

// ackTracker remembers which packet numbers arrived, to build ACK frames and
// to drop duplicates. It keeps the newest ackRangesKept ranges; anything
// below the oldest kept range is treated as already seen (a sender whose
// packet really was lost will retransmit its frames in a new one).
type ackTracker struct {
	ranges       rangeSet // half-open
	floor        uint64   // packet numbers below this count as seen
	largestAt    time.Time
	pending      int       // ack-eliciting packets since our last ACK
	unackedSince time.Time // first unacknowledged ack-eliciting arrival
	ackNow       bool      // out-of-order arrival: acknowledge immediately
	anyNew       bool      // something arrived since our last ACK
	largest      uint64
	hasLargest   bool
}

func (t *ackTracker) seen(pn uint64) bool {
	return pn < t.floor || t.ranges.contains(pn)
}

func (t *ackTracker) record(now time.Time, pn uint64, ackEliciting bool) {
	if t.hasLargest && (pn < t.largest || pn > t.largest+1) {
		t.ackNow = true // reordering or a gap: tell the sender quickly
	}
	if !t.hasLargest || pn > t.largest {
		t.largest, t.hasLargest, t.largestAt = pn, true, now
	}
	t.ranges.add(pn, pn+1)
	for len(t.ranges) > ackRangesKept {
		t.floor = t.ranges[0].hi
		t.ranges = t.ranges[1:]
	}
	t.anyNew = true
	if ackEliciting {
		if t.pending == 0 {
			t.unackedSince = now
		}
		t.pending++
	}
}

// due reports whether an ACK must go out now (rather than ride along).
func (t *ackTracker) due(now time.Time, maxAckDelay time.Duration) bool {
	if t.pending == 0 {
		return false
	}
	return t.pending >= 2 || t.ackNow || !now.Before(t.unackedSince.Add(maxAckDelay))
}

func (t *ackTracker) deadline(maxAckDelay time.Duration) (time.Time, bool) {
	if t.pending == 0 {
		return time.Time{}, false
	}
	return t.unackedSince.Add(maxAckDelay), true
}

func (t *ackTracker) frame(now time.Time) wire.Ack {
	a := wire.Ack{Delay: uint64(now.Sub(t.largestAt) / time.Microsecond)}
	for i := len(t.ranges) - 1; i >= 0; i-- {
		a.Ranges = append(a.Ranges, wire.AckRange{Smallest: t.ranges[i].lo, Largest: t.ranges[i].hi - 1})
	}
	return a
}

func (t *ackTracker) sent() {
	t.pending, t.ackNow, t.anyNew = 0, false, false
}
