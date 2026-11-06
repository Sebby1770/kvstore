package ktp

import (
	"math"
	"time"
)

// BBR (Cardwell et al., "BBR: Congestion-Based Congestion Control", 2016),
// version 1, adapted to KTP.
//
// Loss-based controllers read every lost packet as "the queue overflowed"
// and halve the window, so random loss -- a flaky Wi-Fi hop, a congested
// middlebox, simnet's Loss knob -- caps their throughput at about
// MSS/RTT × 1.22/√p no matter how fast the link is. BBR instead builds a
// model of the path from delivery-rate samples: BtlBw, the windowed maximum
// delivery rate (the bottleneck bandwidth), and RTprop, the windowed minimum
// RTT (the propagation delay). It paces at BtlBw and keeps about two BDPs
// (BtlBw × RTprop) in flight, and treats loss as no signal at all.
//
// States:
//
//	Startup   pacing and cwnd gain 2/ln2: doubles the sending rate each round
//	          until BtlBw stops growing by 25% for three rounds (pipe full).
//	Drain     pacing gain ln2/2 until inflight falls to one BDP, emptying the
//	          queue Startup built.
//	ProbeBW   cycles pacing gain 1.25, 0.75, 1, 1, 1, 1, 1, 1 one RTprop each:
//	          probe for more bandwidth, drain the probe's queue, cruise.
//	ProbeRTT  if RTprop has not been re-measured for 10s, drop to four
//	          packets in flight for 200ms and a round so queues drain and the
//	          true propagation delay shows.
const (
	bbrHighGain         = 2.885 // 2/ln(2)
	bbrDrainGain        = 1 / bbrHighGain
	bbrCwndGain         = 2.0
	bbrBtlBwRounds      = 10
	bbrProbeRTTInterval = 10 * time.Second
	bbrProbeRTTDuration = 200 * time.Millisecond
	bbrFullBwThresh     = 1.25
	bbrFullBwRounds     = 3
	// Startup also ends on a round with heavy loss (BBRv2's fix for v1
	// overflowing shallow buffers): at least bbrStartupLossPkts lost and
	// more than bbrStartupLossRate of the round's packets. The rate sits
	// well above typical random loss so that does not end Startup early.
	bbrStartupLossPkts = 8
	bbrStartupLossRate = 0.04
)

var bbrPacingCycle = [8]float64{1.25, 0.75, 1, 1, 1, 1, 1, 1}

type bbrState uint8

const (
	bbrStartup bbrState = iota
	bbrDrain
	bbrProbeBW
	bbrProbeRTT
)

func (s bbrState) String() string { return [...]string{"startup", "drain", "probe_bw", "probe_rtt"}[s] }

type bwSample struct {
	round uint64
	rate  float64
}

type bbr struct {
	mss   int
	state bbrState
	cwnd  int

	bw          []bwSample // per-round maxima over the last bbrBtlBwRounds rounds
	btlBw       float64
	rtProp      time.Duration
	rtPropStamp time.Time

	round              uint64
	nextRoundDelivered uint64
	roundStart         bool

	fullBw      float64
	fullBwCount int
	filledPipe  bool

	pacingGain, cwndGain float64
	cycleIdx             int
	cycleStamp           time.Time

	probeRTTDone      time.Time
	probeRTTRoundDone bool
	priorCwnd         int

	roundLostPkts, roundLostBytes int
	roundDelivered                int
}

func newBBR(mss int) *bbr {
	return &bbr{mss: mss, cwnd: initialWindow(mss), rtProp: time.Duration(math.MaxInt64),
		pacingGain: bbrHighGain, cwndGain: bbrHighGain}
}

func (b *bbr) Name() string  { return "bbr" }
func (b *bbr) SSThresh() int { return math.MaxInt }

// Window is the congestion window (four packets during ProbeRTT).
func (b *bbr) Window() int {
	if b.state == bbrProbeRTT {
		return min(b.cwnd, 4*b.mss)
	}
	return b.cwnd
}

// State names the current BBR state (for stats and tests).
func (b *bbr) State() string { return b.state.String() }

// OnAck is a no-op: BBR moves its window on rate samples, not per packet.
func (b *bbr) OnAck(now, sentAt time.Time, bytes int, rtt *rttStats, limited bool) {}

// OnLoss is a no-op: random loss is not a congestion signal to BBR (heavy
// loss during Startup is handled per round in onPacketsLost).
func (b *bbr) OnLoss(now, sentAt time.Time) {}

func (b *bbr) onPacketsLost(now time.Time, packets, bytes int) {
	b.roundLostPkts += packets
	b.roundLostBytes += bytes
}

// OnPersistentCongestion: the path went black for several PTOs; start over
// from a small window but keep the model, which will be re-validated.
func (b *bbr) OnPersistentCongestion() {
	b.priorCwnd = max(b.priorCwnd, b.cwnd)
	b.cwnd = 4 * b.mss
}

func (b *bbr) pacingRate() float64 {
	if b.btlBw <= 0 {
		return 0 // no model yet: the connection's default pacing applies
	}
	return b.pacingGain * b.btlBw
}

func (b *bbr) bdp(gain float64) int {
	if b.btlBw <= 0 || b.rtProp == time.Duration(math.MaxInt64) {
		return initialWindow(b.mss)
	}
	return int(gain * b.btlBw * b.rtProp.Seconds())
}

func (b *bbr) updateBtlBw(rate float64) {
	keep := b.bw[:0]
	for _, s := range b.bw {
		if b.round-s.round < bbrBtlBwRounds {
			keep = append(keep, s)
		}
	}
	b.bw = keep
	if n := len(b.bw); n > 0 && b.bw[n-1].round == b.round {
		b.bw[n-1].rate = max(b.bw[n-1].rate, rate)
	} else {
		b.bw = append(b.bw, bwSample{round: b.round, rate: rate})
	}
	b.btlBw = 0
	for _, s := range b.bw {
		b.btlBw = max(b.btlBw, s.rate)
	}
}

func (b *bbr) onRateSample(now time.Time, rs rateSample) {
	// Round-trip counting: a round ends when a packet sent after the
	// previous round ended is acknowledged.
	b.roundStart = false
	b.roundDelivered += rs.acked
	if rs.priorDelivered >= b.nextRoundDelivered {
		b.nextRoundDelivered = rs.delivered
		b.round++
		b.roundStart = true
		if !b.filledPipe && b.roundLostPkts >= bbrStartupLossPkts &&
			float64(b.roundLostBytes) > bbrStartupLossRate*float64(b.roundDelivered+b.roundLostBytes) {
			b.filledPipe = true // the queue overflowed: the pipe is (over)full
		}
		b.roundLostPkts, b.roundLostBytes, b.roundDelivered = 0, 0, 0
	}
	// Model: application-limited samples may only raise the estimate.
	if rs.rate > 0 && (!rs.appLimited || rs.rate >= b.btlBw) {
		b.updateBtlBw(rs.rate)
	}
	rtPropExpired := !b.rtPropStamp.IsZero() && now.Sub(b.rtPropStamp) > bbrProbeRTTInterval
	if rs.rtt > 0 && (rs.rtt <= b.rtProp || rtPropExpired) {
		b.rtProp, b.rtPropStamp = rs.rtt, now
	}

	// Is the pipe full? (Startup stops when bandwidth plateaus.)
	if !b.filledPipe && b.roundStart && !rs.appLimited {
		if b.btlBw >= b.fullBw*bbrFullBwThresh {
			b.fullBw, b.fullBwCount = b.btlBw, 0
		} else if b.fullBwCount++; b.fullBwCount >= bbrFullBwRounds {
			b.filledPipe = true
		}
	}

	switch b.state {
	case bbrStartup:
		if b.filledPipe {
			b.state, b.pacingGain, b.cwndGain = bbrDrain, bbrDrainGain, bbrHighGain
		}
	case bbrDrain:
		if rs.inflight <= b.bdp(1) {
			b.enterProbeBW(now)
		}
	case bbrProbeBW:
		if now.Sub(b.cycleStamp) > b.rtProp {
			// The 1.25 probe phase lasts until inflight actually reached
			// 1.25 BDP; the 0.75 drain phase may end early once inflight is
			// back down to one BDP.
			gain := bbrPacingCycle[b.cycleIdx]
			advance := true
			if gain > 1 && rs.inflight < b.bdp(gain) {
				advance = now.Sub(b.cycleStamp) > 2*b.rtProp
			}
			if advance {
				b.cycleIdx = (b.cycleIdx + 1) % len(bbrPacingCycle)
				b.cycleStamp = now
				b.pacingGain = bbrPacingCycle[b.cycleIdx]
			}
		}
	}

	// ProbeRTT: refresh the propagation delay estimate.
	if b.state != bbrProbeRTT && rtPropExpired {
		b.state = bbrProbeRTT
		b.pacingGain = 1
		b.priorCwnd = max(b.priorCwnd, b.cwnd)
		b.probeRTTDone = time.Time{}
	}
	if b.state == bbrProbeRTT {
		if b.probeRTTDone.IsZero() && rs.inflight <= 4*b.mss {
			b.probeRTTDone = now.Add(bbrProbeRTTDuration)
			b.probeRTTRoundDone = false
			b.nextRoundDelivered = rs.delivered
		} else if !b.probeRTTDone.IsZero() {
			if b.roundStart {
				b.probeRTTRoundDone = true
			}
			if b.probeRTTRoundDone && !now.Before(b.probeRTTDone) {
				b.rtPropStamp = now
				b.cwnd = max(b.cwnd, b.priorCwnd)
				b.priorCwnd = 0
				if b.filledPipe {
					b.enterProbeBW(now)
				} else {
					b.state, b.pacingGain, b.cwndGain = bbrStartup, bbrHighGain, bbrHighGain
				}
			}
		}
	}

	// Window: grow toward cwnd_gain × BDP (plus room for ACK aggregation).
	target := b.bdp(b.cwndGain) + 3*b.mss
	if b.filledPipe {
		b.cwnd = min(b.cwnd+rs.acked, target)
	} else if b.cwnd < target || rs.delivered < uint64(initialWindow(b.mss)) {
		b.cwnd += rs.acked
	}
	b.cwnd = max(b.cwnd, 4*b.mss)
}

func (b *bbr) enterProbeBW(now time.Time) {
	b.state = bbrProbeBW
	b.cwndGain = bbrCwndGain
	// Start in a cruising phase (the RFC picks randomly among non-0.75
	// phases; a fixed one keeps runs reproducible).
	b.cycleIdx = 2
	b.cycleStamp = now
	b.pacingGain = bbrPacingCycle[b.cycleIdx]
}
