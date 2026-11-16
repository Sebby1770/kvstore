// Package vtime is the runtime's notion of time: clocks disciplined to a
// cluster grandmaster by PTP-style two-way time transfer, with a rigorous
// error bound on every reading; matrix clocks for causal stability; and a
// watermark sequencer that turns hybrid-logical-clock timestamps into one
// deterministic total order on every node without waiting for wall clocks
// to agree.
//
// # Disciplined clocks (IEEE 1588 delay request-response)
//
// The grandmaster -- chosen by a best-master-clock comparison of the
// members' clock datasets (BMCA) -- sends SYNC carrying its time t1; a
// follower notes when it arrived (t2), answers DELAY_REQ at t3, and learns
// t4, when the request reached the master. Then
//
//	offset θ = ((t2 - t1) - (t4 - t3)) / 2   (follower minus master)
//	delay  δ = ((t2 - t1) + (t4 - t3)) / 2   (mean one-way delay)
//
// Whatever the asymmetry between the two directions, the true offset lies
// in [θ - δ, θ + δ]: the estimate is wrong by half the difference of the
// one-way delays, and that is at most their mean. A follower keeps the
// sample with the smallest δ from a short window (the "lucky packet"
// filter), steps its clock on a large offset, and otherwise slews phase and
// frequency with a PI servo.
//
// Every reading comes as an Interval [earliest, latest] that provably
// contains true (grandmaster) time: half-width ε = δ of the sample in force,
// plus the residual the servo has not yet removed, plus the worst-case drift
// of the local oscillator since that sample. That is TrueTime's contract,
// built from network measurements instead of GPS receivers and atomic
// clocks. With PTP-grade timestamping (hardware time appliances) δ is
// nanoseconds to microseconds; with software timestamps over a datacenter
// network it is tens of microseconds to a millisecond -- still an order of
// magnitude tighter than NTP's typical bound, and, unlike NTP's, a bound.
//
// The hybrid logical clocks the runtime stamps events with (package hlc)
// read their physical component from a disciplined clock, so HLC time stays
// within ε of grandmaster time on every node.
package vtime

import (
	"sync"
	"time"
)

// Interval is a time reading with its uncertainty: true time is in
// [Earliest, Latest]. Values are nanoseconds since the Unix epoch.
type Interval struct {
	Earliest, Latest int64
}

// Width is the interval's size (2ε).
func (i Interval) Width() time.Duration { return time.Duration(i.Latest - i.Earliest) }

// Contains reports whether t lies in the interval.
func (i Interval) Contains(t int64) bool { return t >= i.Earliest && t <= i.Latest }

// Clock is a local oscillator corrected towards grandmaster time. It is
// safe for concurrent use.
type Clock struct {
	mu    sync.Mutex
	local func() int64 // the raw oscillator, nanoseconds

	// corrected(l) = baseTime + (l - baseLocal) * (1 + rate)
	baseLocal, baseTime int64
	rate                float64

	synced     bool
	master     bool    // this clock is the grandmaster: exact by definition
	maxDrift   float64 // worst-case oscillator error (fraction, e.g. 200e-6)
	sampleAt   int64   // local time of the last applied sample
	sampleEps  int64   // δ of that sample plus the residual left after it
	lastAdjust int64   // local time of the previous servo step (for the I term)
	steps      uint64
	samples    uint64
}

// NewClock wraps a raw oscillator (nil = the system clock). maxDrift bounds
// its frequency error (default 200 ppm, generous for crystal oscillators).
func NewClock(local func() int64, maxDrift float64) *Clock {
	if local == nil {
		local = func() int64 { return time.Now().UnixNano() }
	}
	if maxDrift <= 0 {
		maxDrift = 200e-6
	}
	l := local()
	return &Clock{local: local, baseLocal: l, baseTime: l, maxDrift: maxDrift}
}

// SetMaster marks this clock as the grandmaster's: it is the reference, so
// its readings have no uncertainty and it never adjusts.
func (c *Clock) SetMaster(m bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if m && !c.master {
		// Keep the current reading continuous when promoted.
		l := c.local()
		c.baseTime, c.baseLocal, c.rate = c.correctedLocked(l), l, 0
	}
	c.master = m
	if m {
		c.synced = true
	}
}

func (c *Clock) correctedLocked(l int64) int64 {
	return c.baseTime + int64(float64(l-c.baseLocal)*(1+c.rate))
}

// Now is the corrected time in nanoseconds.
func (c *Clock) Now() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.correctedLocked(c.local())
}

// Time is Now as a time.Time (the physical source for an hlc.Clock).
func (c *Clock) Time() time.Time { return time.Unix(0, c.Now()) }

// Interval is the corrected time with its error bound. An unsynchronised
// follower knows nothing: its interval is unbounded.
func (c *Clock) Interval() Interval {
	c.mu.Lock()
	defer c.mu.Unlock()
	l := c.local()
	now := c.correctedLocked(l)
	if c.master {
		return Interval{now, now}
	}
	if !c.synced {
		return Interval{Earliest: 0, Latest: 1<<62 - 1}
	}
	// Since the sample, the corrected clock can have drifted by the
	// oscillator's error plus whatever error the frequency correction
	// itself carries -- bounded by |rate| (it is meant to cancel the drift).
	r := c.rate
	if r < 0 {
		r = -r
	}
	eps := c.sampleEps + int64(float64(l-c.sampleAt)*(c.maxDrift+r)) + 1
	return Interval{now - eps, now + eps}
}

// Epsilon is the current half-width of Interval.
func (c *Clock) Epsilon() time.Duration { return c.Interval().Width() / 2 }

// Synced reports whether the clock has applied at least one sample (or is
// the grandmaster).
func (c *Clock) Synced() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.synced
}

// Servo gains and limits.
const (
	kp            = 0.7
	ki            = 0.3
	maxRate       = 500e-6
	stepThreshold = int64(5 * time.Millisecond)
)

// apply corrects the clock by a measured offset theta (this clock minus the
// master's, in ns) whose uncertainty is delta, taken at local time l.
func (c *Clock) apply(theta, delta int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.master {
		return
	}
	l := c.local()
	now := c.correctedLocked(l)
	c.samples++
	residual := int64(0)
	if !c.synced || theta > stepThreshold || theta < -stepThreshold {
		// Step: jump straight to the master's time (the first sample, or a
		// large error after a fault). Frequency learning starts afresh.
		c.baseTime, c.baseLocal = now-theta, l
		c.synced = true
		c.steps++
		c.lastAdjust = l
	} else {
		// Slew: remove most of the phase error now, and fold the rest into
		// the frequency so persistent drift stops recurring.
		c.baseTime, c.baseLocal = now-int64(kp*float64(theta)), l
		if dt := l - c.lastAdjust; dt > 0 && c.lastAdjust != 0 {
			c.rate -= ki * float64(theta) / float64(dt)
			c.rate = max(min(c.rate, maxRate), -maxRate)
		}
		c.lastAdjust = l
		residual = theta - int64(kp*float64(theta))
	}
	if residual < 0 {
		residual = -residual
	}
	c.sampleAt, c.sampleEps = l, delta+residual
}

// ClockStats describes the discipline loop.
type ClockStats struct {
	Synced  bool          `json:"synced"`
	Master  bool          `json:"master"`
	Rate    float64       `json:"rate_ppm"`
	Epsilon time.Duration `json:"epsilon"`
	Samples uint64        `json:"samples"`
	Steps   uint64        `json:"steps"`
}

// Stats snapshots the clock.
func (c *Clock) Stats() ClockStats {
	eps := c.Epsilon()
	c.mu.Lock()
	defer c.mu.Unlock()
	return ClockStats{Synced: c.synced, Master: c.master, Rate: c.rate * 1e6, Epsilon: eps, Samples: c.samples, Steps: c.steps}
}
