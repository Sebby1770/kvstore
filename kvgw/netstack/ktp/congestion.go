package ktp

import (
	"math"
	"time"
)

// CongestionController decides how many bytes may be in flight. The
// connection reports every send, acknowledgement and loss; the controller
// answers with a window. Both implementations follow RFC 9002 §7 for slow
// start, recovery periods and persistent congestion; they differ in how the
// window grows in congestion avoidance and how far it falls on loss.
type CongestionController interface {
	Name() string
	Window() int
	SSThresh() int
	// OnAck reports acknowledged bytes of a packet sent at sentAt. limited is
	// false when the sender was application-limited (well under the window),
	// in which case the window must not grow on evidence it never tested.
	OnAck(now, sentAt time.Time, bytes int, rtt *rttStats, limited bool)
	// OnLoss reports a congestion event for a lost packet sent at sentAt.
	// Losses of packets sent before the current recovery period began are
	// part of the same event and must not reduce the window again.
	OnLoss(now, sentAt time.Time)
	// OnPersistentCongestion collapses the window to its minimum.
	OnPersistentCongestion()
}

func initialWindow(mss int) int { return min(10*mss, max(14720, 2*mss)) }
func minimumWindow(mss int) int { return 2 * mss }

// ---- NewReno ----

type newReno struct {
	mss, cwnd, ssthresh int
	recoveryStart       time.Time
	ackedInCA           int
}

func newNewReno(mss int) *newReno {
	return &newReno{mss: mss, cwnd: initialWindow(mss), ssthresh: math.MaxInt}
}

func (c *newReno) Name() string  { return "newreno" }
func (c *newReno) Window() int   { return c.cwnd }
func (c *newReno) SSThresh() int { return c.ssthresh }

func (c *newReno) inRecovery(sentAt time.Time) bool {
	return !c.recoveryStart.IsZero() && !sentAt.After(c.recoveryStart)
}

func (c *newReno) OnAck(_, sentAt time.Time, bytes int, _ *rttStats, limited bool) {
	if c.inRecovery(sentAt) || !limited {
		return
	}
	if c.cwnd < c.ssthresh {
		c.cwnd += bytes // slow start: one MSS per MSS acknowledged
		return
	}
	// Congestion avoidance: one MSS per window acknowledged.
	c.ackedInCA += bytes
	if c.ackedInCA >= c.cwnd {
		c.ackedInCA -= c.cwnd
		c.cwnd += c.mss
	}
}

func (c *newReno) OnLoss(now, sentAt time.Time) {
	if c.inRecovery(sentAt) {
		return
	}
	c.recoveryStart = now
	c.ssthresh = max(c.cwnd/2, minimumWindow(c.mss))
	c.cwnd = c.ssthresh
	c.ackedInCA = 0
}

func (c *newReno) OnPersistentCongestion() {
	c.cwnd = minimumWindow(c.mss)
	c.recoveryStart = time.Time{}
	c.ackedInCA = 0
}

// ---- CUBIC (RFC 9438) ----

const (
	cubicC    = 0.4
	cubicBeta = 0.7
)

type cubic struct {
	mss, cwnd, ssthresh int
	recoveryStart       time.Time

	wMax       float64   // window (bytes) before the last reduction
	k          float64   // seconds for the cubic curve to return to wMax
	epochStart time.Time // start of the current congestion-avoidance epoch
	wEst       float64   // Reno-friendly estimate (bytes)
	cwndInc    float64   // fractional growth carried between ACKs
}

func newCubic(mss int) *cubic {
	return &cubic{mss: mss, cwnd: initialWindow(mss), ssthresh: math.MaxInt}
}

func (c *cubic) Name() string  { return "cubic" }
func (c *cubic) Window() int   { return c.cwnd }
func (c *cubic) SSThresh() int { return c.ssthresh }

func (c *cubic) inRecovery(sentAt time.Time) bool {
	return !c.recoveryStart.IsZero() && !sentAt.After(c.recoveryStart)
}

// target is W_cubic(t) in bytes.
func (c *cubic) target(t time.Duration) float64 {
	x := t.Seconds() - c.k
	return (cubicC*x*x*x)*float64(c.mss) + c.wMax
}

func (c *cubic) OnAck(now, sentAt time.Time, bytes int, rtt *rttStats, limited bool) {
	if c.inRecovery(sentAt) || !limited {
		return
	}
	if c.cwnd < c.ssthresh {
		c.cwnd += bytes
		return
	}
	if c.epochStart.IsZero() {
		c.epochStart = now
		if c.wMax < float64(c.cwnd) {
			c.wMax = float64(c.cwnd)
			c.k = 0
		}
		c.wEst = float64(c.cwnd)
	}
	t := now.Sub(c.epochStart) + rtt.smoothedOr(0)
	target := c.target(t)
	cw := float64(c.cwnd)
	if target > 1.5*cw {
		target = 1.5 * cw
	}
	// Reno-friendly region: grow at least as fast as Reno with the same beta.
	alpha := 3 * (1 - cubicBeta) / (1 + cubicBeta)
	c.wEst += alpha * float64(c.mss) * float64(bytes) / cw
	if c.wEst > target {
		target = c.wEst
	}
	if target > cw {
		c.cwndInc += (target - cw) * float64(bytes) / cw
		if c.cwndInc >= 1 {
			inc := int(c.cwndInc)
			c.cwnd += inc
			c.cwndInc -= float64(inc)
		}
	}
}

func (c *cubic) OnLoss(now, sentAt time.Time) {
	if c.inRecovery(sentAt) {
		return
	}
	c.recoveryStart = now
	cw := float64(c.cwnd)
	// Fast convergence: if we are already below the previous maximum,
	// release more bandwidth for newer flows.
	if cw < c.wMax {
		c.wMax = cw * (1 + cubicBeta) / 2
	} else {
		c.wMax = cw
	}
	c.cwnd = max(int(cw*cubicBeta), minimumWindow(c.mss))
	c.ssthresh = c.cwnd
	c.k = math.Cbrt(c.wMax * (1 - cubicBeta) / cubicC / float64(c.mss))
	c.epochStart = time.Time{}
	c.cwndInc = 0
}

func (c *cubic) OnPersistentCongestion() {
	c.cwnd = minimumWindow(c.mss)
	c.recoveryStart = time.Time{}
	c.epochStart = time.Time{}
	c.wMax = 0
	c.k = 0
}

func newController(name string, mss int) CongestionController {
	switch name {
	case "cubic":
		return newCubic(mss)
	case "bbr":
		return newBBR(mss)
	}
	return newNewReno(mss)
}

// ---- pacing ----

// pacer is a token bucket that spreads a window over an RTT instead of
// bursting it onto the wire. Rate is 1.25 × cwnd / srtt (a little faster than
// the window, so pacing never becomes the bottleneck); the bucket holds a
// burst of up to ten packets.
type pacer struct {
	tokens   float64
	last     time.Time
	capacity float64
}

// defaultRate is 1.25 × cwnd / srtt, or 0 (unpaced) before an RTT sample.
func defaultRate(cwnd int, rtt *rttStats) float64 {
	if !rtt.hasSample || rtt.smoothed <= 0 {
		return 0
	}
	return 1.25 * float64(cwnd) / rtt.smoothed.Seconds()
}

func (p *pacer) refill(now time.Time, rate float64, mss int) {
	p.capacity = float64(10 * mss)
	if p.last.IsZero() {
		p.tokens = p.capacity
	} else if now.After(p.last) {
		p.tokens = min(p.capacity, p.tokens+now.Sub(p.last).Seconds()*rate)
	}
	p.last = now
}

// allow reports whether size bytes may be sent at now at the given rate
// (bytes/s, 0 = unpaced), or when they may.
func (p *pacer) allow(now time.Time, size int, r float64, mss int) (bool, time.Time) {
	if r <= 0 {
		return true, time.Time{}
	}
	p.refill(now, r, mss)
	if p.tokens >= float64(size) {
		return true, time.Time{}
	}
	wait := time.Duration((float64(size) - p.tokens) / r * float64(time.Second))
	return false, now.Add(max(wait, time.Microsecond))
}

func (p *pacer) spend(size int) { p.tokens -= float64(size) }
