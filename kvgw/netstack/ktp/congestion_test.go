package ktp

import (
	"math"
	"testing"
	"time"

	"github.com/Sebby1770/kvstore/kvgw/netstack/wire"
)

const mss = 1350

var epoch = time.Unix(1_700_000_000, 0)

func TestNewRenoSlowStartAndRecovery(t *testing.T) {
	c := newNewReno(mss)
	rtt := &rttStats{}
	w0 := c.Window()
	if w0 != initialWindow(mss) {
		t.Fatalf("initial window %d", w0)
	}
	// Slow start: acknowledging a window's worth doubles it.
	for i := 0; i < w0/mss; i++ {
		c.OnAck(epoch, epoch, mss, rtt, true)
	}
	if c.Window() != 2*w0 {
		t.Fatalf("after one RTT of acks: %d, want %d", c.Window(), 2*w0)
	}
	// App-limited acks do not grow the window.
	c.OnAck(epoch, epoch, mss, rtt, false)
	if c.Window() != 2*w0 {
		t.Fatal("window grew while application-limited")
	}
	// A loss halves it; further losses of packets sent before recovery
	// started are the same congestion event.
	lossAt := epoch.Add(time.Second)
	c.OnLoss(lossAt, epoch.Add(500*time.Millisecond))
	if c.Window() != w0 || c.SSThresh() != w0 {
		t.Fatalf("after loss: cwnd %d ssthresh %d, want %d", c.Window(), c.SSThresh(), w0)
	}
	c.OnLoss(lossAt.Add(time.Millisecond), epoch.Add(900*time.Millisecond))
	if c.Window() != w0 {
		t.Fatal("second loss in the same recovery period reduced the window again")
	}
	// Acks of pre-recovery packets do not grow it either.
	c.OnAck(lossAt, epoch.Add(800*time.Millisecond), mss, rtt, true)
	if c.Window() != w0 {
		t.Fatal("grew on an ack from before recovery")
	}
	// Congestion avoidance: about one MSS per window acknowledged.
	sent := lossAt.Add(time.Millisecond)
	for i := 0; i < w0/mss; i++ {
		c.OnAck(lossAt.Add(time.Second), sent, mss, rtt, true)
	}
	if c.Window() != w0+mss {
		t.Fatalf("congestion avoidance: %d, want %d", c.Window(), w0+mss)
	}
	c.OnPersistentCongestion()
	if c.Window() != minimumWindow(mss) {
		t.Fatalf("persistent congestion: %d", c.Window())
	}
}

func TestCubicReductionAndGrowth(t *testing.T) {
	c := newCubic(mss)
	rtt := &rttStats{hasSample: true, smoothed: 50 * time.Millisecond}
	for i := 0; i < 90; i++ { // grow well past the initial window in slow start
		c.OnAck(epoch, epoch, mss, rtt, true)
	}
	before := c.Window()
	lossAt := epoch.Add(time.Second)
	c.OnLoss(lossAt, epoch.Add(500*time.Millisecond))
	if want := int(float64(before) * cubicBeta); c.Window() != want {
		t.Fatalf("beta: window %d, want %d (0.7 x %d)", c.Window(), want, before)
	}
	k := math.Cbrt(float64(before) * (1 - cubicBeta) / cubicC / mss)
	if math.Abs(c.k-k) > 1e-9 {
		t.Fatalf("K = %v, want %v", c.k, k)
	}
	// The window grows back toward W_max in concave fashion: fast at
	// first, slowing as it approaches the old maximum around t = K.
	now := lossAt
	sent := lossAt.Add(time.Millisecond)
	var samples []int
	for step := 0; step < 400; step++ {
		now = now.Add(10 * time.Millisecond)
		for i := 0; i < c.Window()/mss/5+1; i++ {
			c.OnAck(now, sent, mss, rtt, true)
		}
		samples = append(samples, c.Window())
	}
	if samples[len(samples)-1] <= before {
		t.Fatalf("cubic never regained the old maximum %d: %v", before, samples[len(samples)-1])
	}
	for i := 1; i < len(samples); i++ {
		if samples[i] < samples[i-1] {
			t.Fatalf("window shrank without loss at step %d", i)
		}
	}
}

func TestRTTEstimatorMatchesRFC9002(t *testing.T) {
	var r rttStats
	r.update(epoch, 100*time.Millisecond, 0)
	if r.smoothed != 100*time.Millisecond || r.variance != 50*time.Millisecond || r.minRTT != 100*time.Millisecond {
		t.Fatalf("first sample: %+v", r)
	}
	// Second sample 140ms with 10ms ack delay: adjusted 130ms.
	r.update(epoch, 140*time.Millisecond, 10*time.Millisecond)
	wantVar := (3*50*time.Millisecond + 30*time.Millisecond) / 4
	wantS := (7*100*time.Millisecond + 130*time.Millisecond) / 8
	if r.variance != wantVar || r.smoothed != wantS {
		t.Fatalf("second sample: smoothed %v var %v, want %v %v", r.smoothed, r.variance, wantS, wantVar)
	}
	// Ack delay is not subtracted when that would go below min_rtt.
	r.update(epoch, 105*time.Millisecond, 50*time.Millisecond)
	if r.latest != 105*time.Millisecond || r.minRTT != 100*time.Millisecond {
		t.Fatalf("third sample: %+v", r)
	}
	pto := r.pto(100*time.Millisecond, 25*time.Millisecond)
	if want := r.smoothed + 4*r.variance + 25*time.Millisecond; pto != want {
		t.Fatalf("pto %v want %v", pto, want)
	}
}

func TestPTOBackoff(t *testing.T) {
	r := recovery{cc: newNewReno(mss)}
	r.onSent(&sentPacket{pn: 0, sentAt: epoch, size: 100, ackEliciting: true})
	t0, _ := r.timer(100*time.Millisecond, 0)
	r.onTimeout(t0, 0)
	t1, _ := r.timer(100*time.Millisecond, 0)
	r.onTimeout(t1, 0)
	t2, _ := r.timer(100*time.Millisecond, 0)
	d0, d1, d2 := t0.Sub(epoch), t1.Sub(epoch), t2.Sub(epoch)
	if d1 != 2*d0 || d2 != 4*d0 {
		t.Fatalf("PTO did not double: %v %v %v", d0, d1, d2)
	}
}

func TestLossDetectionThresholds(t *testing.T) {
	r := recovery{cc: newNewReno(mss)}
	for pn := uint64(0); pn < 6; pn++ {
		r.onSent(&sentPacket{pn: pn, sentAt: epoch.Add(time.Duration(pn) * time.Millisecond), size: 100, ackEliciting: true})
	}
	// Ack only packet 4: packets 0 and 1 are >= 3 behind -> lost by packet
	// threshold; 2 and 3 wait for the time threshold.
	now := epoch.Add(50 * time.Millisecond)
	_, lost := r.onAck(now, ackOf(4), 0, 0, r.cc.Window())
	if len(lost) != 2 || lost[0].pn != 0 || lost[1].pn != 1 {
		t.Fatalf("packet threshold: lost %v", pns(lost))
	}
	if r.lossTime.IsZero() {
		t.Fatal("loss timer not armed for packets 2 and 3")
	}
	// Each fires when its own sent time + 9/8 RTT passes: 2 first, then 3.
	lost, _ = r.onTimeout(r.lossTime, 0)
	if len(lost) != 1 || lost[0].pn != 2 {
		t.Fatalf("time threshold: lost %v", pns(lost))
	}
	lost, _ = r.onTimeout(r.lossTime, 0)
	if len(lost) != 1 || lost[0].pn != 3 {
		t.Fatalf("time threshold: lost %v", pns(lost))
	}
}

func pns(ps []*sentPacket) []uint64 {
	var out []uint64
	for _, p := range ps {
		out = append(out, p.pn)
	}
	return out
}

func ackOf(pns ...uint64) (a wire.Ack) {
	for i := len(pns) - 1; i >= 0; i-- {
		a.Ranges = append(a.Ranges, wire.AckRange{Smallest: pns[i], Largest: pns[i]})
	}
	return a
}
