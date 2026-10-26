package ktp

import (
	"testing"
	"time"

	"github.com/Sebby1770/kvstore/kvgw/netstack/simnet"
)

// BBR must be as correct as the loss-based controllers under every fault.
func TestBBRBulkTransferUnderFaults(t *testing.T) {
	for _, loss := range []float64{0, 0.05, 0.20} {
		link := simnet.Link{Latency: 15 * time.Millisecond, Jitter: 5 * time.Millisecond,
			Loss: loss, Duplicate: 0.05, Reorder: 0.10, Corrupt: 0.01}
		h := transfer(t, 300+uint64(loss*100), link, 8, 256<<10, func(h *harness) { h.cfg[0].Congestion = "bbr" })
		if st := h.conns[0].Stats(); st.Congestion != "bbr" {
			t.Fatalf("controller %q", st.Congestion)
		}
	}
}

// On a bandwidth-limited path with random (non-congestive) loss, a
// loss-based controller collapses to the Mathis bound while BBR keeps the
// pipe full. This is the reason KTP ships a model-based controller.
func TestBBRToleratesRandomLoss(t *testing.T) {
	const bw = 2_000_000 // bytes/s
	const size = 2 << 20
	link := simnet.Link{Latency: 20 * time.Millisecond, Bandwidth: bw, QueueBytes: 100_000, Loss: 0.02}
	elapsed := func(cc string) float64 {
		h := transfer(t, 41, link, 1, size, func(h *harness) { h.cfg[0].Congestion = cc })
		return h.now.Sub(h.start).Seconds()
	}
	bbr, reno := elapsed("bbr"), elapsed("newreno")
	ideal := float64(size) / bw
	if util := ideal / bbr; util < 0.6 {
		t.Fatalf("bbr utilisation %.2f at 2%% random loss (%.1fs for %.1fs of data)", util, bbr, ideal)
	}
	if reno < 3*bbr {
		t.Fatalf("expected newreno to be far slower at 2%% loss: bbr %.1fs, newreno %.1fs", bbr, reno)
	}
	t.Logf("2 MiB over 2 MB/s at 2%% loss: bbr %.1fs (%.0f%% of link), newreno %.1fs", bbr, 100*ideal/bbr, reno)
}

// Feed BBR a clean path model and watch it walk Startup -> Drain -> ProbeBW
// with a window near cwnd_gain x BDP.
func TestBBRStateMachine(t *testing.T) {
	b := newBBR(mss)
	now := epoch
	const rate = 1_000_000.0 // bytes/s
	rtt := 50 * time.Millisecond
	var delivered uint64
	sample := func(r float64, inflight int) {
		prior := delivered
		delivered += 100_000
		now = now.Add(rtt)
		b.onRateSample(now, rateSample{rate: r, rtt: rtt, delivered: delivered, priorDelivered: prior,
			acked: 100_000, inflight: inflight})
	}
	sample(rate, 200_000)
	if b.State() != "startup" {
		t.Fatalf("state %s", b.State())
	}
	for i := 0; i < 5; i++ { // bandwidth stops growing: pipe full after 3 rounds
		sample(rate, 200_000)
	}
	if !b.filledPipe || b.State() != "drain" {
		t.Fatalf("after a plateau: filled=%v state=%s", b.filledPipe, b.State())
	}
	sample(rate, 40_000) // inflight below one BDP (50 KB): drain done
	if b.State() != "probe_bw" {
		t.Fatalf("state %s, want probe_bw", b.State())
	}
	bdp := rate * rtt.Seconds()
	for i := 0; i < 20; i++ {
		sample(rate, int(bdp))
	}
	want := int(bbrCwndGain*bdp) + 3*mss
	if b.Window() != want {
		t.Fatalf("cwnd %d, want 2 x BDP + 3 MSS = %d", b.Window(), want)
	}
	if r := b.pacingRate(); r < 0.75*rate || r > 1.25*rate {
		t.Fatalf("pacing rate %.0f outside the probe cycle around %.0f", r, rate)
	}
	// Loss does not move the window.
	b.OnLoss(now, now)
	if b.Window() != want {
		t.Fatal("bbr reacted to loss")
	}
	// After 10s without a new RTprop sample, ProbeRTT drains to 4 packets.
	now = now.Add(11 * time.Second)
	b.onRateSample(now, rateSample{rate: rate, rtt: 2 * rtt, delivered: delivered + 1, priorDelivered: delivered, acked: 1, inflight: int(bdp)})
	if b.State() != "probe_rtt" || b.Window() != 4*mss {
		t.Fatalf("state %s window %d, want probe_rtt with 4 packets", b.State(), b.Window())
	}
}

func TestBBRStartupExitsOnHeavyLoss(t *testing.T) {
	b := newBBR(mss)
	now := epoch
	var delivered uint64
	for round := 0; round < 3; round++ {
		prior := delivered
		delivered += 50_000
		now = now.Add(40 * time.Millisecond)
		b.onRateSample(now, rateSample{rate: float64(delivered) * 10, rtt: 40 * time.Millisecond,
			delivered: delivered, priorDelivered: prior, acked: 50_000})
		if round == 1 {
			b.onPacketsLost(now, 20, 20*mss) // a burst of tail drops (>4% of the round)
		}
	}
	if !b.filledPipe {
		t.Fatal("startup ignored a round of heavy loss")
	}
}
