package foresight

import (
	"math/rand/v2"
	"testing"
	"time"
)

// link synthesises cumulative KTP-style telemetry for one peer.
type link struct {
	rng              *rand.Rand
	sent, lost, ptos uint64
	rtt, jitter      time.Duration
	loss             float64
	cwnd             int
	answer           float64 // heartbeat answer probability
	staleness        time.Duration
	probeRTT         time.Duration
}

func healthyLink(seed uint64) *link {
	return &link{rng: rand.New(rand.NewPCG(seed, 11)), rtt: 20 * time.Millisecond, jitter: 8 * time.Millisecond,
		loss: 0.005, cwnd: 100_000, answer: 0.99, probeRTT: 20 * time.Millisecond}
}

func (l *link) sample() Sample {
	pkts := uint64(200 + l.rng.IntN(100))
	l.sent += pkts
	for i := uint64(0); i < pkts; i++ {
		if l.rng.Float64() < l.loss {
			l.lost++
		}
	}
	if l.loss > 0.5 {
		l.ptos += 2
	}
	srtt := l.rtt + time.Duration(l.rng.Int64N(int64(l.jitter)+1))
	cw := l.cwnd + l.rng.IntN(l.cwnd/5+1) - l.cwnd/10
	s := Sample{SRTT: srtt, MinRTT: 20 * time.Millisecond, RTTVar: l.jitter / 2, CWND: cw,
		PacketsSent: l.sent, PacketsLost: l.lost, PTOs: l.ptos, Probed: true}
	if l.rng.Float64() < l.answer {
		s.Answered = true
		s.ProbeRTT = l.probeRTT + time.Duration(l.rng.Int64N(int64(l.jitter)+1))
		s.Staleness = l.staleness
	}
	return s
}

var t0 = time.Unix(1_700_000_000, 0)

func TestHealthyJitteryLinkIsNotFlagged(t *testing.T) {
	seeds := uint64(20)
	if testing.Short() {
		seeds = 5
	}
	for seed := uint64(1); seed <= seeds; seed++ {
		d := New(Config{Seed: seed})
		l := healthyLink(seed)
		failing, degrading := 0, 0
		for i := 0; i < 400; i++ {
			a := d.Observe("p", l.sample(), t0.Add(time.Duration(i)*time.Second))
			if i < 40 {
				continue // warm-up
			}
			switch a.State {
			case "failing":
				failing++
			case "degrading":
				degrading++
			}
		}
		if failing > 0 || degrading > 12 {
			t.Fatalf("seed %d: healthy link flagged failing %d / degrading %d times of 360", seed, failing, degrading)
		}
	}
}

// A link that degrades -- latency climbing, loss creeping up, the
// congestion window collapsing -- and then dies. foresight must call it
// failing before it dies, and much earlier than φ alone would.
func TestGradualDegradationPredictedBeforeDeath(t *testing.T) {
	for seed := uint64(1); seed <= 5; seed++ {
		d := New(Config{Seed: seed})
		l := healthyLink(seed)
		phi := NewPhi(100)
		at := func(i int) time.Time { return t0.Add(time.Duration(i) * time.Second) }
		i := 0
		for ; i < 150; i++ {
			s := l.sample()
			d.Observe("p", s, at(i))
			if s.Answered {
				phi.Heartbeat(at(i))
			}
		}
		firstDegrading, firstFailing := -1, -1
		const ramp = 25
		for k := 0; k < ramp; k++ { // the grey failure
			f := float64(k+1) / ramp
			l.rtt = 20*time.Millisecond + time.Duration(f*180)*time.Millisecond
			l.probeRTT = l.rtt
			l.loss = 0.005 + f*0.2
			l.cwnd = int(100_000 * (1 - 0.9*f))
			l.answer = 0.99 - 0.4*f
			s := l.sample()
			a := d.Observe("p", s, at(i))
			if s.Answered {
				phi.Heartbeat(at(i))
			}
			if a.State != "healthy" && firstDegrading < 0 {
				firstDegrading = k
			}
			if a.State == "failing" && firstFailing < 0 {
				firstFailing = k
			}
			i++
		}
		death := i
		phiFires := -1
		l.loss, l.answer = 1, 0
		for k := 0; k < 30; k++ { // dead
			d.Observe("p", l.sample(), at(i))
			if phiFires < 0 && phi.Level(at(i)) > 8 {
				phiFires = i
			}
			i++
		}
		if firstFailing < 0 {
			t.Fatalf("seed %d: never predicted failing before death (degrading from step %d)", seed, firstDegrading)
		}
		lead := death - (150 + firstFailing)
		if lead < 3 {
			t.Fatalf("seed %d: failing only %d intervals before death", seed, lead)
		}
		if phiFires >= 0 && phiFires-(150+firstFailing) < 5 {
			t.Fatalf("seed %d: foresight (%d) barely beat φ (%d)", seed, 150+firstFailing, phiFires)
		}
		t.Logf("seed %d: degrading at ramp step %d, failing at %d (%d intervals before death); φ>8 at %d intervals after death",
			seed, firstDegrading, firstFailing, lead, phiFires-death)
		// And it recovers: the node comes back healthy.
		l.rtt, l.probeRTT, l.loss, l.cwnd, l.answer = 20*time.Millisecond, 20*time.Millisecond, 0.005, 100_000, 0.99
		var last Assessment
		for k := 0; k < 60; k++ {
			last = d.Observe("p", l.sample(), at(i))
			i++
		}
		if last.State != "healthy" {
			t.Fatalf("seed %d: still %s after recovery (%+v)", seed, last.State, last)
		}
	}
}

// The network answers but the host process has stopped making progress:
// hung, not dead.
func TestHungProcess(t *testing.T) {
	d := New(Config{Seed: 1})
	l := healthyLink(1)
	var a Assessment
	for i := 0; i < 200; i++ {
		if i >= 150 {
			l.staleness = time.Duration(i-149) * time.Second
		}
		a = d.Observe("p", l.sample(), t0.Add(time.Duration(i)*time.Second))
	}
	if !a.Hung || a.State == "healthy" || a.Phi > 3 {
		t.Fatalf("hung process not recognised: %+v", a)
	}
}

func TestPhi(t *testing.T) {
	p := NewPhi(50)
	for i := 0; i < 50; i++ {
		p.Heartbeat(t0.Add(time.Duration(i) * time.Second))
	}
	last := t0.Add(49 * time.Second)
	if v := p.Level(last.Add(time.Second)); v > 1 {
		t.Fatalf("on-time heartbeat φ = %.2f", v)
	}
	prev := 0.0
	for s := 2; s < 10; s++ {
		v := p.Level(last.Add(time.Duration(s) * time.Second))
		if v <= prev {
			t.Fatalf("φ not increasing with silence: %.2f then %.2f", prev, v)
		}
		prev = v
	}
	if prev < 8 {
		t.Fatalf("9s of silence on a 1s heartbeat: φ = %.2f", prev)
	}
}
