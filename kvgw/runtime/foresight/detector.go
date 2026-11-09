// Package foresight predicts node failure before it happens. Timeout-based
// detectors (SWIM's suspicion, Raft's election timer) notice a node only
// once it has stopped answering, and are blind to grey failures -- a node
// that still answers but ever more slowly, or whose network answers while
// its application has hung. foresight watches the micro-signals that
// precede an outage on each peer link, every interval:
//
//   - transport telemetry from KTP: RTT inflation over the path minimum,
//     jitter (RTT variance), loss rate, probe timeouts, and congestion-window
//     collapse (the transport's window shrinking is the earliest sign of a
//     path in trouble);
//   - data-path heartbeats (package offload): their RTT, misses, and the
//     peer's reported host-process staleness.
//
// Three detectors read them. A φ-accrual detector grades the silence since
// the last heartbeat. An online LSTM per peer learns the link's normal
// dynamics and forecasts the next few intervals, so a trend towards failure
// shows up before it arrives; its one-step prediction error ("surprise")
// flags behaviour the link has never shown. A Mamdani fuzzy-inference stage
// combines all of it -- graded evidence, graded rules -- into one failure
// likelihood with reasons, and a state (healthy, degrading, failing) with
// hysteresis. A separate flag marks a peer whose network answers while its
// process does not (hung, not dead).
//
// The runtime acts on the forecast (package node): a leader that sees its
// own links failing hands leadership over before it is cut off, and actor
// placement steers away from peers predicted to fail.
//
// The detector is sans-IO: callers feed samples with explicit times.
package foresight

import (
	"math"
	"sort"
	"time"
)

// Sample is one interval's telemetry for a peer link. Counters are
// cumulative (as ktp.Stats reports them); the detector takes differences.
type Sample struct {
	SRTT, MinRTT, RTTVar time.Duration
	CWND                 int
	PacketsSent          uint64
	PacketsLost          uint64
	PTOs                 uint64

	// From data-path heartbeats (optional): whether this interval's probe
	// was answered, its RTT, and the peer's reported host staleness.
	Probed    bool
	Answered  bool
	ProbeRTT  time.Duration
	Staleness time.Duration
}

// State classifies a peer.
type State int

const (
	Healthy State = iota
	Degrading
	Failing
)

func (s State) String() string { return [...]string{"healthy", "degrading", "failing"}[s] }

// Assessment is the detector's view of one peer.
type Assessment struct {
	Peer       string        `json:"peer"`
	State      string        `json:"state"`
	Score      float64       `json:"score"` // failure likelihood, 0..1
	Phi        float64       `json:"phi"`
	Forecast   float64       `json:"forecast"` // predicted worsening (z units) over the horizon
	Surprise   float64       `json:"surprise"` // last prediction error (z units)
	Loss       float64       `json:"loss"`
	Inflation  float64       `json:"rtt_inflation"`
	Staleness  time.Duration `json:"staleness"`
	Hung       bool          `json:"hung"` // network answers, process does not
	Reasons    []string      `json:"reasons,omitempty"`
	Samples    int           `json:"samples"`
	TrainLoss  float64       `json:"train_loss"`
	stateValue State
}

// Config tunes the detector.
type Config struct {
	Window  int     // LSTM training window (default 16)
	Hidden  int     // LSTM hidden units (default 12)
	Horizon int     // forecast steps (default 5)
	LR      float64 // learning rate (default 0.01)
	Warmup  int     // samples before the forecast is trusted (default 30)
	Seed    uint64
}

func (c *Config) defaults() {
	if c.Window <= 0 {
		c.Window = 16
	}
	if c.Hidden <= 0 {
		c.Hidden = 12
	}
	if c.Horizon <= 0 {
		c.Horizon = 5
	}
	if c.LR <= 0 {
		c.LR = 0.01
	}
	if c.Warmup <= 0 {
		c.Warmup = 30
	}
}

// features, all oriented so that larger is worse.
const (
	fInflation = iota // srtt / min_rtt - 1
	fJitter           // rttvar / srtt
	fLoss             // lost / sent this interval
	fPTO              // probe timeouts this interval
	fCollapse         // 1 - cwnd / peak cwnd
	fProbeRTT         // heartbeat RTT / its minimum - 1
	fMiss             // heartbeat unanswered
	nFeatures
)

var featureWeight = [nFeatures]float64{1, 0.6, 1.2, 0.8, 0.8, 1, 1.5}

type peerState struct {
	lstm      *LSTM
	phi       *Phi
	hist      [][]float64 // normalised feature vectors, oldest first
	mean, vr  [nFeatures]float64
	prev      Sample
	have      bool
	peakCWND  int
	minProbe  time.Duration
	samples   int
	missRun   int // consecutive unanswered heartbeats
	staleness time.Duration
	last      Assessment
	state     State
	lastPred  []float64
	surprise  float64
	trainLoss float64
	raw       [nFeatures]float64
}

// Detector tracks every peer.
type Detector struct {
	cfg   Config
	peers map[string]*peerState
}

// New makes a detector.
func New(cfg Config) *Detector {
	cfg.defaults()
	return &Detector{cfg: cfg, peers: map[string]*peerState{}}
}

func (d *Detector) peer(name string) *peerState {
	p := d.peers[name]
	if p == nil {
		seed := d.cfg.Seed
		for _, c := range name {
			seed = seed*131 + uint64(c)
		}
		p = &peerState{lstm: NewLSTM(d.cfg.Hidden, nFeatures, seed), phi: NewPhi(100)}
		for i := range p.vr {
			p.vr[i] = 1
		}
		d.peers[name] = p
	}
	return p
}

// Forget drops a peer (it left the cluster).
func (d *Detector) Forget(name string) { delete(d.peers, name) }

// Heartbeat records a liveness signal from name (a gossip ack, a heartbeat
// reply) for the φ detector.
func (d *Detector) Heartbeat(name string, now time.Time) { d.peer(name).phi.Heartbeat(now) }

// Observe feeds one interval's telemetry for name and returns the updated
// assessment.
func (d *Detector) Observe(name string, s Sample, now time.Time) Assessment {
	p := d.peer(name)
	if s.Probed && s.Answered {
		p.phi.Heartbeat(now)
		p.staleness = s.Staleness
	}
	if !p.have {
		p.prev, p.have = s, true
		p.peakCWND = s.CWND
		return d.assess(name, p, now)
	}
	var raw [nFeatures]float64
	if s.MinRTT > 0 && s.SRTT > 0 {
		raw[fInflation] = math.Max(float64(s.SRTT)/float64(s.MinRTT)-1, 0)
		raw[fJitter] = float64(s.RTTVar) / float64(s.SRTT)
	}
	if sent := s.PacketsSent - p.prev.PacketsSent; sent > 0 && s.PacketsLost >= p.prev.PacketsLost {
		raw[fLoss] = math.Min(float64(s.PacketsLost-p.prev.PacketsLost)/float64(sent), 1)
	}
	if s.PTOs >= p.prev.PTOs {
		raw[fPTO] = math.Min(float64(s.PTOs-p.prev.PTOs), 10)
	}
	p.peakCWND = max(p.peakCWND, s.CWND)
	if p.peakCWND > 0 {
		raw[fCollapse] = 1 - float64(s.CWND)/float64(p.peakCWND)
	}
	if s.Probed {
		if s.Answered {
			if p.minProbe == 0 || s.ProbeRTT < p.minProbe {
				p.minProbe = s.ProbeRTT
			}
			if p.minProbe > 0 {
				raw[fProbeRTT] = math.Max(float64(s.ProbeRTT)/float64(p.minProbe)-1, 0)
			}
			p.missRun = 0
		} else {
			raw[fMiss] = 1
			raw[fProbeRTT] = p.raw[fProbeRTT] // carry the last value
			p.missRun++
		}
	}
	p.raw = raw
	p.prev = s
	p.samples++

	// Normalise against the link's own slowly adapting baseline -- frozen
	// while the peer looks unwell, so failure is never learnt as normal.
	z := make([]float64, nFeatures)
	learn := p.state == Healthy
	const alpha = 0.02
	for i, x := range raw {
		if learn || p.samples < 10 {
			delta := x - p.mean[i]
			p.mean[i] += alpha * delta
			p.vr[i] = (1-alpha)*p.vr[i] + alpha*delta*delta
			if p.samples < 10 { // fast start
				p.mean[i] += (x - p.mean[i]) / float64(p.samples)
			}
		}
		sd := math.Sqrt(math.Max(p.vr[i], 1e-4))
		z[i] = math.Max(math.Min((x-p.mean[i])/sd, 6), -6)
	}
	// Surprise: how wrong last interval's one-step prediction was.
	if p.lastPred != nil {
		var e float64
		for i := range z {
			diff := z[i] - p.lastPred[i]
			e += featureWeight[i] * diff * diff
		}
		p.surprise = math.Sqrt(e / float64(nFeatures))
	}
	p.hist = append(p.hist, z)
	if len(p.hist) > d.cfg.Window {
		p.hist = p.hist[1:]
	}
	if len(p.hist) >= 3 {
		p.trainLoss = p.lstm.Train(p.hist, d.cfg.LR)
	}
	return d.assess(name, p, now)
}

func (d *Detector) assess(name string, p *peerState, now time.Time) Assessment {
	a := Assessment{Peer: name, Phi: p.phi.Level(now), Samples: p.samples, Surprise: p.surprise,
		Loss: p.raw[fLoss], Inflation: p.raw[fInflation], Staleness: p.staleness, TrainLoss: p.trainLoss}
	// Forecast: the worst predicted weighted worsening over the horizon,
	// trusted progressively as the model warms up. Two forecasters vote,
	// and the more pessimistic wins: the LSTM, which forecasts the dynamics
	// it has learnt for this link (recurring patterns, the shape of past
	// trouble), and a smoothed persistence-plus-trend extrapolation, which
	// covers what no model trained on a healthy link can know -- a ramp it
	// has never seen. Smoothing keeps one noisy interval from counting as
	// a trend.
	if n := len(p.hist); n >= 4 {
		bad := make([]float64, n)
		for t, v := range p.hist {
			bad[t] = badness(v)
		}
		k := min(4, n)
		var smooth float64
		for _, b := range bad[n-k:] {
			smooth += b
		}
		smooth /= float64(k)
		m := min(8, n)
		slope := lsSlope(bad[n-m:])
		a.Forecast = smooth + math.Max(slope, 0)*float64(d.cfg.Horizon)
	}
	if len(p.hist) >= 3 {
		fc := p.lstm.Forecast(p.hist, d.cfg.Horizon)
		p.lastPred = fc[0]
		trust := math.Min(1, float64(p.samples)/float64(d.cfg.Warmup))
		for _, y := range fc {
			var w, sum float64
			for i, v := range y {
				sum += featureWeight[i] * math.Max(v, 0)
				w += featureWeight[i]
			}
			a.Forecast = math.Max(a.Forecast, trust*sum/w)
		}
	}
	// Current worsening, in the same z units, so the rules see now and next.
	var nowZ float64
	if n := len(p.hist); n > 0 {
		nowZ = badness(p.hist[n-1])
	}
	miss := p.raw[fMiss]

	phiLow, phiMed, phiHigh := Falling(1, 3), Trapezoid(1, 3, 5, 8), Rising(5, 8)
	fLowF, fMedF, fHighF := Falling(0.5, 1.2), Trapezoid(0.5, 1.2, 1.8, 2.6), Rising(1.8, 2.6)
	zMed, zHigh := Trapezoid(0.6, 1.2, 2, 3), Rising(2, 3)
	lossMed, lossHigh := Trapezoid(0.02, 0.06, 0.15, 0.3), Rising(0.15, 0.3)
	infMed, infHigh := Trapezoid(0.5, 1.5, 3, 6), Rising(3, 6)
	stale := Rising(time.Second.Seconds(), 3*time.Second.Seconds())(a.Staleness.Seconds())
	surpriseHigh := Rising(2.5, 4)(a.Surprise)

	rules := []firing{
		{"silence (phi high)", phiHigh(a.Phi), outFailing},
		{"silence growing and forecast worsening", min(phiMed(a.Phi), max(fMedF(a.Forecast), fHighF(a.Forecast))), outFailing},
		{"forecast: link failing", min(fHighF(a.Forecast), max(zHigh(nowZ), lossHigh(a.Loss), miss)), outFailing},
		{"heavy loss", lossHigh(a.Loss), outFailing},
		{"latency several times normal and loss climbing", min(infHigh(a.Inflation), max(lossMed(a.Loss), lossHigh(a.Loss), miss)), outFailing},
		// One lost probe is noise (and already in the forecast); a run of
		// them, or a silence φ calls unusual, is evidence.
		{"heartbeats unanswered", min(miss, max(phiMed(a.Phi), Rising(1, 3)(float64(p.missRun)))), outFailing},
		{"forecast: link degrading", max(fHighF(a.Forecast), min(fMedF(a.Forecast), zMed(nowZ))), outDegrading},
		{"latency inflated", max(infHigh(a.Inflation), min(infMed(a.Inflation), zMed(nowZ))), outDegrading},
		{"loss rising", lossMed(a.Loss), outDegrading},
		{"behaviour never seen before", surpriseHigh, outDegrading},
		{"host process stale", stale, outDegrading},
		{"steady", min(phiLow(a.Phi), fLowF(a.Forecast), 1-zHigh(nowZ), 1-lossMed(a.Loss)-lossHigh(a.Loss), 1-stale), outHealthy},
	}
	a.Score, a.Reasons = infer(rules)
	// Hysteresis: enter a worse state above its threshold, leave it only
	// once clearly below, so a borderline link does not flap.
	switch p.state {
	case Healthy:
		if a.Score >= 0.62 {
			p.state = Failing
		} else if a.Score >= 0.38 {
			p.state = Degrading
		}
	case Degrading:
		if a.Score >= 0.62 {
			p.state = Failing
		} else if a.Score < 0.28 {
			p.state = Healthy
		}
	case Failing:
		if a.Score < 0.28 {
			p.state = Healthy
		} else if a.Score < 0.5 {
			p.state = Degrading
		}
	}
	a.State, a.stateValue = p.state.String(), p.state
	a.Hung = stale > 0.5 && a.Phi < 3
	p.last = a
	return a
}

// badness is a feature vector's weighted worsening (positive z only).
func badness(z []float64) float64 {
	var sum, w float64
	for i, v := range z {
		sum += featureWeight[i] * math.Max(v, 0)
		w += featureWeight[i]
	}
	return sum / w
}

// lsSlope is the least-squares slope of ys against 0, 1, 2, ...
func lsSlope(ys []float64) float64 {
	n := float64(len(ys))
	if n < 2 {
		return 0
	}
	var sx, sy, sxx, sxy float64
	for i, y := range ys {
		x := float64(i)
		sx, sy, sxx, sxy = sx+x, sy+y, sxx+x*x, sxy+x*y
	}
	return (n*sxy - sx*sy) / (n*sxx - sx*sx)
}

// Assess returns name's latest assessment, re-evaluating φ at now.
func (d *Detector) Assess(name string, now time.Time) (Assessment, bool) {
	p, ok := d.peers[name]
	if !ok {
		return Assessment{}, false
	}
	return d.assess(name, p, now), true
}

// StateOf is name's current state (Healthy if unknown).
func (d *Detector) StateOf(name string) State {
	if p, ok := d.peers[name]; ok {
		return p.state
	}
	return Healthy
}

// All returns the latest assessment of every peer, sorted by name.
func (d *Detector) All() []Assessment {
	out := make([]Assessment, 0, len(d.peers))
	for _, p := range d.peers {
		out = append(out, p.last)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Peer < out[j].Peer })
	return out
}
