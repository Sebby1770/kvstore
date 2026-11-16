package foresight

import (
	"math"
	"time"
)

// Phi is the φ-accrual failure detector (Hayashibara et al., 2004). Instead
// of a binary timeout it outputs a continuous suspicion level from the
// distribution of past heartbeat inter-arrival times: φ = -log10(P(a
// heartbeat this late or later)). φ = 1 means a 10% chance the node is
// merely slow, φ = 3 a 0.1% chance, and so on. A link that is normally
// jittery needs a longer silence to reach the same φ than a steady one.
type Phi struct {
	intervals []float64 // seconds, ring
	pos, n    int
	last      time.Time
	minStd    float64
}

// NewPhi keeps a window of the last size intervals.
func NewPhi(size int) *Phi {
	if size <= 0 {
		size = 100
	}
	return &Phi{intervals: make([]float64, size), minStd: 0.01}
}

// Heartbeat records an arrival.
func (p *Phi) Heartbeat(now time.Time) {
	if !p.last.IsZero() {
		p.intervals[p.pos] = now.Sub(p.last).Seconds()
		p.pos = (p.pos + 1) % len(p.intervals)
		if p.n < len(p.intervals) {
			p.n++
		}
	}
	p.last = now
}

// Level is φ at now (0 before two heartbeats have been seen).
func (p *Phi) Level(now time.Time) float64 {
	if p.n == 0 {
		return 0
	}
	var mean, sq float64
	for i := 0; i < p.n; i++ {
		mean += p.intervals[i]
	}
	mean /= float64(p.n)
	for i := 0; i < p.n; i++ {
		d := p.intervals[i] - mean
		sq += d * d
	}
	std := math.Max(math.Sqrt(sq/float64(p.n)), math.Max(p.minStd, mean*0.1))
	x := now.Sub(p.last).Seconds()
	u := (x - mean) / (std * math.Sqrt2)
	if u > 6 {
		// Far in the tail erfc underflows; use its asymptotic expansion,
		// erfc(u) ~ exp(-u²) / (u √π), so φ keeps rising with the silence.
		return u*u/math.Ln10 + math.Log10(2*u*math.SqrtPi)
	}
	return -math.Log10(0.5 * math.Erfc(u))
}

// Since is the time since the last heartbeat.
func (p *Phi) Since(now time.Time) time.Duration {
	if p.last.IsZero() {
		return 0
	}
	return now.Sub(p.last)
}
