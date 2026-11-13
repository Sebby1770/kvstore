package foresight

import (
	"math"
	"math/rand/v2"
)

// LSTM is a one-layer long short-term memory network trained online to
// predict the next telemetry vector from the ones before it. It is small
// (a dozen hidden units) because it models one link, and it is trained by
// truncated backpropagation through time over a sliding window with Adam,
// one step per new observation.
//
// Per step, with z = [h(t-1); x(t)]:
//
//	i = σ(Wi z + bi)   f = σ(Wf z + bf)   o = σ(Wo z + bo)   g = tanh(Wg z + bg)
//	c = f ⊙ c(t-1) + i ⊙ g      h = o ⊙ tanh(c)      y = Wy h + by
//
// and the loss is the mean squared error between y(t) and x(t+1).
type LSTM struct {
	H, D int
	W    []float64 // 4H x (H+D): gate blocks i, f, o, g
	B    []float64 // 4H
	Wy   []float64 // D x H
	By   []float64 // D

	opt  adam
	grad grads
}

type grads struct{ W, B, Wy, By []float64 }

// NewLSTM makes a network with hidden size h over d-dimensional inputs,
// initialised from seed (Xavier-scaled weights, forget-gate bias 1).
func NewLSTM(h, d int, seed uint64) *LSTM {
	rng := rand.New(rand.NewPCG(seed, 0x15f3))
	n := &LSTM{H: h, D: d, W: make([]float64, 4*h*(h+d)), B: make([]float64, 4*h), Wy: make([]float64, d*h), By: make([]float64, d)}
	s := math.Sqrt(1 / float64(h+d))
	for i := range n.W {
		n.W[i] = (rng.Float64()*2 - 1) * s
	}
	for j := h; j < 2*h; j++ {
		n.B[j] = 1 // remember by default
	}
	sy := math.Sqrt(1 / float64(h))
	for i := range n.Wy {
		n.Wy[i] = (rng.Float64()*2 - 1) * sy
	}
	n.grad = grads{W: make([]float64, len(n.W)), B: make([]float64, len(n.B)), Wy: make([]float64, len(n.Wy)), By: make([]float64, len(n.By))}
	n.opt = newAdam(len(n.W) + len(n.B) + len(n.Wy) + len(n.By))
	return n
}

func sigmoid(x float64) float64 { return 1 / (1 + math.Exp(-x)) }

// step is one forward step's cache.
type step struct {
	z, i, f, o, g, c, cPrev, th, h, y []float64
}

// forward runs the network over xs from zero state, returning the caches.
func (n *LSTM) forward(xs [][]float64, h0, c0 []float64) []step {
	H, D := n.H, n.D
	hPrev, cPrev := h0, c0
	if hPrev == nil {
		hPrev, cPrev = make([]float64, H), make([]float64, H)
	}
	out := make([]step, len(xs))
	for t, x := range xs {
		s := step{z: make([]float64, H+D), i: make([]float64, H), f: make([]float64, H), o: make([]float64, H),
			g: make([]float64, H), c: make([]float64, H), cPrev: cPrev, th: make([]float64, H), h: make([]float64, H), y: make([]float64, D)}
		copy(s.z, hPrev)
		copy(s.z[H:], x)
		for j := 0; j < H; j++ {
			var ai, af, ao, ag float64
			ri, rf, ro, rg := j*(H+D), (H+j)*(H+D), (2*H+j)*(H+D), (3*H+j)*(H+D)
			for k, zk := range s.z {
				ai += n.W[ri+k] * zk
				af += n.W[rf+k] * zk
				ao += n.W[ro+k] * zk
				ag += n.W[rg+k] * zk
			}
			s.i[j] = sigmoid(ai + n.B[j])
			s.f[j] = sigmoid(af + n.B[H+j])
			s.o[j] = sigmoid(ao + n.B[2*H+j])
			s.g[j] = math.Tanh(ag + n.B[3*H+j])
			s.c[j] = s.f[j]*cPrev[j] + s.i[j]*s.g[j]
			s.th[j] = math.Tanh(s.c[j])
			s.h[j] = s.o[j] * s.th[j]
		}
		for d := 0; d < D; d++ {
			v := n.By[d]
			for j := 0; j < H; j++ {
				v += n.Wy[d*H+j] * s.h[j]
			}
			s.y[d] = v
		}
		out[t] = s
		hPrev, cPrev = s.h, s.c
	}
	return out
}

// loss is the mean squared prediction error over a window (inputs
// xs[:len-1], targets xs[1:]).
func (n *LSTM) loss(xs [][]float64) float64 {
	steps := n.forward(xs[:len(xs)-1], nil, nil)
	var l float64
	for t, s := range steps {
		for d := range s.y {
			e := s.y[d] - xs[t+1][d]
			l += 0.5 * e * e
		}
	}
	return l / float64(len(steps))
}

// backward fills n.grad with ∂loss/∂parameters for a window.
func (n *LSTM) backward(xs [][]float64) float64 {
	H, D := n.H, n.D
	g := &n.grad
	for _, v := range [][]float64{g.W, g.B, g.Wy, g.By} {
		clear(v)
	}
	steps := n.forward(xs[:len(xs)-1], nil, nil)
	T := float64(len(steps))
	dhNext, dcNext := make([]float64, H), make([]float64, H)
	var l float64
	for t := len(steps) - 1; t >= 0; t-- {
		s := steps[t]
		dh := append([]float64(nil), dhNext...)
		for d := 0; d < D; d++ {
			e := s.y[d] - xs[t+1][d]
			l += 0.5 * e * e
			dy := e / T
			g.By[d] += dy
			for j := 0; j < H; j++ {
				g.Wy[d*H+j] += dy * s.h[j]
				dh[j] += n.Wy[d*H+j] * dy
			}
		}
		dz := make([]float64, H+D)
		for j := 0; j < H; j++ {
			do := dh[j] * s.th[j]
			dc := dh[j]*s.o[j]*(1-s.th[j]*s.th[j]) + dcNext[j]
			di, dgt, df := dc*s.g[j], dc*s.i[j], dc*s.cPrev[j]
			dcNext[j] = dc * s.f[j]
			da := [4]float64{di * s.i[j] * (1 - s.i[j]), df * s.f[j] * (1 - s.f[j]), do * s.o[j] * (1 - s.o[j]), dgt * (1 - s.g[j]*s.g[j])}
			for gate, a := range da {
				row := (gate*H + j) * (H + D)
				g.B[gate*H+j] += a
				for k, zk := range s.z {
					g.W[row+k] += a * zk
					dz[k] += n.W[row+k] * a
				}
			}
		}
		copy(dhNext, dz[:H])
	}
	return l / T
}

// Train takes one Adam step on a window and returns the loss before it.
func (n *LSTM) Train(xs [][]float64, lr float64) float64 {
	if len(xs) < 2 {
		return 0
	}
	l := n.backward(xs)
	// Clip the global gradient norm: one wild sample must not wreck the
	// model.
	var norm float64
	for _, v := range [][]float64{n.grad.W, n.grad.B, n.grad.Wy, n.grad.By} {
		for _, x := range v {
			norm += x * x
		}
	}
	norm = math.Sqrt(norm)
	scale := 1.0
	if norm > 1 {
		scale = 1 / norm
	}
	n.opt.step(lr, scale, [][]float64{n.W, n.B, n.Wy, n.By}, [][]float64{n.grad.W, n.grad.B, n.grad.Wy, n.grad.By})
	return l
}

// Forecast runs the window to build state, then predicts k further steps
// by feeding predictions back in. It returns the one-step prediction for
// the last window element's successor first.
func (n *LSTM) Forecast(xs [][]float64, k int) [][]float64 {
	if len(xs) == 0 || k <= 0 {
		return nil
	}
	steps := n.forward(xs, nil, nil)
	last := steps[len(steps)-1]
	out := [][]float64{append([]float64(nil), last.y...)}
	h, c := last.h, last.c
	for len(out) < k {
		s := n.forward([][]float64{out[len(out)-1]}, h, c)[0]
		out = append(out, append([]float64(nil), s.y...))
		h, c = s.h, s.c
	}
	return out
}

// adam is the Adam optimiser over a flat parameter list.
type adam struct {
	m, v []float64
	t    int
}

func newAdam(n int) adam { return adam{m: make([]float64, n), v: make([]float64, n)} }

func (a *adam) step(lr, scale float64, params, grads [][]float64) {
	const b1, b2, eps = 0.9, 0.999, 1e-8
	a.t++
	c1, c2 := 1-math.Pow(b1, float64(a.t)), 1-math.Pow(b2, float64(a.t))
	k := 0
	for p, ps := range params {
		for i := range ps {
			g := grads[p][i] * scale
			a.m[k] = b1*a.m[k] + (1-b1)*g
			a.v[k] = b2*a.v[k] + (1-b2)*g*g
			ps[i] -= lr * (a.m[k] / c1) / (math.Sqrt(a.v[k]/c2) + eps)
			k++
		}
	}
}
