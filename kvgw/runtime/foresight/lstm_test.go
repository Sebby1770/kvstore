package foresight

import (
	"math"
	"math/rand/v2"
	"testing"
)

// The analytic gradient matches a central-difference numerical gradient for
// every parameter: backpropagation through time is right.
func TestLSTMGradientCheck(t *testing.T) {
	n := NewLSTM(3, 2, 7)
	rng := rand.New(rand.NewPCG(1, 2))
	xs := make([][]float64, 5)
	for i := range xs {
		xs[i] = []float64{rng.NormFloat64(), rng.NormFloat64()}
	}
	n.backward(xs)
	check := func(name string, params, grad []float64) {
		for i := range params {
			orig := params[i]
			const h = 1e-6
			params[i] = orig + h
			lp := n.loss(xs)
			params[i] = orig - h
			lm := n.loss(xs)
			params[i] = orig
			num := (lp - lm) / (2 * h)
			if d := math.Abs(num - grad[i]); d > 1e-6 && d/math.Max(math.Abs(num), math.Abs(grad[i])) > 1e-4 {
				t.Fatalf("%s[%d]: analytic %.8g numeric %.8g", name, i, grad[i], num)
			}
		}
	}
	g := n.grad
	check("W", n.W, append([]float64(nil), g.W...))
	check("B", n.B, append([]float64(nil), g.B...))
	check("Wy", n.Wy, append([]float64(nil), g.Wy...))
	check("By", n.By, append([]float64(nil), g.By...))
}

// Online training learns a periodic signal: the loss falls by an order of
// magnitude and the multi-step forecast tracks the true continuation.
func TestLSTMLearnsSequence(t *testing.T) {
	n := NewLSTM(12, 1, 3)
	series := func(t int) float64 { return math.Sin(float64(t) * 0.3) }
	window := func(end int) [][]float64 {
		w := make([][]float64, 16)
		for i := range w {
			w[i] = []float64{series(end - 15 + i)}
		}
		return w
	}
	first := n.loss(window(15))
	for step := 15; step < 1500; step++ {
		n.Train(window(step), 0.01)
	}
	last := n.loss(window(1500))
	if last > first/10 {
		t.Fatalf("loss %.4f -> %.4f: did not learn", first, last)
	}
	fc := n.Forecast(window(1500), 5)
	for k, y := range fc {
		if want := series(1501 + k); math.Abs(y[0]-want) > 0.25 {
			t.Fatalf("forecast step %d: %.3f, want %.3f", k+1, y[0], want)
		}
	}
}
