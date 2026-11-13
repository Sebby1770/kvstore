package foresight

import "sort"

// A small Mamdani fuzzy-inference engine: crisp inputs are fuzzified by
// membership functions, rules combine them with min (AND) and max (OR), the
// fired consequents clip their output sets, and the aggregate is
// defuzzified by its centroid. Fuzzy logic suits failure detection because
// the evidence is graded and partial -- "latency somewhat high and loss
// rising" -- and a crisp threshold on any single signal either fires late
// or fires falsely.

// MF is a membership function.
type MF func(x float64) float64

// Trapezoid is 0 below a, rises to 1 at b, stays 1 to c, falls to 0 at d.
func Trapezoid(a, b, c, d float64) MF {
	return func(x float64) float64 {
		switch {
		case x <= a || x >= d:
			if (x <= a && a == b) || (x >= d && c == d) {
				return 1
			}
			return 0
		case x < b:
			return (x - a) / (b - a)
		case x <= c:
			return 1
		default:
			return (d - x) / (d - c)
		}
	}
}

// Rising is 0 below a and 1 above b (a shoulder).
func Rising(a, b float64) MF {
	return func(x float64) float64 {
		switch {
		case x <= a:
			return 0
		case x >= b:
			return 1
		}
		return (x - a) / (b - a)
	}
}

// Falling is 1 below a and 0 above b.
func Falling(a, b float64) MF {
	r := Rising(a, b)
	return func(x float64) float64 { return 1 - r(x) }
}

// Output sets on the failure-likelihood universe [0, 1].
var (
	outHealthy   = Falling(0.1, 0.35)
	outDegrading = Trapezoid(0.2, 0.4, 0.55, 0.75)
	outFailing   = Rising(0.6, 0.9)
)

type firing struct {
	name     string
	strength float64
	out      MF
}

// infer aggregates fired rules and returns the centroid plus the rules that
// fired meaningfully, strongest first.
func infer(rules []firing) (score float64, reasons []string) {
	var num, den float64
	for i := 0; i <= 100; i++ {
		x := float64(i) / 100
		mu := 0.0
		for _, r := range rules {
			if r.strength <= 0 {
				continue
			}
			mu = max(mu, min(r.strength, r.out(x)))
		}
		num += x * mu
		den += mu
	}
	if den == 0 {
		return 0, nil
	}
	fired := append([]firing(nil), rules...)
	sort.SliceStable(fired, func(i, j int) bool { return fired[i].strength > fired[j].strength })
	for _, r := range fired {
		if r.strength >= 0.3 {
			reasons = append(reasons, r.name)
		}
	}
	return num / den, reasons
}
