package vtime

// Dataset is what a clock advertises for grandmaster election, compared
// field by field in IEEE 1588's best-master-clock order: lower wins.
type Dataset struct {
	Priority1 uint8  // operator preference
	Class     uint8  // traceability: 6 = locked to a primary reference (GNSS/PTP card), 248 = free-running default
	Accuracy  uint8  // claimed accuracy class (lower is better)
	Variance  uint16 // oscillator stability
	Priority2 uint8  // tie-break preference
	Identity  string // unique name: the final tie-break
}

// Default is the dataset of an ordinary node with no reference clock.
func Default(identity string) Dataset {
	return Dataset{Priority1: 128, Class: 248, Accuracy: 0xfe, Variance: 0xffff, Priority2: 128, Identity: identity}
}

// Better reports whether a beats b.
func (a Dataset) Better(b Dataset) bool {
	switch {
	case a.Priority1 != b.Priority1:
		return a.Priority1 < b.Priority1
	case a.Class != b.Class:
		return a.Class < b.Class
	case a.Accuracy != b.Accuracy:
		return a.Accuracy < b.Accuracy
	case a.Variance != b.Variance:
		return a.Variance < b.Variance
	case a.Priority2 != b.Priority2:
		return a.Priority2 < b.Priority2
	}
	return a.Identity < b.Identity
}

// Elect picks the grandmaster among candidates (the live members). Every
// node runs it over the same membership and so agrees without a vote.
func Elect(candidates []Dataset) (Dataset, bool) {
	if len(candidates) == 0 {
		return Dataset{}, false
	}
	best := candidates[0]
	for _, c := range candidates[1:] {
		if c.Better(best) {
			best = c
		}
	}
	return best, true
}
