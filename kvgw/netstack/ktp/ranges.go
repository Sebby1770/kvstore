package ktp

// rangeSet is a sorted set of disjoint half-open intervals [lo, hi). Streams
// use it for acknowledged and to-be-retransmitted byte ranges; the ACK
// tracker uses it (inclusive, via hi-1) for received packet numbers.
type rangeSet []span

type span struct{ lo, hi uint64 }

// add inserts [lo, hi), merging neighbours it touches.
func (s *rangeSet) add(lo, hi uint64) {
	if lo >= hi {
		return
	}
	r := *s
	// first span that ends at or after lo (could merge)
	i := 0
	for i < len(r) && r[i].hi < lo {
		i++
	}
	j := i
	for j < len(r) && r[j].lo <= hi {
		lo = min(lo, r[j].lo)
		hi = max(hi, r[j].hi)
		j++
	}
	out := append(r[:i:i], span{lo, hi})
	out = append(out, r[j:]...)
	*s = out
}

// remove deletes [lo, hi) from the set.
func (s *rangeSet) remove(lo, hi uint64) {
	if lo >= hi {
		return
	}
	var out rangeSet
	for _, x := range *s {
		if x.hi <= lo || x.lo >= hi {
			out = append(out, x)
			continue
		}
		if x.lo < lo {
			out = append(out, span{x.lo, lo})
		}
		if x.hi > hi {
			out = append(out, span{hi, x.hi})
		}
	}
	*s = out
}

// contains reports whether v is in the set.
func (s rangeSet) contains(v uint64) bool {
	for _, x := range s {
		if v < x.lo {
			return false
		}
		if v < x.hi {
			return true
		}
	}
	return false
}

// covers reports whether all of [lo, hi) is in the set.
func (s rangeSet) covers(lo, hi uint64) bool {
	for _, x := range s {
		if x.lo <= lo && hi <= x.hi {
			return true
		}
	}
	return lo >= hi
}

// gaps returns the parts of [lo, hi) not in the set.
func (s rangeSet) gaps(lo, hi uint64) []span {
	var out []span
	cur := lo
	for _, x := range s {
		if x.hi <= cur {
			continue
		}
		if x.lo >= hi {
			break
		}
		if x.lo > cur {
			out = append(out, span{cur, min(x.lo, hi)})
		}
		cur = max(cur, x.hi)
		if cur >= hi {
			break
		}
	}
	if cur < hi {
		out = append(out, span{cur, hi})
	}
	return out
}

// trimBelow drops everything below v.
func (s *rangeSet) trimBelow(v uint64) {
	r := *s
	for len(r) > 0 && r[0].hi <= v {
		r = r[1:]
	}
	if len(r) > 0 && r[0].lo < v {
		r[0].lo = v
	}
	*s = r
}

func (s rangeSet) empty() bool { return len(s) == 0 }
