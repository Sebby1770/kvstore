// Package crdt provides conflict-free replicated data types: values that MERGE
// instead of last-writer-wins, so concurrent updates all survive and replicas
// converge regardless of the order they exchange state.
//
// Each type's Merge is a join on a lattice — associative, commutative, and
// idempotent — which is exactly what makes "apply in any order, any number of
// times" safe (and is verified in the tests).
package crdt

import (
	"encoding/json"
	"sort"
)

// PNCounter is a positive-negative counter: a grow-only count of increments and
// of decrements, per actor. Merge keeps the per-actor maximum, so re-merging
// stale state can never lose a count.
type PNCounter struct {
	P map[string]int64 `json:"p"`
	N map[string]int64 `json:"n"`
}

func NewPNCounter() *PNCounter {
	return &PNCounter{P: map[string]int64{}, N: map[string]int64{}}
}

// Add applies a signed delta under the given actor id.
func (c *PNCounter) Add(actor string, delta int64) {
	if delta >= 0 {
		c.P[actor] += delta
	} else {
		c.N[actor] += -delta
	}
}

func (c *PNCounter) Value() int64 {
	var v int64
	for _, x := range c.P {
		v += x
	}
	for _, x := range c.N {
		v -= x
	}
	return v
}

func (c *PNCounter) Merge(o *PNCounter) {
	for a, x := range o.P {
		if x > c.P[a] {
			c.P[a] = x
		}
	}
	for a, x := range o.N {
		if x > c.N[a] {
			c.N[a] = x
		}
	}
}

func (c *PNCounter) Bytes() []byte {
	b, _ := json.Marshal(c)
	return b
}

func ParsePNCounter(b []byte) (*PNCounter, error) {
	c := NewPNCounter()
	if len(b) == 0 {
		return c, nil
	}
	if err := json.Unmarshal(b, c); err != nil {
		return nil, err
	}
	if c.P == nil {
		c.P = map[string]int64{}
	}
	if c.N == nil {
		c.N = map[string]int64{}
	}
	return c, nil
}

// ORSet is an observed-remove set (add-wins). Each add stamps the element with
// a unique tag; a remove only retires the tags it has actually observed. So a
// concurrent add (a fresh, unobserved tag) survives a concurrent remove, and
// merging is just a union of adds and of retired tags.
type ORSet struct {
	Adds map[string]map[string]bool `json:"adds"` // element -> set of live tags
	Rem  map[string]bool            `json:"rem"`  // retired tags
}

func NewORSet() *ORSet {
	return &ORSet{Adds: map[string]map[string]bool{}, Rem: map[string]bool{}}
}

// Add stamps element with a (caller-supplied, unique) tag.
func (s *ORSet) Add(element, tag string) {
	if s.Adds[element] == nil {
		s.Adds[element] = map[string]bool{}
	}
	s.Adds[element][tag] = true
}

// Remove retires every tag currently observed for element (observed-remove).
func (s *ORSet) Remove(element string) {
	for tag := range s.Adds[element] {
		s.Rem[tag] = true
	}
}

func (s *ORSet) Contains(element string) bool {
	for tag := range s.Adds[element] {
		if !s.Rem[tag] {
			return true
		}
	}
	return false
}

func (s *ORSet) Members() []string {
	out := []string{}
	for element := range s.Adds {
		if s.Contains(element) {
			out = append(out, element)
		}
	}
	sort.Strings(out)
	return out
}

func (s *ORSet) Merge(o *ORSet) {
	for element, tags := range o.Adds {
		if s.Adds[element] == nil {
			s.Adds[element] = map[string]bool{}
		}
		for tag := range tags {
			s.Adds[element][tag] = true
		}
	}
	for tag := range o.Rem {
		s.Rem[tag] = true
	}
}

func (s *ORSet) Bytes() []byte {
	b, _ := json.Marshal(s)
	return b
}

func ParseORSet(b []byte) (*ORSet, error) {
	s := NewORSet()
	if len(b) == 0 {
		return s, nil
	}
	if err := json.Unmarshal(b, s); err != nil {
		return nil, err
	}
	if s.Adds == nil {
		s.Adds = map[string]map[string]bool{}
	}
	if s.Rem == nil {
		s.Rem = map[string]bool{}
	}
	return s, nil
}
