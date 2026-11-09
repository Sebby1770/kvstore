package vtime

import "sort"

// Matrix is a matrix clock: row i is node i's vector clock as this node
// knows it (how many events from each origin node i is known to have seen).
// The diagonal row is this node's own vector clock. Its use is causal
// *stability*: an event from origin j with sequence s is stable -- every
// member has it -- once s <= min over rows i of M[i][j]. Stable events can be
// forgotten (their retransmission buffers garbage-collected) without
// anyone ever needing them again.
type Matrix struct {
	self string
	rows map[string]map[string]uint64
}

// NewMatrix starts a matrix clock for self.
func NewMatrix(self string) *Matrix {
	return &Matrix{self: self, rows: map[string]map[string]uint64{self: {}}}
}

func (m *Matrix) row(n string) map[string]uint64 {
	r := m.rows[n]
	if r == nil {
		r = map[string]uint64{}
		m.rows[n] = r
	}
	return r
}

// Observe records that node has seen events up to seq from origin.
func (m *Matrix) Observe(node, origin string, seq uint64) {
	if r := m.row(node); seq > r[origin] {
		r[origin] = seq
	}
}

// MergeRow folds in node's vector clock (from a digest or a message's
// dependencies): node has seen at least these.
func (m *Matrix) MergeRow(node string, vc map[string]uint64) {
	r := m.row(node)
	for o, s := range vc {
		if s > r[o] {
			r[o] = s
		}
	}
}

// Own is this node's vector clock (a copy).
func (m *Matrix) Own() map[string]uint64 {
	out := map[string]uint64{}
	for o, s := range m.rows[m.self] {
		out[o] = s
	}
	return out
}

// Get is M[node][origin].
func (m *Matrix) Get(node, origin string) uint64 { return m.rows[node][origin] }

// SetMembers restricts stability to the given members (always including
// self): rows of departed nodes stop holding garbage collection back.
func (m *Matrix) SetMembers(members []string) {
	keep := map[string]bool{m.self: true}
	for _, n := range members {
		keep[n] = true
	}
	for n := range m.rows {
		if !keep[n] {
			delete(m.rows, n)
		}
	}
	for n := range keep {
		m.row(n)
	}
}

// Stable is, for origin, the highest sequence every member has seen.
func (m *Matrix) Stable(origin string) uint64 {
	first := true
	var lo uint64
	for _, r := range m.rows {
		if s := r[origin]; first || s < lo {
			lo, first = s, false
		}
	}
	return lo
}

// Members lists the nodes with rows, sorted.
func (m *Matrix) Members() []string {
	out := make([]string, 0, len(m.rows))
	for n := range m.rows {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}
