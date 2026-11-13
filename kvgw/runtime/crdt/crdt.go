// Package crdt is a store of delta-state conflict-free replicated data types
// (Almeida, Shoker & Baquero) for coordination-free replication: every
// replica applies its own updates at once, ships each update as a small
// delta, and merges whatever deltas arrive, in whatever order, any number of
// times -- and all replicas converge to the same state. There are no locks,
// no leaders and no rounds; the only requirement is that deltas eventually
// arrive, and (for the observed-remove types) arrive in causal order, which
// package causal provides over an unreliable network.
//
// Types:
//
//   - counter: a PN-counter (per-replica increment and decrement totals).
//   - register: a last-writer-wins register ordered by hybrid logical clock
//     timestamp, then replica name.
//   - mvregister: a multi-value register: concurrent writes are all kept
//     until a later write that saw them supersedes them.
//   - set: an add-wins observed-remove set: a remove deletes only the adds
//     it has seen, so a concurrent add survives.
//
// Each delta is itself a valid state, and merging is a join on a
// semilattice -- commutative, associative and idempotent (checked by the
// tests) -- which is what makes duplicate, reordered and repeated delivery
// harmless.
package crdt

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/Sebby1770/kvstore/kvgw/runtime/hlc"
)

// Kinds.
const (
	KindCounter    = "counter"
	KindRegister   = "register"
	KindMVRegister = "mvregister"
	KindSet        = "set"
)

// Errors.
var (
	ErrKind   = errors.New("crdt: object exists with a different kind")
	ErrAction = errors.New("crdt: unknown action for this kind")
)

// Dot is one event of one replica.
type Dot struct {
	Node string `json:"n"`
	Seq  uint64 `json:"s"`
}

// Context records which dots a state has seen: a version vector for the
// contiguous prefix of each replica's dots, plus a cloud of out-of-order
// ones (normally empty under causal delivery).
type Context struct {
	VV    map[string]uint64 `json:"vv,omitempty"`
	Cloud []Dot             `json:"cloud,omitempty"`
}

func (c *Context) seen(d Dot) bool {
	if d.Seq <= c.VV[d.Node] {
		return true
	}
	for _, x := range c.Cloud {
		if x == d {
			return true
		}
	}
	return false
}

func (c *Context) add(d Dot) {
	if c.VV == nil {
		c.VV = map[string]uint64{}
	}
	if !c.seen(d) {
		c.Cloud = append(c.Cloud, d)
	}
	c.compact()
}

func (c *Context) join(o Context) {
	if c.VV == nil {
		c.VV = map[string]uint64{}
	}
	for n, s := range o.VV {
		if s > c.VV[n] {
			c.VV[n] = s
		}
	}
	for _, d := range o.Cloud {
		if !c.seen(d) {
			c.Cloud = append(c.Cloud, d)
		}
	}
	c.compact()
}

// compact folds cloud dots that extend the contiguous prefix into the VV.
func (c *Context) compact() {
	for changed := true; changed; {
		changed = false
		kept := c.Cloud[:0]
		for _, d := range c.Cloud {
			switch {
			case d.Seq <= c.VV[d.Node]:
				changed = true // covered: drop
			case d.Seq == c.VV[d.Node]+1:
				c.VV[d.Node] = d.Seq
				changed = true
			default:
				kept = append(kept, d)
			}
		}
		c.Cloud = kept
	}
	sort.Slice(c.Cloud, func(i, j int) bool {
		if c.Cloud[i].Node != c.Cloud[j].Node {
			return c.Cloud[i].Node < c.Cloud[j].Node
		}
		return c.Cloud[i].Seq < c.Cloud[j].Seq
	})
	if len(c.Cloud) == 0 {
		c.Cloud = nil
	}
}

// next is the replica's next dot. It does not record it: the mutator's
// delta carries it, and joining the delta records it -- recording it first
// would make the join treat the new dot as already seen and removed.
func (c *Context) next(node string) Dot {
	return Dot{Node: node, Seq: c.VV[node] + 1}
}

// joinDots is the observed-remove join on one key's dots: keep a dot if
// both sides have it, or if one side has it and the other has not seen it
// (and so has not removed it).
func joinDots(a []Dot, ac Context, b []Dot, bc Context) []Dot {
	in := func(d Dot, s []Dot) bool {
		for _, x := range s {
			if x == d {
				return true
			}
		}
		return false
	}
	var out []Dot
	for _, d := range a {
		if in(d, b) || !bc.seen(d) {
			out = append(out, d)
		}
	}
	for _, d := range b {
		if !in(d, a) && !ac.seen(d) {
			out = append(out, d)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Node != out[j].Node {
			return out[i].Node < out[j].Node
		}
		return out[i].Seq < out[j].Seq
	})
	return out
}

// ---- counter ----

// Counter is a PN-counter.
type Counter struct {
	P map[string]int64 `json:"p,omitempty"`
	N map[string]int64 `json:"n,omitempty"`
}

func (c *Counter) init() {
	if c.P == nil {
		c.P, c.N = map[string]int64{}, map[string]int64{}
	}
}

// Add applies a signed amount as replica self and returns the delta.
func (c *Counter) Add(self string, amount int64) *Counter {
	c.init()
	d := &Counter{P: map[string]int64{}, N: map[string]int64{}}
	if amount >= 0 {
		c.P[self] += amount
		d.P[self] = c.P[self]
	} else {
		c.N[self] -= amount
		d.N[self] = c.N[self]
	}
	return d
}

// Join merges another counter state (per-replica maxima).
func (c *Counter) Join(o *Counter) {
	c.init()
	for k, v := range o.P {
		c.P[k] = max(c.P[k], v)
	}
	for k, v := range o.N {
		c.N[k] = max(c.N[k], v)
	}
}

// Value is the count.
func (c *Counter) Value() int64 {
	var v int64
	for _, x := range c.P {
		v += x
	}
	for _, x := range c.N {
		v -= x
	}
	return v
}

// ---- last-writer-wins register ----

// Register is a last-writer-wins register.
type Register struct {
	Value string        `json:"v"`
	TS    hlc.Timestamp `json:"ts"`
	Node  string        `json:"node"`
	Set   bool          `json:"set"`
}

func (r *Register) wins(o *Register) bool {
	if !o.Set {
		return true
	}
	if r.TS != o.TS {
		return r.TS > o.TS
	}
	return r.Node > o.Node
}

// Write sets the value at timestamp ts and returns the delta.
func (r *Register) Write(self, v string, ts hlc.Timestamp) *Register {
	w := &Register{Value: v, TS: ts, Node: self, Set: true}
	r.Join(w)
	return w
}

// Join keeps the later write.
func (r *Register) Join(o *Register) {
	if o.Set && o.wins(r) {
		*r = *o
	}
}

// ---- multi-value register ----

// MVRegister keeps every concurrent value.
type MVRegister struct {
	Vals map[string]string `json:"vals,omitempty"` // "node:seq" -> value
	Ctx  Context           `json:"ctx"`
}

func dotKey(d Dot) string { return fmt.Sprintf("%s:%d", d.Node, d.Seq) }

// parseDotKey reverses dotKey. ok is false for anything dotKey could not
// have produced (keys arrive from peers: they are validated, never trusted).
func parseDotKey(k string) (d Dot, ok bool) {
	i := strings.LastIndexByte(k, ':')
	if i < 0 {
		return Dot{}, false
	}
	s, err := strconv.ParseUint(k[i+1:], 10, 64)
	if err != nil {
		return Dot{}, false
	}
	return Dot{Node: k[:i], Seq: s}, true
}

func (m *MVRegister) dots() []Dot {
	out := make([]Dot, 0, len(m.Vals))
	for k := range m.Vals {
		if d, ok := parseDotKey(k); ok {
			out = append(out, d)
		}
	}
	return out
}

// Write replaces every value this replica has seen and returns the delta.
func (m *MVRegister) Write(self, v string) *MVRegister {
	d := m.Ctx.next(self)
	delta := &MVRegister{Vals: map[string]string{dotKey(d): v}}
	for _, old := range m.dots() {
		delta.Ctx.add(old)
	}
	delta.Ctx.add(d)
	m.Join(delta)
	return delta
}

// Join merges another state.
func (m *MVRegister) Join(o *MVRegister) {
	keep := joinDots(m.dots(), m.Ctx, o.dots(), o.Ctx)
	vals := map[string]string{}
	for _, d := range keep {
		k := dotKey(d)
		if v, ok := m.Vals[k]; ok {
			vals[k] = v
		} else {
			vals[k] = o.Vals[k]
		}
	}
	m.Vals = vals
	m.Ctx.join(o.Ctx)
}

// Values lists the concurrent values, sorted.
func (m *MVRegister) Values() []string {
	out := []string{}
	for _, v := range m.Vals {
		out = append(out, v)
	}
	sort.Strings(out)
	return out
}

// ---- add-wins observed-remove set ----

// Set is an add-wins observed-remove set.
type Set struct {
	Elems map[string][]Dot `json:"elems,omitempty"`
	Ctx   Context          `json:"ctx"`
}

// Add inserts e (superseding this replica's view of older adds of e).
func (s *Set) Add(self, e string) *Set {
	d := s.Ctx.next(self)
	delta := &Set{Elems: map[string][]Dot{e: {d}}}
	for _, old := range s.Elems[e] {
		delta.Ctx.add(old)
	}
	delta.Ctx.add(d)
	s.Join(delta)
	return delta
}

// Remove deletes the adds of e this replica has observed; a concurrent add
// elsewhere survives (add wins).
func (s *Set) Remove(e string) *Set {
	delta := &Set{}
	for _, old := range s.Elems[e] {
		delta.Ctx.add(old)
	}
	s.Join(delta)
	return delta
}

// Join merges another state.
func (s *Set) Join(o *Set) {
	out := map[string][]Dot{}
	keys := map[string]bool{}
	for k := range s.Elems {
		keys[k] = true
	}
	for k := range o.Elems {
		keys[k] = true
	}
	for k := range keys {
		if ds := joinDots(s.Elems[k], s.Ctx, o.Elems[k], o.Ctx); len(ds) > 0 {
			out[k] = ds
		}
	}
	s.Elems = out
	s.Ctx.join(o.Ctx)
}

// Members lists the elements, sorted.
func (s *Set) Members() []string {
	out := []string{}
	for k := range s.Elems {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// Contains reports membership.
func (s *Set) Contains(e string) bool { return len(s.Elems[e]) > 0 }

// ---- store ----

// Object is one named replicated object.
type Object struct {
	Kind string      `json:"kind"`
	C    *Counter    `json:"counter,omitempty"`
	R    *Register   `json:"register,omitempty"`
	M    *MVRegister `json:"mvregister,omitempty"`
	S    *Set        `json:"set,omitempty"`
}

func newObject(kind string) (*Object, error) {
	o := &Object{Kind: kind}
	switch kind {
	case KindCounter:
		o.C = &Counter{}
	case KindRegister:
		o.R = &Register{}
	case KindMVRegister:
		o.M = &MVRegister{}
	case KindSet:
		o.S = &Set{}
	default:
		return nil, fmt.Errorf("crdt: unknown kind %q", kind)
	}
	return o, nil
}

func (o *Object) join(d *Object) error {
	if d.Kind != o.Kind {
		return ErrKind
	}
	switch o.Kind {
	case KindCounter:
		if d.C != nil {
			o.C.Join(d.C)
		}
	case KindRegister:
		if d.R != nil {
			o.R.Join(d.R)
		}
	case KindMVRegister:
		if d.M != nil {
			o.M.Join(d.M)
		}
	case KindSet:
		if d.S != nil {
			o.S.Join(d.S)
		}
	}
	return nil
}

// Value renders the object's value for clients.
func (o *Object) Value() any {
	switch o.Kind {
	case KindCounter:
		return o.C.Value()
	case KindRegister:
		if !o.R.Set {
			return nil
		}
		return o.R.Value
	case KindMVRegister:
		return o.M.Values()
	case KindSet:
		return o.S.Members()
	}
	return nil
}

// Op is a client update.
type Op struct {
	Name   string `json:"name"`
	Kind   string `json:"kind"`
	Action string `json:"action"` // counter: add; register / mvregister: set; set: add, remove
	Value  string `json:"value,omitempty"`
	Amount int64  `json:"amount,omitempty"`
}

// Delta is one replicated update: a named object's delta state.
type Delta struct {
	Name string  `json:"name"`
	Obj  *Object `json:"obj"`
}

// Store holds the replica's objects. It is not safe for concurrent use;
// the node serialises access.
type Store struct {
	self  string
	clock func() hlc.Timestamp
	objs  map[string]*Object
}

// NewStore makes an empty store for replica self; clock stamps register
// writes.
func NewStore(self string, clock func() hlc.Timestamp) *Store {
	return &Store{self: self, clock: clock, objs: map[string]*Object{}}
}

func (s *Store) object(name, kind string) (*Object, error) {
	if o, ok := s.objs[name]; ok {
		if o.Kind != kind {
			return nil, ErrKind
		}
		return o, nil
	}
	o, err := newObject(kind)
	if err != nil {
		return nil, err
	}
	s.objs[name] = o
	return o, nil
}

// Apply performs a local update and returns the delta to replicate.
func (s *Store) Apply(op Op) (Delta, error) {
	o, err := s.object(op.Name, op.Kind)
	if err != nil {
		return Delta{}, err
	}
	d := &Object{Kind: op.Kind}
	switch {
	case op.Kind == KindCounter && op.Action == "add":
		d.C = o.C.Add(s.self, op.Amount)
	case op.Kind == KindRegister && op.Action == "set":
		d.R = o.R.Write(s.self, op.Value, s.clock())
	case op.Kind == KindMVRegister && op.Action == "set":
		d.M = o.M.Write(s.self, op.Value)
	case op.Kind == KindSet && op.Action == "add":
		d.S = o.S.Add(s.self, op.Value)
	case op.Kind == KindSet && op.Action == "remove":
		d.S = o.S.Remove(op.Value)
	default:
		return Delta{}, ErrAction
	}
	return Delta{Name: op.Name, Obj: d}, nil
}

// Limits on what a peer's delta may carry. Joins compare dots pairwise, so
// without them one oversized delta (a snapshot can be megabytes) would cost
// quadratic time. Honest deltas are far below both: a cloud holds only dots
// that arrived out of causal order, and an element or register carries one
// dot per concurrent writer.
const (
	maxCloud     = 1024    // out-of-order dots in one causal context
	maxDots      = 1024    // dots on one set element, or values in one MV register
	maxTotalDots = 1 << 16 // dots across a whole object: joins check each against the cloud
)

// validate rejects deltas no honest replica produces, before they touch the
// store: malformed dot keys and oversized dot collections.
func (o *Object) validate() error {
	ctx := func(c *Context) error {
		if len(c.Cloud) > maxCloud {
			return fmt.Errorf("crdt: causal context has %d out-of-order dots (limit %d)", len(c.Cloud), maxCloud)
		}
		return nil
	}
	if o.M != nil {
		if len(o.M.Vals) > maxDots {
			return fmt.Errorf("crdt: register has %d concurrent values (limit %d)", len(o.M.Vals), maxDots)
		}
		for k := range o.M.Vals {
			if _, ok := parseDotKey(k); !ok {
				return fmt.Errorf("crdt: malformed dot key %q", k)
			}
		}
		if err := ctx(&o.M.Ctx); err != nil {
			return err
		}
	}
	if o.S != nil {
		total := 0
		for e, ds := range o.S.Elems {
			if len(ds) > maxDots {
				return fmt.Errorf("crdt: element %q has %d dots (limit %d)", e, len(ds), maxDots)
			}
			if total += len(ds); total > maxTotalDots {
				return fmt.Errorf("crdt: set carries more than %d dots", maxTotalDots)
			}
		}
		if err := ctx(&o.S.Ctx); err != nil {
			return err
		}
	}
	return nil
}

// Merge joins a remote delta (or a full state from Snapshot). A delta that
// fails validation is rejected whole: the store is untouched.
func (s *Store) Merge(d Delta) error {
	if d.Obj == nil {
		return errors.New("crdt: empty delta")
	}
	if err := d.Obj.validate(); err != nil {
		return err
	}
	o, err := s.object(d.Name, d.Obj.Kind)
	if err != nil {
		return err
	}
	return o.join(d.Obj)
}

// Get returns an object's kind and value.
func (s *Store) Get(name string) (kind string, value any, ok bool) {
	o, ok := s.objs[name]
	if !ok {
		return "", nil, false
	}
	return o.Kind, o.Value(), true
}

// Names lists the objects, sorted.
func (s *Store) Names() []string {
	out := make([]string, 0, len(s.objs))
	for n := range s.objs {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// Snapshot is the whole state as deltas (each object's full state is a
// valid delta), for bootstrapping a joining replica.
func (s *Store) Snapshot() []Delta {
	out := make([]Delta, 0, len(s.objs))
	for _, n := range s.Names() {
		b, _ := json.Marshal(s.objs[n])
		var cp Object
		json.Unmarshal(b, &cp)
		out = append(out, Delta{Name: n, Obj: &cp})
	}
	return out
}

// EncodeDelta and DecodeDelta move deltas through the delivery layer.
func EncodeDelta(d Delta) ([]byte, error) { return json.Marshal(d) }

// DecodeDelta parses a delta.
func DecodeDelta(b []byte) (Delta, error) {
	var d Delta
	if err := json.Unmarshal(b, &d); err != nil {
		return Delta{}, err
	}
	if d.Obj == nil || d.Name == "" {
		return Delta{}, errors.New("crdt: malformed delta")
	}
	if _, err := newObject(d.Obj.Kind); err != nil {
		return Delta{}, err
	}
	if err := d.Obj.validate(); err != nil {
		return Delta{}, err
	}
	return d, nil
}
