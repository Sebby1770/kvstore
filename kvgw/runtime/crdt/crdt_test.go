package crdt

import (
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"reflect"
	"strings"
	"testing"

	"github.com/Sebby1770/kvstore/kvgw/runtime/causal"
	"github.com/Sebby1770/kvstore/kvgw/runtime/hlc"
)

func clone(o *Object) *Object {
	b, _ := json.Marshal(o)
	var c Object
	json.Unmarshal(b, &c)
	return &c
}

func canon(o *Object) string {
	b, _ := json.Marshal(o.Value())
	return string(b)
}

// randomStates builds a few divergent replicas of one object by random
// local updates and partial exchanges.
func randomStates(rng *rand.Rand, kind string) []*Object {
	var stores []*Store
	var ts hlc.Timestamp
	for i := 0; i < 3; i++ {
		stores = append(stores, NewStore(fmt.Sprintf("r%d", i), func() hlc.Timestamp { ts++; return ts }))
	}
	for step := 0; step < 30; step++ {
		s := stores[rng.IntN(3)]
		op := Op{Name: "x", Kind: kind}
		switch kind {
		case KindCounter:
			op.Action, op.Amount = "add", rng.Int64N(21)-10
		case KindRegister, KindMVRegister:
			op.Action, op.Value = "set", fmt.Sprint(rng.IntN(5))
		case KindSet:
			op.Action, op.Value = []string{"add", "remove"}[rng.IntN(2)], fmt.Sprint(rng.IntN(4))
		}
		d, err := s.Apply(op)
		if err != nil {
			panic(err)
		}
		if rng.IntN(2) == 0 { // ship to one other replica
			stores[rng.IntN(3)].Merge(Delta{Name: "x", Obj: clone(d.Obj)})
		}
	}
	var out []*Object
	for _, s := range stores {
		if o, ok := s.objs["x"]; ok {
			out = append(out, o)
		} else {
			o, _ := newObject(kind)
			out = append(out, o)
		}
	}
	return out
}

func join(a, b *Object) *Object {
	c := clone(a)
	c.join(clone(b))
	return c
}

func TestLatticeLaws(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 1))
	for _, kind := range []string{KindCounter, KindRegister, KindMVRegister, KindSet} {
		for trial := 0; trial < 200; trial++ {
			s := randomStates(rng, kind)
			a, b, c := s[0], s[1], s[2]
			if canon(join(a, b)) != canon(join(b, a)) {
				t.Fatalf("%s: join not commutative", kind)
			}
			if canon(join(join(a, b), c)) != canon(join(a, join(b, c))) {
				t.Fatalf("%s: join not associative", kind)
			}
			if canon(join(a, a)) != canon(a) {
				t.Fatalf("%s: join not idempotent", kind)
			}
		}
	}
}

func TestAddWins(t *testing.T) {
	a, b, c := &Set{}, &Set{}, &Set{}
	d1 := a.Add("a", "x")
	b.Join(d1)
	c.Join(d1)
	rm := b.Remove("x")    // b saw a's add and removes it...
	add := c.Add("c", "x") // ...while c, concurrently, adds x again
	for _, s := range []*Set{a, b, c} {
		s.Join(rm)
		s.Join(add)
	}
	for i, s := range []*Set{a, b, c} {
		if !s.Contains("x") {
			t.Fatalf("replica %d lost the concurrent add", i)
		}
	}
	// A remove that has seen every add does remove.
	final := a.Remove("x")
	b.Join(final)
	if b.Contains("x") {
		t.Fatal("observed remove did not remove")
	}
}

func TestMVRegisterKeepsConcurrentWrites(t *testing.T) {
	a, b := &MVRegister{}, &MVRegister{}
	da := a.Write("a", "red")
	db := b.Write("b", "blue")
	a.Join(db)
	b.Join(da)
	if !reflect.DeepEqual(a.Values(), []string{"blue", "red"}) || !reflect.DeepEqual(b.Values(), a.Values()) {
		t.Fatalf("concurrent writes: %v / %v", a.Values(), b.Values())
	}
	dc := a.Write("a", "green") // has seen both: supersedes both
	b.Join(dc)
	if !reflect.DeepEqual(b.Values(), []string{"green"}) {
		t.Fatalf("superseding write: %v", b.Values())
	}
}

func TestRegisterLastWriterWins(t *testing.T) {
	r := &Register{}
	early := (&Register{}).Write("b", "old", hlc.Make(100, 0))
	late := (&Register{}).Write("a", "new", hlc.Make(100, 1))
	r.Join(late)
	r.Join(early)
	if r.Value != "new" {
		t.Fatalf("got %q", r.Value)
	}
	tie := (&Register{}).Write("z", "tie", hlc.Make(100, 1))
	r.Join(tie)
	if r.Value != "tie" {
		t.Fatal("equal timestamps must break ties by replica name")
	}
}

func TestStoreKinds(t *testing.T) {
	s := NewStore("a", func() hlc.Timestamp { return 1 })
	if _, err := s.Apply(Op{Name: "n", Kind: KindCounter, Action: "add", Amount: 2}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Apply(Op{Name: "n", Kind: KindSet, Action: "add", Value: "x"}); err != ErrKind {
		t.Fatalf("kind clash: %v", err)
	}
	if _, err := s.Apply(Op{Name: "n", Kind: KindCounter, Action: "remove"}); err != ErrAction {
		t.Fatalf("bad action: %v", err)
	}
	if _, err := DecodeDelta([]byte(`{"name":"q","obj":{"kind":"bogus"}}`)); err == nil {
		t.Fatal("decoded an unknown kind")
	}
}

// Replicas update concurrently and replicate deltas over causal broadcast
// on a lossy, duplicating network; after it heals they hold identical state,
// and the counter equals the sum of every increment made anywhere.
func TestStoresConvergeOverCausalDelivery(t *testing.T) {
	const n = 5
	rng := rand.New(rand.NewPCG(4, 4))
	var ts hlc.Timestamp
	type replica struct {
		store *Store
		node  *causal.Node
	}
	names := make([]string, n)
	reps := map[string]*replica{}
	for i := range names {
		names[i] = fmt.Sprintf("r%d", i)
	}
	for i, name := range names {
		nd := causal.New(causal.Config{Self: name, Rand: rand.New(rand.NewPCG(4, uint64(i)))})
		nd.SetPeers(names)
		reps[name] = &replica{store: NewStore(name, func() hlc.Timestamp { ts++; return ts }), node: nd}
	}
	type pkt struct {
		from, to string
		b        []byte
	}
	var inflight []pkt
	loss := 0.25
	pump := func(name string) {
		r := reps[name]
		for _, d := range r.node.Deliveries() {
			delta, err := DecodeDelta(d.Payload)
			if err != nil {
				t.Fatal(err)
			}
			if err := r.store.Merge(delta); err != nil {
				t.Fatal(err)
			}
		}
		for _, o := range r.node.Outbox() {
			if rng.Float64() >= loss {
				inflight = append(inflight, pkt{name, o.To, o.Payload})
				if rng.IntN(10) == 0 {
					inflight = append(inflight, pkt{name, o.To, o.Payload})
				}
			}
		}
	}
	deliverSome := func() {
		rng.Shuffle(len(inflight), func(i, j int) { inflight[i], inflight[j] = inflight[j], inflight[i] })
		k := len(inflight) / 2
		batch := inflight[:k]
		inflight = append([]pkt(nil), inflight[k:]...)
		for _, p := range batch {
			reps[p.to].node.Handle(p.from, p.b)
			pump(p.to)
		}
	}
	var total int64
	for step := 0; step < 600; step++ {
		name := names[rng.IntN(n)]
		r := reps[name]
		var op Op
		switch rng.IntN(4) {
		case 0:
			amt := rng.Int64N(10) + 1
			total += amt
			op = Op{Name: "hits", Kind: KindCounter, Action: "add", Amount: amt}
		case 1:
			op = Op{Name: "tags", Kind: KindSet, Action: []string{"add", "remove"}[rng.IntN(2)], Value: fmt.Sprint(rng.IntN(8))}
		case 2:
			op = Op{Name: "leader", Kind: KindRegister, Action: "set", Value: name}
		default:
			op = Op{Name: "color", Kind: KindMVRegister, Action: "set", Value: fmt.Sprint(rng.IntN(3))}
		}
		delta, err := r.store.Apply(op)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := EncodeDelta(delta)
		if _, err := r.node.Broadcast(b); err != nil {
			t.Fatal(err)
		}
		pump(name)
		deliverSome()
		if step%20 == 0 {
			for _, nm := range names {
				reps[nm].node.Tick()
				pump(nm)
			}
		}
	}
	loss = 0
	for round := 0; round < 400; round++ {
		for _, nm := range names {
			reps[nm].node.Tick()
			pump(nm)
		}
		for len(inflight) > 0 {
			deliverSome()
			if len(inflight) == 1 {
				p := inflight[0]
				inflight = nil
				reps[p.to].node.Handle(p.from, p.b)
				pump(p.to)
			}
		}
	}
	want, _ := json.Marshal(reps[names[0]].store.Snapshot())
	for _, nm := range names[1:] {
		got, _ := json.Marshal(reps[nm].store.Snapshot())
		if string(got) != string(want) {
			t.Fatalf("%s diverged:\n%s\nvs %s:\n%s", nm, got, names[0], want)
		}
	}
	if _, v, _ := reps["r3"].store.Get("hits"); v.(int64) != total {
		t.Fatalf("counter %v, want %d", v, total)
	}
}

func FuzzDecodeDelta(f *testing.F) {
	s := NewStore("a", func() hlc.Timestamp { return 1 })
	d, _ := s.Apply(Op{Name: "s", Kind: KindSet, Action: "add", Value: "v"})
	b, _ := EncodeDelta(d)
	f.Add(b)
	// One seed per kind, plus hostile shapes, so mutation starts inside every
	// decoder rather than having to invent a kind byte by byte.
	for _, op := range []Op{
		{Name: "c", Kind: KindCounter, Action: "add", Amount: -3},
		{Name: "r", Kind: KindRegister, Action: "set", Value: "v"},
		{Name: "m", Kind: KindMVRegister, Action: "set", Value: "v"},
		{Name: "s", Kind: KindSet, Action: "remove", Value: "v"},
	} {
		d, _ := s.Apply(op)
		b, _ := EncodeDelta(d)
		f.Add(b)
	}
	f.Add([]byte(`{"name":"m","obj":{"kind":"mvregister","mvregister":{"vals":{"nocolon":"v","a:1":"w"},"ctx":{"vv":{"a":18446744073709551615},"cloud":[{"n":"a","s":0}]}}}}`))
	f.Add([]byte(`{"name":"s","obj":{"kind":"set","counter":{"p":{"a":1}},"set":{"elems":{"e":[{"n":"a","s":3},{"n":"a","s":3}]},"ctx":{"cloud":[{"n":"a","s":2},{"n":"a","s":1}]}}}}`))
	f.Fuzz(func(t *testing.T, b []byte) {
		st := NewStore("me", func() hlc.Timestamp { return 1 })
		// The snapshot path merges without DecodeDelta: it must be as safe.
		var raw Delta
		if json.Unmarshal(b, &raw) == nil && raw.Obj != nil {
			st.Merge(raw)
		}
		d, err := DecodeDelta(b)
		if err != nil {
			return
		}
		st.Merge(d)
		st.Merge(d) // idempotent, and safe on top of itself
		st.Snapshot()
	})
}

// Deltas no honest replica produces are rejected whole, never panic, and
// leave the store untouched.
func TestHostileDeltasRejected(t *testing.T) {
	bigCloud := `[`
	for i := 0; i < maxCloud+1; i++ {
		if i > 0 {
			bigCloud += ","
		}
		bigCloud += fmt.Sprintf(`{"n":"x","s":%d}`, 2*i+10)
	}
	bigCloud += `]`
	for name, raw := range map[string]string{
		"dot key without a colon": `{"name":"m","obj":{"kind":"mvregister","mvregister":{"vals":{"nocolon":"v"}}}}`,
		"dot key with a bad seq":  `{"name":"m","obj":{"kind":"mvregister","mvregister":{"vals":{"a:notanumber":"v"}}}}`,
		"empty dot key":           `{"name":"m","obj":{"kind":"mvregister","mvregister":{"vals":{"":"v"}}}}`,
		"oversized cloud (set)":   `{"name":"m","obj":{"kind":"set","set":{"ctx":{"cloud":` + bigCloud + `}}}}`,
		"oversized cloud (mvreg)": `{"name":"m","obj":{"kind":"mvregister","mvregister":{"ctx":{"cloud":` + bigCloud + `}}}}`,
		"oversized element dots":  `{"name":"m","obj":{"kind":"set","set":{"elems":{"e":` + bigCloud + `}}}}`,
	} {
		if _, err := DecodeDelta([]byte(raw)); err == nil {
			t.Errorf("%s: decoded", name)
		}
		// The snapshot path unmarshals without DecodeDelta: Merge must hold
		// the line by itself.
		var d Delta
		if err := json.Unmarshal([]byte(raw), &d); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		s := NewStore("me", func() hlc.Timestamp { return 1 })
		if err := s.Merge(d); err == nil {
			t.Errorf("%s: merged", name)
		}
		if len(s.Names()) != 0 {
			// The object may be created empty; it must hold nothing hostile.
			if _, v, _ := s.Get("m"); fmt.Sprint(v) != "[]" {
				t.Errorf("%s: rejected delta left state behind: %v", name, v)
			}
		}
	}
	// A populated field that does not match the declared kind is ignored.
	var d Delta
	json.Unmarshal([]byte(`{"name":"c","obj":{"kind":"counter","mvregister":{"vals":{"a:1":"v"}}}}`), &d)
	s := NewStore("me", func() hlc.Timestamp { return 1 })
	if err := s.Merge(d); err != nil {
		t.Fatal(err)
	}
	if _, v, _ := s.Get("c"); v != int64(0) {
		t.Fatalf("mismatched field leaked into the counter: %v", v)
	}
}

// Many elements each under the per-element cap still cannot add up to an
// object whose join would take billions of comparisons.
func TestHostileDeltaTotalDots(t *testing.T) {
	var b strings.Builder
	b.WriteString(`{"name":"s","obj":{"kind":"set","set":{"elems":{`)
	per := maxDots
	for e := 0; e*per <= maxTotalDots; e++ {
		if e > 0 {
			b.WriteString(",")
		}
		fmt.Fprintf(&b, `"e%d":[`, e)
		for i := 0; i < per; i++ {
			if i > 0 {
				b.WriteString(",")
			}
			fmt.Fprintf(&b, `{"n":"x","s":%d}`, e*per+i+1)
		}
		b.WriteString("]")
	}
	b.WriteString(`}}}}`)
	if _, err := DecodeDelta([]byte(b.String())); err == nil || !strings.Contains(err.Error(), "more than") {
		t.Fatalf("an oversized set decoded: %v", err)
	}
}
