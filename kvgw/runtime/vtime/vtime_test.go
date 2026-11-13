package vtime

import (
	"fmt"
	"math/rand/v2"
	"reflect"
	"testing"
	"time"

	"github.com/Sebby1770/kvstore/kvgw/runtime/hlc"
)

// sim drives a master and a follower through exchanges on simulated time.
// True time is the master's clock; the follower's oscillator is off by
// offset and runs fast or slow by drift; each direction has its own delay
// distribution (base + uniform jitter), so the path is asymmetric.
type sim struct {
	T          int64 // true time, ns
	rng        *rand.Rand
	master     *Master
	follower   *Follower
	fclock     *Clock
	fwdBase    int64
	backBase   int64
	jitter     int64
	violations int
	checks     int
}

func newSim(seed uint64, offset time.Duration, drift float64, fwd, back, jitter time.Duration) *sim {
	s := &sim{T: int64(1_700_000_000 * time.Second), rng: rand.New(rand.NewPCG(seed, 99)),
		fwdBase: int64(fwd), backBase: int64(back), jitter: int64(jitter)}
	start := s.T
	mclock := NewClock(func() int64 { return s.T }, 0)
	s.fclock = NewClock(func() int64 { return start + int64(float64(s.T-start)*(1+drift)) + int64(offset) }, 200e-6)
	s.master = NewMaster(mclock, 1)
	s.follower = NewFollower(s.fclock, 8)
	s.follower.Adopt(1)
	return s
}

func (s *sim) delay(base int64) int64 { return base + s.rng.Int64N(s.jitter+1) }

// check asserts the follower's interval contains true time.
func (s *sim) check(t *testing.T) {
	if !s.fclock.Synced() {
		return
	}
	s.checks++
	if iv := s.fclock.Interval(); !iv.Contains(s.T) {
		s.violations++
		if s.violations < 4 {
			t.Errorf("true time %d outside [%d, %d] (off by %v, ε %v)", s.T, iv.Earliest, iv.Latest,
				time.Duration(s.fclock.Now()-s.T), iv.Width()/2)
		}
	}
}

// exchange runs one SYNC / DELAY_REQ / DELAY_RESP round.
func (s *sim) exchange(t *testing.T) {
	sync := s.master.Sync()
	s.T += s.delay(s.fwdBase)
	req, ok := s.follower.HandleSync(sync)
	if !ok {
		t.Fatal("follower ignored SYNC")
	}
	s.T += s.delay(s.backBase)
	resp, _ := s.master.HandleDelayReq(req)
	s.T += s.delay(s.fwdBase)
	s.follower.HandleDelayResp(resp)
	s.check(t)
}

func (s *sim) advance(t *testing.T, d time.Duration, checks int) {
	step := int64(d) / int64(checks)
	for i := 0; i < checks; i++ {
		s.T += step
		s.check(t)
	}
}

func TestDisciplineConverges(t *testing.T) {
	s := newSim(1, 37*time.Millisecond, 80e-6, time.Millisecond, 2*time.Millisecond, 300*time.Microsecond)
	if s.fclock.Interval().Contains(s.T) && s.fclock.Interval().Width() < time.Hour {
		t.Fatal("an unsynchronised clock claimed a bounded interval")
	}
	for i := 0; i < 120; i++ {
		s.exchange(t)
		s.advance(t, time.Second, 10)
	}
	err := time.Duration(s.fclock.Now() - s.T)
	if err < 0 {
		err = -err
	}
	// The path is 1ms one way and 2ms the other: an unavoidable 0.5ms bias,
	// which the bound must (and does) cover.
	if err > 900*time.Microsecond {
		t.Fatalf("after 2 minutes the follower is off by %v", err)
	}
	if eps := s.fclock.Epsilon(); eps > 3*time.Millisecond {
		t.Fatalf("ε = %v, want the order of the path delay", eps)
	}
	if st := s.fclock.Stats(); st.Steps != 1 || st.Rate > -50 || st.Rate < -110 {
		t.Fatalf("servo should step once and learn ~-80ppm: %+v", st)
	}
	if s.violations > 0 {
		t.Fatalf("%d of %d checks found true time outside the interval", s.violations, s.checks)
	}
}

// The interval contract under random offsets, drifts, asymmetries and
// jitter: true time is always inside.
func TestIntervalAlwaysContainsTrueTime(t *testing.T) {
	for seed := uint64(1); seed <= 40; seed++ {
		rng := rand.New(rand.NewPCG(seed, 7))
		off := time.Duration(rng.Int64N(int64(2*time.Second))) - time.Second
		drift := (rng.Float64()*2 - 1) * 150e-6
		fwd := time.Duration(rng.Int64N(int64(5*time.Millisecond))) + 50*time.Microsecond
		back := time.Duration(rng.Int64N(int64(5*time.Millisecond))) + 50*time.Microsecond
		jit := time.Duration(rng.Int64N(int64(2 * time.Millisecond)))
		s := newSim(seed, off, drift, fwd, back, jit)
		for i := 0; i < 60; i++ {
			s.exchange(t)
			s.advance(t, time.Duration(rng.Int64N(int64(3*time.Second)))+time.Millisecond, 5)
		}
		if s.violations > 0 {
			t.Fatalf("seed %d (offset %v drift %.0fppm fwd %v back %v jitter %v): %d violations",
				seed, off, drift*1e6, fwd, back, jit, s.violations)
		}
	}
}

func TestEpochsAndMessages(t *testing.T) {
	s := newSim(3, 0, 0, time.Millisecond, time.Millisecond, 0)
	old := NewMaster(NewClock(func() int64 { return s.T }, 0), 7)
	if _, ok := s.follower.HandleSync(old.Sync()); ok {
		t.Fatal("follower took SYNC from a master it has not adopted")
	}
	m := Message{Type: MsgDelayResp, Seq: 9, Time: -5, Epoch: 3}
	got, err := Decode(m.Encode())
	if err != nil || got != m {
		t.Fatalf("round trip %+v %v", got, err)
	}
	for _, bad := range [][]byte{nil, make([]byte, ptpLen), append([]byte{TagPTP, 9}, make([]byte, 20)...)} {
		if _, err := Decode(bad); err == nil {
			t.Fatalf("decoded %x", bad)
		}
	}
	mc := NewClock(func() int64 { return 42 }, 0)
	mc.SetMaster(true)
	if iv := mc.Interval(); iv.Width() != 0 || iv.Earliest != 42 {
		t.Fatalf("grandmaster interval %+v", iv)
	}
	if w := CommitWait(mc, 50); w != 8 {
		t.Fatalf("commit wait %d", w)
	}
}

func TestElect(t *testing.T) {
	a, b, c := Default("a"), Default("b"), Default("c")
	if best, _ := Elect([]Dataset{c, b, a}); best.Identity != "a" {
		t.Fatalf("identity tie-break picked %s", best.Identity)
	}
	gps := Default("z")
	gps.Class = 6 // locked to a reference
	if best, _ := Elect([]Dataset{a, b, gps}); best.Identity != "z" {
		t.Fatal("a reference-locked clock should win")
	}
	pref := Default("y")
	pref.Priority1 = 1
	if best, _ := Elect([]Dataset{gps, pref}); best.Identity != "y" {
		t.Fatal("priority1 outranks clock class")
	}
}

func TestMatrixStability(t *testing.T) {
	m := NewMatrix("a")
	m.SetMembers([]string{"a", "b", "c"})
	m.Observe("a", "a", 5)
	m.MergeRow("b", map[string]uint64{"a": 3})
	m.MergeRow("c", map[string]uint64{"a": 4, "b": 1})
	if s := m.Stable("a"); s != 3 {
		t.Fatalf("stable(a) = %d, want 3 (b has only seen 3)", s)
	}
	m.MergeRow("b", map[string]uint64{"a": 2}) // stale: never moves backwards
	if s := m.Stable("a"); s != 3 {
		t.Fatalf("stale row rolled stability back: %d", s)
	}
	m.SetMembers([]string{"a", "c"}) // b leaves: it no longer holds GC back
	if s := m.Stable("a"); s != 4 {
		t.Fatalf("after b left, stable(a) = %d, want 4", s)
	}
}

// Three origins, three replicas, each fed the events in its own random
// interleaving (per-origin order kept): all release the same total order.
func TestSequencerDeterministicOrder(t *testing.T) {
	members := []string{"n1", "n2", "n3"}
	rng := rand.New(rand.NewPCG(5, 5))
	streams := map[string][]Event{}
	for _, m := range members {
		ts := hlc.Make(1000, 0)
		for i := 0; i < 50; i++ {
			next := hlc.Make(ts.Wall()+rng.Int64N(3), uint16(rng.IntN(3)))
			if next <= ts { // an origin's HLC readings strictly increase
				next = ts + 1
			}
			ts = next
			streams[m] = append(streams[m], Event{TS: ts, Origin: m, Payload: []byte(fmt.Sprintf("%s-%d", m, i))})
		}
	}
	var orders [][]string
	for r := 0; r < 3; r++ {
		s := NewSequencer(members)
		pos := map[string]int{}
		var order []string
		for done := 0; done < 150; {
			m := members[rng.IntN(3)]
			if pos[m] == len(streams[m]) {
				continue
			}
			if !s.Add(streams[m][pos[m]]) {
				t.Fatalf("replica %d rejected an in-order event", r)
			}
			pos[m]++
			done++
			for _, e := range s.Ready() {
				order = append(order, string(e.Payload))
			}
		}
		for _, m := range members {
			s.Watermark(m, hlc.MaxTimestamp-1) // everyone has finished
		}
		for _, e := range s.Ready() {
			order = append(order, string(e.Payload))
		}
		if len(order) != 150 {
			t.Fatalf("replica %d released %d of 150", r, len(order))
		}
		orders = append(orders, order)
	}
	if !reflect.DeepEqual(orders[0], orders[1]) || !reflect.DeepEqual(orders[1], orders[2]) {
		t.Fatal("replicas released different orders")
	}
	// Nothing may be inserted below what was released.
	s := NewSequencer([]string{"x"})
	s.Add(Event{TS: 10, Origin: "x"})
	s.Ready()
	if s.Add(Event{TS: 9, Origin: "x"}) {
		t.Fatal("accepted an event in the released past")
	}
}

func FuzzDecode(f *testing.F) {
	f.Add(Message{Type: MsgSync, Seq: 1, Time: 2, Epoch: 3}.Encode())
	f.Fuzz(func(t *testing.T, b []byte) {
		m, err := Decode(b)
		if err == nil {
			if again, err := Decode(m.Encode()); err != nil || again != m {
				t.Fatalf("round trip %+v -> %+v %v", m, again, err)
			}
		}
	})
}
