package hlc

import (
	"encoding/json"
	"errors"
	"flag"
	"math/rand/v2"
	"sort"
	"sync"
	"testing"
	"time"
)

var seedFlag = flag.Uint64("seed", 0, "replay a single randomized run with this seed")

// seeds returns the seeds a property test should run: the -seed flag alone if
// given, otherwise a fixed spread plus one fresh seed so CI keeps exploring.
func seeds(t *testing.T, n int) []uint64 {
	t.Helper()
	if *seedFlag != 0 {
		return []uint64{*seedFlag}
	}
	if testing.Short() && n > 8 {
		n = 8
	}
	out := make([]uint64, 0, n+1)
	for i := 1; i <= n; i++ {
		out = append(out, uint64(i))
	}
	return append(out, uint64(time.Now().UnixNano()))
}

// fakeClock is a jittery physical clock: it mostly moves forward, sometimes
// stalls, and sometimes steps backwards, like NTP slewing gone wrong.
type fakeClock struct {
	mu  sync.Mutex
	ms  int64
	rng *rand.Rand
}

func (f *fakeClock) now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return time.UnixMilli(f.ms)
}

func (f *fakeClock) jitter() {
	f.mu.Lock()
	defer f.mu.Unlock()
	switch r := f.rng.IntN(10); {
	case r < 7:
		f.ms += f.rng.Int64N(10)
	case r < 9:
		// stall
	default:
		f.ms -= f.rng.Int64N(20)
		if f.ms < 0 {
			f.ms = 0
		}
	}
}

func (f *fakeClock) get() int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.ms
}

func TestTimestampOrderMatchesLexicographic(t *testing.T) {
	for _, seed := range seeds(t, 4) {
		rng := rand.New(rand.NewPCG(seed, 1))
		for i := 0; i < 20000; i++ {
			w1, w2 := rng.Int64N(1<<20), rng.Int64N(1<<20)
			if rng.IntN(2) == 0 {
				w2 = w1
			}
			l1, l2 := uint16(rng.UintN(1<<16)), uint16(rng.UintN(1<<16))
			a, b := Make(w1, l1), Make(w2, l2)
			want := 0
			switch {
			case w1 < w2 || (w1 == w2 && l1 < l2):
				want = -1
			case w1 > w2 || (w1 == w2 && l1 > l2):
				want = 1
			}
			if got := a.Compare(b); got != want {
				t.Fatalf("seed %d: Compare(%v,%v)=%d want %d", seed, a, b, got, want)
			}
			if a.Wall() != w1 || a.Logical() != l1 {
				t.Fatalf("seed %d: parts of %v = (%d,%d) want (%d,%d)", seed, a, a.Wall(), a.Logical(), w1, l1)
			}
		}
	}
}

func TestStringRoundTrip(t *testing.T) {
	for _, seed := range seeds(t, 4) {
		rng := rand.New(rand.NewPCG(seed, 2))
		for i := 0; i < 5000; i++ {
			ts := Timestamp(rng.Uint64())
			got, err := Parse(ts.String())
			if err != nil || got != ts {
				t.Fatalf("seed %d: Parse(%q) = %v, %v; want %v", seed, ts.String(), got, err, ts)
			}
			b, _ := json.Marshal(struct{ T Timestamp }{ts})
			var back struct{ T Timestamp }
			if err := json.Unmarshal(b, &back); err != nil || back.T != ts {
				t.Fatalf("seed %d: json round trip of %v via %s = %v, %v", seed, ts, b, back.T, err)
			}
		}
	}
	for _, bad := range []string{"", ".", "1.", ".1", "x", "1.x", "1.65536", "-1.0", "281474976710656.0", "1.2.3"} {
		if _, err := Parse(bad); !errors.Is(err, ErrSyntax) {
			t.Errorf("Parse(%q) err = %v, want ErrSyntax", bad, err)
		}
	}
	if ts, err := Parse("42"); err != nil || ts != Make(42, 0) {
		t.Errorf("Parse(\"42\") = %v, %v", ts, err)
	}
}

func TestNowTracksPhysicalTime(t *testing.T) {
	ms := int64(1_700_000_000_000)
	c := New(func() time.Time { return time.UnixMilli(ms) }, time.Second)
	if got := c.Now(); got != Make(ms, 0) {
		t.Fatalf("first Now = %v, want %v", got, Make(ms, 0))
	}
	if got := c.Now(); got != Make(ms, 1) {
		t.Fatalf("second Now in same ms = %v, want %v", got, Make(ms, 1))
	}
	ms += 10
	if got := c.Now(); got != Make(ms, 0) {
		t.Fatalf("Now after physical advance = %v, want %v (logical resets)", got, Make(ms, 0))
	}
	ms -= 5 // clock steps back: HLC must not
	if got := c.Now(); got != Make(ms+5, 1) {
		t.Fatalf("Now after backwards step = %v, want %v", got, Make(ms+5, 1))
	}
}

func TestLogicalOverflowSpillsIntoWall(t *testing.T) {
	const ms = 1000
	c := New(func() time.Time { return time.UnixMilli(ms) }, 0)
	prev := c.Now()
	for i := 0; i < MaxLogical+10; i++ {
		ts := c.Now()
		if ts <= prev {
			t.Fatalf("not monotonic at i=%d: %v after %v", i, ts, prev)
		}
		prev = ts
	}
	if prev.Wall() != ms+1 {
		t.Fatalf("after %d events in one ms wall = %d, want %d (carry)", MaxLogical+11, prev.Wall(), ms+1)
	}
	// MaxLogical+10 increments from (ms, 0) land on (ms+1, 9).
	if prev.Logical() != 9 {
		t.Fatalf("logical after carry = %d, want 9", prev.Logical())
	}
	// Update with a remote at the edge of the counter carries too.
	r := Make(ms+1, MaxLogical)
	got, err := c.Update(r)
	if err != nil || got != Make(ms+2, 0) || got <= r {
		t.Fatalf("Update(%v) = %v, %v; want %v", r, got, err, Make(ms+2, 0))
	}
}

func TestUpdateRejectsFarFuture(t *testing.T) {
	ms := int64(1_000_000)
	c := New(func() time.Time { return time.UnixMilli(ms) }, 500*time.Millisecond)
	before := c.Now()
	_, err := c.Update(Make(ms+501, 0))
	if !errors.Is(err, ErrClockOffset) {
		t.Fatalf("Update 501ms ahead: err = %v, want ErrClockOffset", err)
	}
	var oe *OffsetError
	if !errors.As(err, &oe) || oe.Physical != ms || oe.Error() == "" {
		t.Fatalf("error %v not an *OffsetError with physical %d", err, ms)
	}
	if c.Last() != before {
		t.Fatalf("rejected update changed the clock: %v -> %v", before, c.Last())
	}
	if got, err := c.Update(Make(ms+500, 7)); err != nil || got != Make(ms+500, 8) {
		t.Fatalf("Update exactly at max offset = %v, %v; want %v", got, err, Make(ms+500, 8))
	}
	// Disabled check: anything goes, except wrapping.
	d := New(func() time.Time { return time.UnixMilli(ms) }, 0)
	if _, err := d.Update(Make(MaxWall, 3)); err != nil {
		t.Fatalf("disabled check rejected: %v", err)
	}
	if _, err := d.Update(MaxTimestamp); !errors.Is(err, ErrOverflow) {
		t.Fatalf("Update(MaxTimestamp) err = %v, want ErrOverflow", err)
	}
}

// TestPropertyMonotonicSingleClock drives one clock with a random interleaving
// of local events and receives against a jittery physical clock and checks:
//   - every result is strictly greater than the previous one;
//   - an accepted Update result is strictly greater than the remote;
//   - rejected updates are exactly those beyond MaxOffset and change nothing;
//   - the wall part never exceeds the largest physical or accepted remote wall
//     time seen, except by the carry slack of logical overflow (none here).
func TestPropertyMonotonicSingleClock(t *testing.T) {
	const maxOff = 100 * time.Millisecond
	for _, seed := range seeds(t, 32) {
		rng := rand.New(rand.NewPCG(seed, 3))
		fc := &fakeClock{ms: 1_000_000, rng: rand.New(rand.NewPCG(seed, 4))}
		c := New(fc.now, maxOff)
		var prev Timestamp
		var bound int64 // max physical or accepted remote wall seen
		for i := 0; i < 5000; i++ {
			fc.jitter()
			phys := fc.get()
			if phys > bound {
				bound = phys
			}
			if rng.IntN(2) == 0 {
				ts := c.Now()
				if ts <= prev {
					t.Fatalf("seed %d step %d: Now %v not after %v", seed, i, ts, prev)
				}
				prev = ts
			} else {
				remote := Make(phys+rng.Int64N(300)-150, uint16(rng.UintN(1<<16)))
				ts, err := c.Update(remote)
				tooFar := remote.Wall()-phys > maxOff.Milliseconds()
				if tooFar {
					if !errors.Is(err, ErrClockOffset) {
						t.Fatalf("seed %d step %d: remote %v (phys %d) accepted", seed, i, remote, phys)
					}
					if c.Last() != prev {
						t.Fatalf("seed %d step %d: rejected update moved clock", seed, i)
					}
				} else {
					if err != nil {
						t.Fatalf("seed %d step %d: Update(%v) phys %d: %v", seed, i, remote, phys, err)
					}
					if ts <= prev || ts <= remote {
						t.Fatalf("seed %d step %d: Update(%v)=%v not after prev %v and remote", seed, i, remote, ts, prev)
					}
					if remote.Wall() > bound {
						bound = remote.Wall()
					}
					prev = ts
				}
			}
			if prev.Wall() > bound+1 {
				t.Fatalf("seed %d step %d: wall %d ran ahead of bound %d", seed, i, prev.Wall(), bound)
			}
		}
	}
}

// TestPropertyCausalityAcrossNodes simulates several nodes with skewed,
// jittery clocks exchanging messages in random order. Every receive event must
// be ordered after its send event and after everything the receiver did
// before, and drift from the fastest physical clock stays bounded.
func TestPropertyCausalityAcrossNodes(t *testing.T) {
	const nodes = 5
	const maxOff = 250 * time.Millisecond
	type msg struct {
		to int
		ts Timestamp
	}
	for _, seed := range seeds(t, 32) {
		rng := rand.New(rand.NewPCG(seed, 5))
		clocks := make([]*Clock, nodes)
		phys := make([]*fakeClock, nodes)
		last := make([]Timestamp, nodes)
		for i := range clocks {
			phys[i] = &fakeClock{ms: 1_000_000 + rng.Int64N(200), rng: rand.New(rand.NewPCG(seed, uint64(100+i)))}
			clocks[i] = New(phys[i].now, maxOff)
		}
		var inflight []msg
		var histMaxPhys int64
		for step := 0; step < 4000; step++ {
			n := rng.IntN(nodes)
			// All physical clocks advance together on average; skew stays
			// within a couple of hundred ms because of the jitter model.
			for _, p := range phys {
				p.jitter()
			}
			switch {
			case len(inflight) > 0 && rng.IntN(3) == 0:
				k := rng.IntN(len(inflight))
				m := inflight[k]
				inflight = append(inflight[:k], inflight[k+1:]...)
				ts, err := clocks[m.to].Update(m.ts)
				if err != nil {
					// Skew can exceed maxOff under this jitter model;
					// the rejection itself is checked elsewhere.
					continue
				}
				if ts <= m.ts || ts <= last[m.to] {
					t.Fatalf("seed %d step %d: receive %v at node %d not after send %v / prev %v", seed, step, ts, m.to, m.ts, last[m.to])
				}
				last[m.to] = ts
			default:
				ts := clocks[n].Now()
				if ts <= last[n] {
					t.Fatalf("seed %d step %d: node %d Now %v not after %v", seed, step, n, ts, last[n])
				}
				last[n] = ts
				inflight = append(inflight, msg{to: rng.IntN(nodes), ts: ts})
			}
			for _, p := range phys {
				if v := p.get(); v > histMaxPhys {
					histMaxPhys = v
				}
			}
			for i, l := range last {
				// A clock only ever adopts physical readings and remotes that
				// were within maxOff of the receiver's physical clock, so by
				// induction no wall part exceeds the largest physical reading
				// ever taken plus maxOff (plus one ms of carry slack).
				if l.Wall() > histMaxPhys+maxOff.Milliseconds()+1 {
					t.Fatalf("seed %d: node %d wall %d drifted past %d+%s", seed, i, l.Wall(), histMaxPhys, maxOff)
				}
			}
		}
	}
}

// TestConcurrentUse hammers one clock from many goroutines; every issued
// timestamp must be unique (strict monotonicity under the mutex) and the race
// detector must stay quiet.
func TestConcurrentUse(t *testing.T) {
	fc := &fakeClock{ms: 5_000, rng: rand.New(rand.NewPCG(7, 7))}
	c := New(fc.now, time.Second)
	const workers, per = 8, 2000
	results := make([][]Timestamp, workers)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(uint64(w), 9))
			prev := Timestamp(0)
			for i := 0; i < per; i++ {
				var ts Timestamp
				if rng.IntN(3) == 0 {
					fc.jitter()
					var err error
					ts, err = c.Update(Make(fc.get()+rng.Int64N(100), 0))
					if err != nil {
						t.Errorf("update: %v", err)
						return
					}
				} else {
					ts = c.Now()
				}
				if ts <= prev {
					t.Errorf("worker %d: %v not after %v", w, ts, prev)
					return
				}
				prev = ts
				results[w] = append(results[w], ts)
			}
		}(w)
	}
	wg.Wait()
	var all []Timestamp
	for _, r := range results {
		all = append(all, r...)
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	for i := 1; i < len(all); i++ {
		if all[i] == all[i-1] {
			t.Fatalf("duplicate timestamp %v issued", all[i])
		}
	}
}

func FuzzParse(f *testing.F) {
	for _, s := range []string{"0.0", "1729850000123.7", "42", "281474976710655.65535", "", "1.", "x.y"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		ts, err := Parse(s)
		if err != nil {
			return
		}
		back, err := Parse(ts.String())
		if err != nil || back != ts {
			t.Fatalf("Parse(%q)=%v but String round trip gives %v, %v", s, ts, back, err)
		}
	})
}
