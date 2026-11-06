package swim

import (
	"container/heap"
	"fmt"
	"hash/fnv"
	"math/rand/v2"
	"sort"
	"testing"
	"time"
)

// sim is a deterministic in-memory network of SWIM nodes on virtual time.
type sim struct {
	t       *testing.T
	now     time.Time
	rng     *rand.Rand
	nodes   map[string]*Node
	crashed map[string]bool
	cut     func(from, to string) bool
	loss    float64
	latency time.Duration
	q       dq
	seq     uint64
	trace   []string
	maxSize int
	cfg     func(name string) Config
}

type dmsg struct {
	at       time.Time
	seq      uint64
	from, to string
	payload  []byte
}
type dq []dmsg

func (h dq) Len() int { return len(h) }
func (h dq) Less(i, j int) bool {
	if !h[i].at.Equal(h[j].at) {
		return h[i].at.Before(h[j].at)
	}
	return h[i].seq < h[j].seq
}
func (h dq) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *dq) Push(x any)   { *h = append(*h, x.(dmsg)) }
func (h *dq) Pop() any     { o := *h; x := o[len(o)-1]; *h = o[:len(o)-1]; return x }

func newSim(t *testing.T, seed uint64) *sim {
	return &sim{t: t, now: time.Unix(1_700_000_000, 0), rng: rand.New(rand.NewPCG(seed, 9)),
		nodes: map[string]*Node{}, crashed: map[string]bool{}, latency: 5 * time.Millisecond}
}

func (s *sim) add(name string, seeds ...string) *Node {
	cfg := Config{Name: name, Addr: name, Meta: map[string]string{"role": "test"},
		Rand: rand.New(rand.NewPCG(s.rng.Uint64(), 1))}
	if s.cfg != nil {
		c := s.cfg(name)
		c.Name, c.Addr, c.Rand = name, name, cfg.Rand
		cfg = c
	}
	n := New(cfg, s.now)
	s.nodes[name] = n
	delete(s.crashed, name)
	n.Join(seeds, s.now)
	s.collect(name)
	return n
}

func (s *sim) collect(name string) {
	n := s.nodes[name]
	for _, m := range n.Outbox() {
		if s.maxSize > 0 && len(m.Payload) > s.maxSize {
			s.t.Fatalf("%s sent a %d-byte message (limit %d)", name, len(m.Payload), s.maxSize)
		}
		if s.crashed[name] || s.crashed[m.To] || (s.cut != nil && s.cut(name, m.To)) {
			continue
		}
		if s.loss > 0 && s.rng.Float64() < s.loss {
			continue
		}
		s.seq++
		d := s.latency + time.Duration(s.rng.Int64N(int64(s.latency)+1))
		heap.Push(&s.q, dmsg{at: s.now.Add(d), seq: s.seq, from: name, to: m.To, payload: m.Payload})
	}
	for _, e := range n.Events() {
		s.trace = append(s.trace, fmt.Sprintf("%d %s %s %s/%d", s.now.UnixMilli(), name, e.Kind, e.Member.Name, e.Member.Incarnation))
	}
}

func (s *sim) names() []string {
	var out []string
	for n := range s.nodes {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

func (s *sim) run(d time.Duration) {
	end := s.now.Add(d)
	for {
		next := end
		if len(s.q) > 0 && s.q[0].at.Before(next) {
			next = s.q[0].at
		}
		for _, name := range s.names() {
			if !s.crashed[name] {
				if dl := s.nodes[name].NextDeadline(); dl.Before(next) {
					next = dl
				}
			}
		}
		if next.After(s.now) {
			s.now = next
		}
		for len(s.q) > 0 && !s.q[0].at.After(s.now) {
			m := heap.Pop(&s.q).(dmsg)
			if s.crashed[m.to] || (s.cut != nil && s.cut(m.from, m.to)) {
				continue
			}
			if n := s.nodes[m.to]; n != nil {
				n.Handle(m.from, m.payload, s.now)
				s.collect(m.to)
			}
		}
		for _, name := range s.names() {
			if !s.crashed[name] && !s.nodes[name].NextDeadline().After(s.now) {
				s.nodes[name].Tick(s.now)
				s.collect(name)
			}
		}
		if !s.now.Before(end) {
			return
		}
	}
}

// view returns name → state as seen by observer.
func (s *sim) view(observer string) map[string]State {
	out := map[string]State{}
	for _, m := range s.nodes[observer].Members() {
		out[m.Name] = m.State
	}
	return out
}

// allSee reports whether every observer holds name in state want. A dead or
// left member that has been reclaimed (forgotten after DeadReclaim) counts.
func (s *sim) allSee(name string, want State, among []string) bool {
	for _, o := range among {
		st, ok := s.view(o)[name]
		if !ok && (want == StateDead || want == StateLeft) {
			continue
		}
		if !ok || st != want {
			return false
		}
	}
	return true
}

func (s *sim) live() []string {
	var out []string
	for _, n := range s.names() {
		if !s.crashed[n] {
			out = append(out, n)
		}
	}
	return out
}

func (s *sim) converged() bool {
	live := s.live()
	for _, o := range live {
		v := s.view(o)
		for _, n := range live {
			if st, ok := v[n]; !ok || st != StateAlive {
				return false
			}
		}
	}
	return true
}

func cluster(t *testing.T, seed uint64, n int) *sim {
	s := newSim(t, seed)
	s.add("n00")
	for i := 1; i < n; i++ {
		s.add(fmt.Sprintf("n%02d", i), "n00")
	}
	return s
}

func TestJoinConverges(t *testing.T) {
	for _, n := range []int{5, 16, 32} {
		s := cluster(t, uint64(n), n)
		s.maxSize = 1200
		s.run(20 * time.Second)
		if !s.converged() {
			t.Fatalf("n=%d: not converged: n00 sees %v", n, s.view("n00"))
		}
	}
}

func TestCrashedNodeIsDeclaredDeadEverywhere(t *testing.T) {
	for seed := uint64(1); seed <= 10; seed++ {
		s := cluster(t, seed, 12)
		s.run(15 * time.Second)
		s.crashed["n05"] = true
		// Bound: a few protocol periods to suspect plus the max suspicion
		// timeout (SuspicionMaxMult × SuspicionMult × log10(n) × interval).
		s.run(40 * time.Second)
		if !s.allSee("n05", StateDead, s.live()) {
			t.Fatalf("seed %d: n05 not dead everywhere", seed)
		}
		if !s.converged() {
			t.Fatalf("seed %d: false positive among live nodes: n00 sees %v", seed, s.view("n00"))
		}
	}
}

func TestSuspectedNodeRefutes(t *testing.T) {
	s := cluster(t, 3, 8)
	s.run(15 * time.Second)
	inc := s.nodes["n03"].Incarnation()
	// n03 cannot send for 2.5s: it misses acks and gets suspected...
	s.cut = func(from, to string) bool { return from == "n03" }
	s.run(2500 * time.Millisecond)
	s.cut = nil
	// ...but recovers well inside the suspicion timeout and refutes.
	s.run(20 * time.Second)
	if !s.converged() {
		t.Fatalf("n03 not alive everywhere after refuting: n00 sees %v", s.view("n00"))
	}
	if s.nodes["n03"].Incarnation() <= inc {
		t.Fatal("n03 never bumped its incarnation to refute")
	}
}

func TestDeadNodeRejoinsWithHigherIncarnation(t *testing.T) {
	s := cluster(t, 4, 8)
	s.run(15 * time.Second)
	s.crashed["n02"] = true
	s.run(40 * time.Second)
	if !s.allSee("n02", StateDead, s.live()) {
		t.Fatal("precondition: n02 dead")
	}
	s.add("n02", "n00") // restart: fresh state, incarnation 0
	s.run(20 * time.Second)
	if !s.converged() {
		t.Fatalf("restarted n02 not alive everywhere: n01 sees %v", s.view("n01"))
	}
}

func TestGracefulLeave(t *testing.T) {
	s := cluster(t, 5, 8)
	s.run(15 * time.Second)
	s.nodes["n04"].Leave(s.now)
	s.collect("n04")
	s.run(time.Second)
	s.crashed["n04"] = true
	s.run(5 * time.Second)
	if !s.allSee("n04", StateLeft, s.live()) {
		t.Fatalf("n04 not left everywhere: %v", s.view("n00"))
	}
}

func TestPartitionHeals(t *testing.T) {
	s := cluster(t, 6, 10)
	s.run(15 * time.Second)
	group := func(n string) int {
		if n < "n05" {
			return 0
		}
		return 1
	}
	s.cut = func(a, b string) bool { return group(a) != group(b) }
	s.run(90 * time.Second) // long enough for each side to declare the other dead
	if s.view("n00")["n07"] != StateDead && s.view("n00")["n07"] != StateSuspect {
		if _, known := s.view("n00")["n07"]; known {
			t.Fatalf("partition never detected: n00 sees n07 as %v", s.view("n00")["n07"])
		}
	}
	s.cut = nil
	s.run(3 * time.Minute) // push-pull (every ~30s) reconnects the halves
	if !s.converged() {
		t.Fatalf("partition did not heal: n00 sees %v, n09 sees %v", s.view("n00"), s.view("n09"))
	}
}

func TestLossyNetworkNoFalseDeaths(t *testing.T) {
	s := cluster(t, 7, 10)
	s.run(15 * time.Second)
	s.loss = 0.10
	deaths := 0
	s.run(10 * time.Minute)
	for _, line := range s.trace {
		var at int64
		var who, kind, subject string
		fmt.Sscanf(line, "%d %s %s %s", &at, &who, &kind, &subject)
		if kind == "dead" {
			deaths++
		}
	}
	if deaths > 0 {
		t.Fatalf("%d false death declarations at 10%% loss", deaths)
	}
	if !s.converged() {
		t.Fatal("not converged after a lossy period")
	}
}

func TestMetadataPropagates(t *testing.T) {
	s := cluster(t, 8, 6)
	s.run(15 * time.Second)
	s.nodes["n02"].UpdateMeta(map[string]string{"role": "leader"}, s.now)
	s.run(5 * time.Second)
	for _, o := range s.live() {
		m, _ := s.nodes[o].Member("n02")
		if m.Meta["role"] != "leader" {
			t.Fatalf("%s sees n02 meta %v", o, m.Meta)
		}
	}
}

func TestDeterministicReplay(t *testing.T) {
	run := func() uint64 {
		s := cluster(t, 42, 10)
		s.loss = 0.05
		s.run(20 * time.Second)
		s.crashed["n03"] = true
		s.run(30 * time.Second)
		h := fnv.New64a()
		for _, l := range s.trace {
			h.Write([]byte(l))
		}
		return h.Sum64()
	}
	if a, b := run(), run(); a != b {
		t.Fatalf("same seed, different traces: %x vs %x", a, b)
	}
}

func TestCodecRoundTrip(t *testing.T) {
	m := message{typ: msgPushPull, from: "a", addr: "a:1", seq: 7, join: true,
		updates: []update{{name: "b", addr: "b:2", state: StateSuspect, incarnation: 3, meta: map[string]string{"k": "v"}}}}
	got, err := decode(m.encode())
	if err != nil || got.from != "a" || !got.join || len(got.updates) != 1 || got.updates[0].meta["k"] != "v" {
		t.Fatalf("%+v %v", got, err)
	}
}

func FuzzHandle(f *testing.F) {
	f.Add((&message{typ: msgPing, from: "x", addr: "x", seq: 1, target: "a"}).encode())
	f.Add((&message{typ: msgPushPull, from: "x", addr: "x", join: true,
		updates: []update{{name: "a", state: StateDead, incarnation: 9}}}).encode())
	f.Fuzz(func(t *testing.T, b []byte) {
		now := time.Unix(0, 0)
		n := New(Config{Name: "a", Addr: "a"}, now)
		n.Handle("x", b, now)
		n.Tick(now.Add(time.Hour))
		for _, m := range n.Members() {
			if m.Name == "" {
				t.Fatal("empty member name")
			}
		}
		if self, ok := n.Member("a"); !ok || self.State != StateAlive {
			t.Fatal("node no longer believes it is alive")
		}
	})
}
