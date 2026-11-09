package causal

import (
	"container/heap"
	"fmt"
	"math/rand/v2"
	"testing"
)

// net is a deterministic lossy, duplicating, reordering network.
type packet struct {
	at       int
	seq      int
	from, to string
	b        []byte
}
type pq []packet

func (q pq) Len() int { return len(q) }
func (q pq) Less(i, j int) bool {
	if q[i].at != q[j].at {
		return q[i].at < q[j].at
	}
	return q[i].seq < q[j].seq
}
func (q pq) Swap(i, j int) { q[i], q[j] = q[j], q[i] }
func (q *pq) Push(x any)   { *q = append(*q, x.(packet)) }
func (q *pq) Pop() any {
	o := *q
	p := o[len(o)-1]
	*q = o[:len(o)-1]
	return p
}

type cluster struct {
	t     *testing.T
	rng   *rand.Rand
	nodes map[string]*Node
	names []string
	q     pq
	now   int
	seq   int
	loss  float64
	dup   float64
	cut   map[string]bool // partitioned-off nodes (both directions)
	// per-node record of deliveries, to check order
	seen map[string]VC
	got  map[string]map[string]bool
}

func newCluster(t *testing.T, seed uint64, n int) *cluster {
	c := &cluster{t: t, rng: rand.New(rand.NewPCG(seed, 3)), nodes: map[string]*Node{}, cut: map[string]bool{},
		seen: map[string]VC{}, got: map[string]map[string]bool{}}
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("n%d", i)
		c.names = append(c.names, name)
		c.nodes[name] = New(Config{Self: name, Rand: rand.New(rand.NewPCG(seed, uint64(i)))})
		c.seen[name] = VC{}
		c.got[name] = map[string]bool{}
	}
	for _, nd := range c.nodes {
		nd.SetPeers(c.names)
	}
	return c
}

func (c *cluster) record(at string, d Delivery) {
	prev := c.seen[at]
	if d.Seq != prev[d.Origin]+1 {
		c.t.Fatalf("%s delivered %s#%d after %s#%d (gap or repeat)", at, d.Origin, d.Seq, d.Origin, prev[d.Origin])
	}
	for o, s := range d.Deps {
		if s > prev[o] {
			c.t.Fatalf("%s delivered %s#%d before its dependency %s#%d", at, d.Origin, d.Seq, o, s)
		}
	}
	prev[d.Origin] = d.Seq
	id := fmt.Sprintf("%s#%d", d.Origin, d.Seq)
	if c.got[at][id] {
		c.t.Fatalf("%s delivered %s twice", at, id)
	}
	c.got[at][id] = true
}

func (c *cluster) flush(name string) {
	nd := c.nodes[name]
	for _, d := range nd.Deliveries() {
		c.record(name, d)
	}
	for _, o := range nd.Outbox() {
		if c.cut[name] || c.cut[o.To] || c.rng.Float64() < c.loss {
			continue
		}
		copies := 1
		if c.rng.Float64() < c.dup {
			copies = 2
		}
		for i := 0; i < copies; i++ {
			c.seq++
			heap.Push(&c.q, packet{at: c.now + 1 + c.rng.IntN(20), seq: c.seq, from: name, to: o.To, b: o.Payload})
		}
	}
}

func (c *cluster) broadcast(name string) {
	d, err := c.nodes[name].Broadcast([]byte(fmt.Sprintf("%s says %d", name, c.seq)))
	if err != nil {
		c.t.Fatal(err)
	}
	c.record(name, d)
	c.flush(name)
}

// step advances one time unit: deliver due packets, maybe tick.
func (c *cluster) step(tickEvery int) {
	c.now++
	for len(c.q) > 0 && c.q[0].at <= c.now {
		p := heap.Pop(&c.q).(packet)
		if err := c.nodes[p.to].Handle(p.from, p.b); err != nil {
			c.t.Fatal(err)
		}
		c.flush(p.to)
	}
	if tickEvery > 0 && c.now%tickEvery == 0 {
		for _, name := range c.names {
			c.nodes[name].Tick()
			c.flush(name)
		}
	}
}

func (c *cluster) converged(total int) bool {
	for _, name := range c.names {
		if len(c.got[name]) != total {
			return false
		}
	}
	return true
}

func TestCausalDeliveryUnderChaos(t *testing.T) {
	for seed := uint64(1); seed <= 20; seed++ {
		c := newCluster(t, seed, 5)
		c.loss, c.dup = 0.2, 0.1
		total := 0
		for i := 0; i < 400; i++ {
			if c.rng.IntN(3) == 0 {
				c.broadcast(c.names[c.rng.IntN(5)])
				total++
			}
			c.step(25)
		}
		// Heal: no more loss; anti-entropy finishes the job.
		c.loss, c.dup = 0, 0
		for i := 0; i < 2000 && !(c.converged(total) && len(c.q) == 0); i++ {
			c.step(10)
		}
		if !c.converged(total) {
			for _, n := range c.names {
				t.Logf("%s: %d of %d, %+v", n, len(c.got[n]), total, c.nodes[n].Stats())
			}
			t.Fatalf("seed %d: not converged", seed)
		}
		// Once everything is stable, the retransmission logs empty.
		for i := 0; i < 50; i++ {
			c.step(5)
		}
		for _, n := range c.names {
			st := c.nodes[n].Stats()
			if st.Logged != 0 || st.Buffered != 0 {
				t.Fatalf("seed %d: %s still holds %d logged, %d buffered", seed, n, st.Logged, st.Buffered)
			}
		}
	}
}

// A partitioned node keeps its own messages, buffers nothing it cannot
// place, and catches up -- in causal order -- when the partition heals.
func TestPartitionHeals(t *testing.T) {
	c := newCluster(t, 7, 4)
	c.cut["n3"] = true
	total := 0
	for i := 0; i < 200; i++ {
		if i%4 == 0 {
			c.broadcast(c.names[i%4])
			total++
		}
		if i%9 == 0 {
			c.broadcast("n3")
			total++
		}
		c.step(10)
	}
	if len(c.got["n3"]) == total {
		t.Fatal("the partition was not a partition")
	}
	delete(c.cut, "n3")
	for i := 0; i < 3000 && !c.converged(total); i++ {
		c.step(5)
	}
	if !c.converged(total) {
		t.Fatalf("not converged after healing: n3 has %d of %d", len(c.got["n3"]), total)
	}
}

func TestFastForward(t *testing.T) {
	a := New(Config{Self: "a"})
	a.SetPeers([]string{"a", "b"})
	b := New(Config{Self: "b"})
	b.SetPeers([]string{"a", "b"})
	var msgs [][]byte
	for i := 0; i < 3; i++ {
		a.Broadcast([]byte{byte(i)})
		for _, o := range a.Outbox() {
			msgs = append(msgs, o.Payload)
		}
	}
	// b receives only the third message, then a snapshot covering 1..2.
	b.Handle("a", msgs[2])
	if len(b.Deliveries()) != 0 {
		t.Fatal("delivered past a gap")
	}
	b.FastForward(VC{"a": 2})
	ds := b.Deliveries()
	if len(ds) != 1 || ds[0].Seq != 3 {
		t.Fatalf("after fast-forward: %+v", ds)
	}
}

func FuzzHandle(f *testing.F) {
	f.Add(encodeData(Delivery{Origin: "x", Seq: 1, Deps: VC{"y": 2}, Payload: []byte("p")}))
	f.Add(encodeDigest("x", VC{"x": 3}))
	f.Fuzz(func(t *testing.T, b []byte) {
		n := New(Config{Self: "me"})
		n.SetPeers([]string{"x", "y"})
		n.Handle("x", b)
		n.Tick()
	})
}
