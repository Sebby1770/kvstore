package simnet

import (
	"bytes"
	"encoding/binary"
	"errors"
	"flag"
	"math"
	"math/bits"
	"math/rand/v2"
	"net"
	"os"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

var seedFlag = flag.Int64("seed", 0, "replay a single randomized run with this seed")

func seeds(t *testing.T, n int) []int64 {
	t.Helper()
	if *seedFlag != 0 {
		return []int64{*seedFlag}
	}
	if testing.Short() && n > 6 {
		n = 6
	}
	out := make([]int64, 0, n+1)
	for i := 1; i <= n; i++ {
		out = append(out, int64(i))
	}
	return append(out, time.Now().UnixNano())
}

func listen(t *testing.T, n *Network, addr string) *PacketConn {
	t.Helper()
	c, err := n.Listen(addr)
	if err != nil {
		t.Fatalf("Listen(%q): %v", addr, err)
	}
	return c
}

func send(t *testing.T, c *PacketConn, to string, b []byte) {
	t.Helper()
	if _, err := c.WriteTo(b, Addr(to)); err != nil {
		t.Fatalf("WriteTo(%s): %v", to, err)
	}
}

type rx struct {
	from string
	data []byte
	at   time.Time
}

// drain reads until no packet arrives for quiet. Only meaningful inside a
// synctest bubble, where waiting costs no real time.
func drain(t *testing.T, c *PacketConn, quiet time.Duration) []rx {
	t.Helper()
	var out []rx
	buf := make([]byte, 64<<10)
	for {
		_ = c.SetReadDeadline(time.Now().Add(quiet))
		n, from, err := c.ReadFrom(buf)
		if err != nil {
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				_ = c.SetReadDeadline(time.Time{})
				return out
			}
			t.Fatalf("ReadFrom: %v", err)
		}
		out = append(out, rx{from: from.String(), data: append([]byte(nil), buf[:n]...), at: time.Now()})
	}
}

func u64(i uint64) []byte { return binary.BigEndian.AppendUint64(nil, i) }

func TestLatencyAndJitterRespected(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const lat, jit = 30 * time.Millisecond, 10 * time.Millisecond
		n := New(1)
		n.SetDefaultLink(Link{Latency: lat, Jitter: jit})
		a, b := listen(t, n, "a:1"), listen(t, n, "b:1")
		const count = 300
		go func() {
			rng := rand.New(rand.NewPCG(1, 1))
			for i := 0; i < count; i++ {
				send(t, a, "b:1", u64(uint64(time.Now().UnixNano())))
				time.Sleep(time.Duration(rng.IntN(5)) * time.Millisecond)
			}
		}()
		got := drain(t, b, time.Second)
		if len(got) != count {
			t.Fatalf("received %d of %d", len(got), count)
		}
		var sawJitter bool
		for _, r := range got {
			sent := time.Unix(0, int64(binary.BigEndian.Uint64(r.data)))
			d := r.at.Sub(sent)
			if d < lat || d > lat+jit {
				t.Fatalf("one-way delay %v outside [%v, %v]", d, lat, lat+jit)
			}
			if d != lat {
				sawJitter = true
			}
			if r.from != "a:1" {
				t.Fatalf("from = %q", r.from)
			}
		}
		if !sawJitter {
			t.Fatal("jitter never applied")
		}
	})
}

func TestFIFOWithoutJitterOrReorder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		n := New(2)
		n.SetDefaultLink(Link{Latency: 5 * time.Millisecond, Loss: 0.1, Duplicate: 0.1})
		a, b := listen(t, n, "a"), listen(t, n, "b")
		// A burst at a single virtual instant: every packet is due at the
		// same moment and must still come out in send order.
		for i := 0; i < 500; i++ {
			send(t, a, "b", u64(uint64(i)))
		}
		got := drain(t, b, time.Second)
		last := -1
		for _, r := range got {
			v := int(binary.BigEndian.Uint64(r.data))
			if v < last {
				t.Fatalf("packet %d arrived after %d", v, last)
			}
			last = v
		}
		if s := n.Stats(); s.Reordered != 0 {
			t.Fatalf("reordered = %d on a FIFO link", s.Reordered)
		}
	})
}

// TestFaultRatesStatistical checks that loss, duplication and corruption
// happen at the configured rates, within 5 standard deviations.
func TestFaultRatesStatistical(t *testing.T) {
	for _, seed := range seeds(t, 6) {
		synctest.Test(t, func(t *testing.T) {
			const total = 20000
			const loss, dup, corrupt = 0.2, 0.05, 0.1
			n := New(seed)
			n.SetQueueLimit(4 * total)
			n.SetDefaultLink(Link{Latency: time.Millisecond, Loss: loss, Duplicate: dup, Corrupt: corrupt})
			a, b := listen(t, n, "a"), listen(t, n, "b")
			payload := make([]byte, 64)
			for i := 0; i < total; i++ {
				send(t, a, "b", payload)
			}
			time.Sleep(time.Second)
			s := n.Stats()
			if s.Sent != total || uint64(b.Queued()) != s.Delivered {
				t.Fatalf("seed %d: stats %v, queued %d", seed, s, b.Queued())
			}
			check := func(name string, got uint64, trials float64, p float64) {
				sigma := math.Sqrt(trials * p * (1 - p))
				if d := math.Abs(float64(got) - trials*p); d > 5*sigma {
					t.Errorf("seed %d: %s = %d, want %.0f ± %.0f", seed, name, got, trials*p, 5*sigma)
				}
			}
			check("loss", s.DroppedLoss, total, loss)
			survivors := float64(total) - float64(s.DroppedLoss)
			check("duplicates", s.Duplicated, survivors, dup)
			check("corrupt", s.Corrupted, survivors, corrupt)
			if s.Delivered != uint64(survivors)+s.Duplicated {
				t.Errorf("seed %d: delivered %d != survivors %v + dups %d", seed, s.Delivered, survivors, s.Duplicated)
			}
		})
	}
}

func TestFatePureAndBounded(t *testing.T) {
	for _, seed := range seeds(t, 10) {
		rng := rand.New(rand.NewPCG(uint64(seed), 77))
		for trial := 0; trial < 50; trial++ {
			l := Link{
				Latency:   time.Duration(rng.IntN(100)) * time.Millisecond,
				Jitter:    time.Duration(rng.IntN(20)) * time.Millisecond,
				Loss:      rng.Float64() * 0.5,
				Duplicate: rng.Float64() * 0.5,
				Reorder:   rng.Float64() * 0.5,
				Corrupt:   rng.Float64() * 0.5,
				MTU:       500 + rng.IntN(1000),
			}
			r1 := rand.New(rand.NewPCG(uint64(seed), uint64(trial)))
			r2 := rand.New(rand.NewPCG(uint64(seed), uint64(trial)))
			hold := l.Latency + l.Jitter
			if hold < time.Millisecond {
				hold = time.Millisecond
			}
			maxDelay := l.Latency + l.Jitter + hold/2 + hold
			for i := 0; i < 200; i++ {
				size := rng.IntN(2000)
				c1, x1 := l.Fate(r1, size)
				c2, x2 := l.Fate(r2, size)
				if x1 != x2 || len(c1) != len(c2) {
					t.Fatalf("seed %d: Fate not deterministic", seed)
				}
				for j := range c1 {
					if c1[j] != c2[j] {
						t.Fatalf("seed %d: Fate delays differ", seed)
					}
					if c1[j] < l.Latency || c1[j] > maxDelay {
						t.Fatalf("seed %d: delay %v outside [%v, %v] for %+v", seed, c1[j], l.Latency, maxDelay, l)
					}
				}
				if len(c1) > 2 {
					t.Fatalf("seed %d: %d copies", seed, len(c1))
				}
				if size > l.MTU && len(c1) != 0 {
					t.Fatalf("seed %d: oversize packet (%d > MTU %d) survived", seed, size, l.MTU)
				}
			}
		}
	}
}

func TestPartitionBlocksBothDirectionsAndHealRestores(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		n := New(3)
		n.SetDefaultLink(Link{Latency: 10 * time.Millisecond})
		names := []string{"a", "b", "c", "d"}
		conns := map[string]*PacketConn{}
		for _, a := range names {
			conns[a] = listen(t, n, a)
		}
		// reach sends one packet along every directed pair and reports
		// which pairs got through.
		reach := func() map[[2]string]bool {
			for _, from := range names {
				for _, to := range names {
					if from != to {
						send(t, conns[from], to, []byte(from))
					}
				}
			}
			out := map[[2]string]bool{}
			for _, to := range names {
				for _, r := range drain(t, conns[to], 100*time.Millisecond) {
					out[[2]string{r.from, to}] = true
				}
			}
			return out
		}
		expect := func(phase string, ok func(from, to string) bool) {
			t.Helper()
			got := reach()
			for _, from := range names {
				for _, to := range names {
					if from == to {
						continue
					}
					if want := ok(from, to); got[[2]string{from, to}] != want {
						t.Fatalf("%s: %s→%s delivered=%v want %v", phase, from, to, !want, want)
					}
					if want := ok(from, to); n.Reachable(from, to) != want {
						t.Fatalf("%s: Reachable(%s,%s) disagrees", phase, from, to)
					}
				}
			}
		}
		side := map[string]int{"a": 0, "b": 0, "c": 1, "d": 1}
		expect("connected", func(string, string) bool { return true })
		n.Partition([]string{"a", "b"}, []string{"c", "d"})
		expect("split", func(f, to string) bool { return side[f] == side[to] })
		n.Heal()
		expect("healed", func(string, string) bool { return true })
		n.Partition([]string{"a"}) // a vs implicit rest
		expect("minority", func(f, to string) bool { return f != "a" && to != "a" })
		n.Heal()
		n.Block("a", "b")
		expect("one-way block", func(f, to string) bool { return !(f == "a" && to == "b") })
		n.Unblock("a", "b")
		n.Block("c", Wildcard)
		expect("wildcard block", func(f, to string) bool { return f != "c" })
		n.Heal()
		n.Isolate("d")
		expect("isolate", func(f, to string) bool { return f != "d" && to != "d" })
		n.Unisolate("d")
		expect("unisolate", func(string, string) bool { return true })

		// A partition also drops packets already in flight.
		send(t, conns["a"], "c", []byte("in flight"))
		time.Sleep(5 * time.Millisecond)
		before := n.Stats().DroppedPartition
		n.Partition([]string{"a"}, []string{"c"})
		if got := drain(t, conns["c"], 50*time.Millisecond); len(got) != 0 {
			t.Fatalf("in-flight packet crossed a fresh partition")
		}
		if n.Stats().DroppedPartition != before+1 {
			t.Fatalf("in-flight drop not counted")
		}
	})
}

func TestDeadlines(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		n := New(4)
		a, b := listen(t, n, "a"), listen(t, n, "b")
		buf := make([]byte, 16)
		isTimeout := func(err error) bool {
			var ne net.Error
			return errors.As(err, &ne) && ne.Timeout() && errors.Is(err, os.ErrDeadlineExceeded)
		}

		start := time.Now()
		_ = b.SetReadDeadline(start.Add(100 * time.Millisecond))
		if _, _, err := b.ReadFrom(buf); !isTimeout(err) {
			t.Fatalf("expected timeout, got %v", err)
		}
		if el := time.Since(start); el != 100*time.Millisecond {
			t.Fatalf("timed out after %v, want exactly 100ms", el)
		}
		// Deadline already in the past: immediate.
		if _, _, err := b.ReadFrom(buf); !isTimeout(err) {
			t.Fatalf("past deadline: %v", err)
		}

		// Moving the deadline while a reader is blocked wakes it.
		_ = b.SetReadDeadline(time.Time{})
		errc := make(chan error, 1)
		go func() { _, _, err := b.ReadFrom(buf); errc <- err }()
		time.Sleep(50 * time.Millisecond)
		synctest.Wait()
		start = time.Now()
		_ = b.SetReadDeadline(start)
		if err := <-errc; !isTimeout(err) {
			t.Fatalf("shortened deadline: %v", err)
		}
		if time.Since(start) != 0 {
			t.Fatalf("reader not woken immediately")
		}

		// Extending the deadline lets a later packet through.
		_ = b.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
		go func() { _, _, err := b.ReadFrom(buf); errc <- err }()
		time.Sleep(50 * time.Millisecond)
		_ = b.SetReadDeadline(time.Now().Add(250 * time.Millisecond))
		time.Sleep(150 * time.Millisecond)
		send(t, a, "b", []byte("late"))
		if err := <-errc; err != nil {
			t.Fatalf("extended deadline: %v", err)
		}

		// Write deadlines: writes never block, so only a past deadline fails.
		_ = a.SetWriteDeadline(time.Now().Add(time.Second))
		send(t, a, "b", []byte("ok"))
		_ = a.SetWriteDeadline(time.Now())
		if _, err := a.WriteTo([]byte("x"), Addr("b")); !isTimeout(err) {
			t.Fatalf("write past deadline: %v", err)
		}
		_ = a.SetDeadline(time.Time{})
		send(t, a, "b", []byte("ok again"))
	})
}

func TestMTU(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		n := New(5)
		n.SetLink("a", "b", Link{MTU: 100})
		a, b := listen(t, n, "a"), listen(t, n, "b")
		send(t, a, "b", make([]byte, 100))
		send(t, a, "b", make([]byte, 101))
		send(t, b, "a", make([]byte, 1000)) // reverse direction has no MTU
		if got := drain(t, b, 10*time.Millisecond); len(got) != 1 || len(got[0].data) != 100 {
			t.Fatalf("a→b delivered %d packets", len(got))
		}
		if got := drain(t, a, 10*time.Millisecond); len(got) != 1 {
			t.Fatalf("b→a delivered %d packets", len(got))
		}
		if s := n.LinkStats("a", "b"); s.DroppedMTU != 1 || s.Delivered != 1 {
			t.Fatalf("a→b stats %v", s)
		}
	})
}

func TestBandwidthSerialises(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const bw, size, count = 100_000, 1000, 100
		const lat = 10 * time.Millisecond
		n := New(6)
		n.SetDefaultLink(Link{Bandwidth: bw, Latency: lat})
		a, b := listen(t, n, "a"), listen(t, n, "b")
		t0 := time.Now()
		for i := 0; i < count; i++ {
			send(t, a, "b", make([]byte, size))
		}
		got := drain(t, b, time.Second)
		if len(got) != count {
			t.Fatalf("delivered %d of %d", len(got), count)
		}
		tx := Link{Bandwidth: bw}.TransmitTime(size)
		if tx != 10*time.Millisecond {
			t.Fatalf("TransmitTime = %v", tx)
		}
		for i, r := range got {
			if want := t0.Add(time.Duration(i+1)*tx + lat); !r.at.Equal(want) {
				t.Fatalf("packet %d arrived at +%v, want +%v", i, r.at.Sub(t0), want.Sub(t0))
			}
		}
		elapsed := got[len(got)-1].at.Sub(t0) - lat
		if thr := float64(count*size) / elapsed.Seconds(); math.Abs(thr-bw)/bw > 0.01 {
			t.Fatalf("throughput %.0f B/s, configured %d", thr, bw)
		}

		// A sender below the line rate sees no queueing delay.
		// Read concurrently: drain stamps a packet when it is read, so a
		// reader that only starts after the sends would see queueing delay
		// that the link never added.
		t1 := time.Now()
		paced := make(chan []rx, 1)
		go func() { paced <- drain(t, b, time.Second) }()
		for i := 0; i < 10; i++ {
			send(t, a, "b", make([]byte, size))
			time.Sleep(20 * time.Millisecond)
		}
		for i, r := range <-paced {
			if want := t1.Add(time.Duration(i)*20*time.Millisecond + tx + lat); !r.at.Equal(want) {
				t.Fatalf("paced packet %d at +%v want +%v", i, r.at.Sub(t1), want.Sub(t1))
			}
		}

		// A bounded backlog tail-drops a burst.
		n.SetLink("a", "b", Link{Bandwidth: bw, QueueBytes: 5 * size})
		n.ResetStats()
		for i := 0; i < count; i++ {
			send(t, a, "b", make([]byte, size))
		}
		got = drain(t, b, time.Second)
		s := n.LinkStats("a", "b")
		if len(got) != 6 || s.DroppedLinkQueue != count-6 {
			t.Fatalf("bounded backlog: delivered %d, stats %v", len(got), s)
		}
	})
}

func TestCloseUnblocksReaders(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		n := New(7)
		c := listen(t, n, "a")
		errc := make(chan error, 3)
		for i := 0; i < 3; i++ {
			go func() {
				_, _, err := c.ReadFrom(make([]byte, 8))
				errc <- err
			}()
		}
		synctest.Wait()
		if err := c.Close(); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 3; i++ {
			if err := <-errc; !errors.Is(err, net.ErrClosed) {
				t.Fatalf("reader got %v, want net.ErrClosed", err)
			}
		}
		if err := c.Close(); !errors.Is(err, net.ErrClosed) {
			t.Fatalf("double close: %v", err)
		}
		if _, err := c.WriteTo([]byte("x"), Addr("b")); !errors.Is(err, net.ErrClosed) {
			t.Fatalf("write after close: %v", err)
		}
		if err := c.SetReadDeadline(time.Now()); !errors.Is(err, net.ErrClosed) {
			t.Fatalf("deadline after close: %v", err)
		}
		// The address is free again.
		c2 := listen(t, n, "a")
		_ = c2.Close()
		_ = n.Close()
		if _, err := n.Listen("z"); err == nil {
			t.Fatal("Listen on closed network succeeded")
		}
	})
}

// TestConcurrentWritersAndReaders checks that nothing is lost or duplicated
// with many goroutines writing to one conn and several reading from it.
func TestConcurrentWritersAndReaders(t *testing.T) {
	run := func(t *testing.T, link Link) {
		const writers, per, readers = 16, 300, 4
		n := New(8)
		n.SetQueueLimit(writers * per)
		n.SetDefaultLink(link)
		dst := listen(t, n, "dst")
		var wg sync.WaitGroup
		for w := 0; w < writers; w++ {
			c := listen(t, n, "")
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				for i := 0; i < per; i++ {
					if _, err := c.WriteTo(u64(uint64(w*per+i)), Addr("dst")); err != nil {
						t.Errorf("write: %v", err)
						return
					}
				}
			}(w)
		}
		var mu sync.Mutex
		seen := make(map[uint64]int)
		var rwg sync.WaitGroup
		for r := 0; r < readers; r++ {
			rwg.Add(1)
			go func() {
				defer rwg.Done()
				buf := make([]byte, 8)
				for {
					_, _, err := dst.ReadFrom(buf)
					if err != nil {
						return
					}
					mu.Lock()
					seen[binary.BigEndian.Uint64(buf)]++
					done := len(seen) == writers*per
					mu.Unlock()
					if done {
						_ = dst.Close()
						return
					}
				}
			}()
		}
		wg.Wait()
		rwg.Wait()
		if len(seen) != writers*per {
			t.Fatalf("received %d distinct of %d", len(seen), writers*per)
		}
		for k, v := range seen {
			if v != 1 {
				t.Fatalf("packet %d received %d times", k, v)
			}
		}
	}
	t.Run("synctest", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) { run(t, Link{Latency: time.Millisecond, Jitter: time.Millisecond}) })
	})
	t.Run("realtime", func(t *testing.T) { run(t, Link{}) })
}

func TestReorderAndDuplicateAndCorrupt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		n := New(9)
		n.SetLink("a", "b", Link{Latency: 10 * time.Millisecond, Reorder: 0.3})
		n.SetLink("a", "c", Link{Duplicate: 1})
		n.SetLink("a", "d", Link{Corrupt: 1})
		a, b, c, d := listen(t, n, "a"), listen(t, n, "b"), listen(t, n, "c"), listen(t, n, "d")
		for i := 0; i < 500; i++ {
			send(t, a, "b", u64(uint64(i)))
			time.Sleep(time.Millisecond)
		}
		got := drain(t, b, time.Second)
		if len(got) != 500 {
			t.Fatalf("reorder link lost packets: %d", len(got))
		}
		inversions := 0
		for i := 1; i < len(got); i++ {
			if binary.BigEndian.Uint64(got[i].data) < binary.BigEndian.Uint64(got[i-1].data) {
				inversions++
			}
		}
		if inversions == 0 || n.LinkStats("a", "b").Reordered == 0 {
			t.Fatalf("no reordering observed (inversions %d, stats %v)", inversions, n.LinkStats("a", "b"))
		}

		for i := 0; i < 50; i++ {
			send(t, a, "c", u64(uint64(i)))
		}
		if got := drain(t, c, time.Second); len(got) != 100 {
			t.Fatalf("Duplicate=1 delivered %d of 100 copies", len(got))
		}

		orig := bytes.Repeat([]byte{0x5a}, 32)
		for i := 0; i < 50; i++ {
			send(t, a, "d", orig)
		}
		for _, r := range drain(t, d, time.Second) {
			diff := 0
			for j := range orig {
				diff += bits.OnesCount8(orig[j] ^ r.data[j])
			}
			if diff != 1 {
				t.Fatalf("corrupted packet differs in %d bits, want 1", diff)
			}
		}
		if !bytes.Equal(orig, bytes.Repeat([]byte{0x5a}, 32)) {
			t.Fatal("corruption modified the sender's buffer")
		}
	})
}

func TestQueueFullAndUnreachable(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		n := New(10)
		n.SetQueueLimit(10)
		a, b := listen(t, n, "a"), listen(t, n, "b")
		for i := 0; i < 25; i++ {
			send(t, a, "b", []byte{byte(i)})
		}
		send(t, a, "nobody", []byte("hello?"))
		time.Sleep(time.Millisecond)
		s := n.Stats()
		if s.Delivered != 10 || s.DroppedQueueFull != 15 || s.DroppedUnreachable != 1 {
			t.Fatalf("stats %v", s)
		}
		got := drain(t, b, time.Millisecond)
		for i, r := range got {
			if r.data[0] != byte(i) {
				t.Fatalf("queue kept the wrong packets: %d at %d", r.data[0], i)
			}
		}
		// Short read buffer truncates like UDP.
		send(t, a, "b", []byte("0123456789"))
		buf := make([]byte, 4)
		k, _, err := b.ReadFrom(buf)
		if err != nil || k != 4 || string(buf) != "0123" {
			t.Fatalf("truncated read = %d %q %v", k, buf[:k], err)
		}
	})
}

func TestListenResolveAndLinks(t *testing.T) {
	n := New(11)
	c1 := listen(t, n, "host:0")
	c2 := listen(t, n, "host:0")
	if c1.LocalAddr().String() == c2.LocalAddr().String() {
		t.Fatalf("ephemeral ports collide: %v", c1.LocalAddr())
	}
	if c1.LocalAddr().Network() != "sim" {
		t.Fatalf("network = %q", c1.LocalAddr().Network())
	}
	if _, err := n.Listen(c1.LocalAddr().String()); err == nil {
		t.Fatal("duplicate Listen succeeded")
	}
	for _, bad := range []string{"*", "a:*"} {
		if _, err := n.Listen(bad); err == nil {
			t.Fatalf("Listen(%q) succeeded", bad)
		}
		if _, err := Resolve(bad); err == nil {
			t.Fatalf("Resolve(%q) succeeded", bad)
		}
	}
	if a, err := n.Resolve("x:1"); err != nil || a.String() != "x:1" || a.Network() != "sim" {
		t.Fatalf("Resolve = %v, %v", a, err)
	}
	if _, err := c1.WriteTo([]byte("x"), nil); err == nil {
		t.Fatal("WriteTo(nil) succeeded")
	}

	def, aAny, anyB, ab := Link{Latency: 1}, Link{Latency: 2}, Link{Latency: 3}, Link{Latency: 4}
	n.SetLink(Wildcard, Wildcard, def)
	n.SetLink("a", Wildcard, aAny)
	n.SetLink(Wildcard, "b", anyB)
	n.SetLink("a", "b", ab)
	cases := []struct {
		from, to string
		want     Link
	}{{"a", "b", ab}, {"a", "c", aAny}, {"c", "b", anyB}, {"c", "d", def}}
	for _, tc := range cases {
		if got := n.Link(tc.from, tc.to); got != tc.want {
			t.Errorf("Link(%s,%s) = %+v want %+v", tc.from, tc.to, got, tc.want)
		}
	}
	n.ClearLink("a", "b")
	if got := n.Link("a", "b"); got != aAny {
		t.Errorf("after ClearLink Link(a,b) = %+v", got)
	}
	if len(n.Addrs()) != 2 {
		t.Errorf("Addrs = %v", n.Addrs())
	}
}

// TestPropertyStatsConserved runs random traffic over random links while the
// topology changes underneath, then checks the accounting identity: every sent
// packet (plus every duplicate) is delivered or dropped for exactly one reason.
func TestPropertyStatsConserved(t *testing.T) {
	for _, seed := range seeds(t, 12) {
		synctest.Test(t, func(t *testing.T) {
			rng := rand.New(rand.NewPCG(uint64(seed), 99))
			n := New(seed)
			n.SetQueueLimit(1 + rng.IntN(64))
			addrs := []string{"a", "b", "c", "d", "e"}
			conns := map[string]*PacketConn{}
			for _, a := range addrs[:4] { // "e" is never listening
				conns[a] = listen(t, n, a)
			}
			randLink := func() Link {
				return Link{
					Latency: time.Duration(rng.IntN(20)) * time.Millisecond, Jitter: time.Duration(rng.IntN(5)) * time.Millisecond,
					Loss: rng.Float64() * 0.3, Duplicate: rng.Float64() * 0.2, Reorder: rng.Float64() * 0.2, Corrupt: rng.Float64() * 0.1,
					Bandwidth: []int{0, 50_000, 1_000_000}[rng.IntN(3)], QueueBytes: rng.IntN(3) * 2000, MTU: []int{0, 600, 1500}[rng.IntN(3)],
				}
			}
			n.SetDefaultLink(randLink())
			for i := 0; i < 3; i++ {
				n.SetLink(addrs[rng.IntN(5)], addrs[rng.IntN(5)], randLink())
			}
			var wg sync.WaitGroup
			for _, a := range addrs[:4] {
				c := conns[a]
				wg.Add(1)
				go func() { // keep receive queues moving sometimes
					defer wg.Done()
					buf := make([]byte, 2000)
					for {
						if _, _, err := c.ReadFrom(buf); err != nil {
							return
						}
					}
				}()
			}
			for step := 0; step < 400; step++ {
				switch r := rng.IntN(20); {
				case r == 0:
					n.Partition(addrs[:rng.IntN(5)], addrs[rng.IntN(5):])
				case r == 1:
					n.Heal()
				case r == 2:
					n.Block(addrs[rng.IntN(5)], addrs[rng.IntN(5)])
				case r == 3:
					n.Isolate(addrs[rng.IntN(5)])
				default:
					from := addrs[rng.IntN(4)]
					send(t, conns[from], addrs[rng.IntN(5)], make([]byte, rng.IntN(1600)))
				}
				time.Sleep(time.Duration(rng.IntN(3)) * time.Millisecond)
			}
			time.Sleep(10 * time.Second) // let every backlog and delay drain
			s := n.Stats()
			if s.Sent+s.Duplicated != s.Delivered+s.Dropped() {
				t.Fatalf("seed %d: accounting broken: %v", seed, s)
			}
			var sum Stats
			for _, from := range addrs {
				for _, to := range addrs {
					ls := n.LinkStats(from, to)
					sum.add(&ls)
				}
			}
			if sum != s {
				t.Fatalf("seed %d: per-link stats %v don't sum to global %v", seed, sum, s)
			}
			if all := n.LinkStats(Wildcard, Wildcard); all != s {
				t.Fatalf("seed %d: wildcard stats %v != global %v", seed, all, s)
			}
			_ = n.Close()
			wg.Wait()
		})
	}
}

// TestDeterministicReplay: with a single sender, the same seed reproduces the
// same arrival schedule exactly.
func TestDeterministicReplay(t *testing.T) {
	run := func(seed int64) []string {
		var trace []string
		synctest.Test(t, func(t *testing.T) {
			n := New(seed)
			n.SetDefaultLink(Link{Latency: 5 * time.Millisecond, Jitter: 5 * time.Millisecond, Loss: 0.2, Duplicate: 0.1, Reorder: 0.1})
			a, b := listen(t, n, "a"), listen(t, n, "b")
			t0 := time.Now()
			for i := 0; i < 300; i++ {
				send(t, a, "b", u64(uint64(i)))
				time.Sleep(time.Millisecond)
			}
			for _, r := range drain(t, b, time.Second) {
				trace = append(trace, r.at.Sub(t0).String()+"/"+string(r.data))
			}
		})
		return trace
	}
	a, b, c := run(42), run(42), run(43)
	if len(a) == 0 || len(a) != len(b) {
		t.Fatalf("replay lengths %d vs %d", len(a), len(b))
	}
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("replay diverged at %d", i)
		}
	}
	same := len(a) == len(c)
	for i := 0; same && i < len(a); i++ {
		same = a[i] == c[i]
	}
	if same {
		t.Fatal("different seeds produced identical traces")
	}
}
