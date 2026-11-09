package pmd

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Sebby1770/kvstore/kvgw/netstack/endpoint"
	"github.com/Sebby1770/kvstore/kvgw/netstack/fabric"
	"github.com/Sebby1770/kvstore/kvgw/netstack/ktp"
)

func TestRingMPMC(t *testing.T) {
	r := fabric.NewRegion(int(RingBytes(64)))
	q, err := FormatRing(r, 0, 64)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := FormatRing(r, 0, 48); err != ErrRingSize {
		t.Fatalf("non-power-of-two size: %v", err)
	}
	const producers, consumers, per = 4, 4, 20000
	seen := make([]int32, producers*per)
	var wg sync.WaitGroup
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			vals := make([]uint64, 0, 8)
			for i := 0; i < per; {
				vals = vals[:0]
				for j := 0; j < 8 && i+j < per; j++ {
					vals = append(vals, uint64(p*per+i+j))
				}
				n := q.Enqueue(vals)
				i += n
			}
		}(p)
	}
	var mu sync.Mutex
	got := 0
	done := make(chan struct{})
	var finish sync.Once
	for c := 0; c < consumers; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			out := make([]uint64, 16)
			for {
				n := q.Dequeue(out)
				mu.Lock()
				for _, v := range out[:n] {
					seen[v]++
				}
				got += n
				finished := got == producers*per
				mu.Unlock()
				if finished {
					finish.Do(func() { close(done) })
				}
				select {
				case <-done:
					return
				default:
				}
			}
		}()
	}
	wg.Wait()
	for v, n := range seen {
		if n != 1 {
			t.Fatalf("value %d seen %d times", v, n)
		}
	}
	if q.Len() != 0 {
		t.Fatalf("ring not empty: %d", q.Len())
	}
	// Full ring takes a partial burst.
	vals := make([]uint64, 100)
	if n := q.Enqueue(vals); n != 64 {
		t.Fatalf("burst into empty 64-ring took %d", n)
	}
	if n := q.Enqueue(vals[:1]); n != 0 {
		t.Fatalf("enqueue into full ring took %d", n)
	}
}

func TestPool(t *testing.T) {
	r := fabric.NewRegion(int(PoolBytes(8, 256)) + 64)
	p, err := FormatPool(r, 0, 8, 256)
	if err != nil {
		t.Fatal(err)
	}
	var ms []Mbuf
	for i := 0; i < 8; i++ {
		m, err := p.Get()
		if err != nil {
			t.Fatal(err)
		}
		copy(m.Room(), fmt.Sprintf("packet %d", i))
		m.SetLen(8)
		ms = append(ms, m)
	}
	if _, err := p.Get(); err != ErrPoolEmpty {
		t.Fatalf("ninth Get: %v", err)
	}
	if string(ms[3].Data()) != "packet 3" {
		t.Fatalf("data %q", ms[3].Data())
	}
	p.Put(ms...)
	if p.Free() != 8 {
		t.Fatalf("free %d", p.Free())
	}
	if m, _ := p.Get(); m.Len() != 0 {
		t.Fatal("reused buffer kept its old length")
	}
}

// Microsoft's published RSS verification vectors (IPv4, with and without
// ports), hashed with the default key.
func TestToeplitzVectors(t *testing.T) {
	ip := func(a, b, c, d byte) []byte { return []byte{a, b, c, d} }
	port := func(p uint16) []byte { return binary.BigEndian.AppendUint16(nil, p) }
	cat := func(parts ...[]byte) []byte { return bytes.Join(parts, nil) }
	cases := []struct {
		in   []byte
		want uint32
	}{
		{cat(ip(66, 9, 149, 187), ip(161, 142, 100, 80)), 0x323e8fc2},
		{cat(ip(66, 9, 149, 187), ip(161, 142, 100, 80), port(2794), port(1766)), 0x51ccc178},
		{cat(ip(199, 92, 111, 2), ip(65, 69, 140, 83)), 0xd718262a},
		{cat(ip(199, 92, 111, 2), ip(65, 69, 140, 83), port(14230), port(4739)), 0xc626b0ea},
	}
	for _, c := range cases {
		if got := Toeplitz(DefaultRSSKey, c.in); got != c.want {
			t.Errorf("Toeplitz(%x) = %#x, want %#x", c.in, got, c.want)
		}
	}
	// One flow, one queue.
	pkt := []byte{0x30, 1, 1, 2, 3, 4, 5, 6, 7, 8}
	if Steer(FlowHash("10.0.0.1:7000", pkt), 4) != Steer(FlowHash("10.0.0.1:7000", pkt), 4) {
		t.Fatal("steering is not stable")
	}
}

func memifPair(t *testing.T, cfg MemifConfig) (*MemifPort, *MemifPort) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "memif")
	a, err := CreateMemif(path, cfg)
	if err != nil {
		t.Skipf("no shared memory: %v", err)
	}
	b, err := OpenMemif(path, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { a.Close(); b.Close() })
	return a, b
}

// Packets cross the interface in place: the receiver's mbuf is the buffer
// the sender wrote (same index in the shared pool), in order, intact.
func TestMemifZeroCopy(t *testing.T) {
	a, b := memifPair(t, MemifConfig{RingSize: 256, Buffers: 512, DataRoom: 256})
	const total = 20000
	var sentIdx sync.Map
	go func() {
		bufs := make([]Mbuf, Burst)
		for i := 0; i < total; {
			n := a.Pool().GetBurst(bufs[:min(Burst, total-i)])
			for j := 0; j < n; j++ {
				binary.LittleEndian.PutUint64(bufs[j].Room(), uint64(i+j))
				bufs[j].SetLen(8)
				sentIdx.Store(uint64(i+j), bufs[j].Index())
			}
			sent := 0
			for sent < n {
				sent += a.TxBurst(bufs[sent:n])
			}
			i += n
		}
	}()
	bufs := make([]Mbuf, Burst)
	next := uint64(0)
	deadline := time.Now().Add(10 * time.Second)
	for next < total && time.Now().Before(deadline) {
		n := b.RxBurst(bufs)
		for _, m := range bufs[:n] {
			v := binary.LittleEndian.Uint64(m.Data())
			if v != next {
				t.Fatalf("packet %d arrived as #%d", v, next)
			}
			if idx, _ := sentIdx.Load(v); idx.(uint32) != m.Index() {
				t.Fatalf("packet %d: sent in buffer %d, received in %d (copied?)", v, idx, m.Index())
			}
			next++
		}
		b.Pool().Put(bufs[:n]...)
	}
	if next != total {
		t.Fatalf("received %d of %d", next, total)
	}
	if st := a.Stats(); st.TxPackets != total {
		t.Fatalf("tx stats %+v", st)
	}
}

// The child half of TestMemifCrossProcess: attach to the parent's
// interface and bounce every packet back in place (flip the first byte).
func TestMemifChildEcho(t *testing.T) {
	path := os.Getenv("PMD_MEMIF_CHILD")
	if path == "" {
		t.Skip("helper for TestMemifCrossProcess")
	}
	p, err := OpenMemif(path, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	l := &Lcore{Port: p}
	l.Run(ctx, func(bufs []Mbuf) {
		for _, m := range bufs {
			d := m.Data()
			if len(d) == 4 && string(d) == "quit" {
				cancel()
			}
			if len(d) > 0 {
				d[0] ^= 0xff
			}
		}
		for sent := 0; sent < len(bufs); {
			sent += p.TxBurst(bufs[sent:])
		}
	})
}

// Two processes exchange packets through shared memory alone: no socket,
// no syscall per packet. The child is this test binary re-executed.
func TestMemifCrossProcess(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns a process")
	}
	path := filepath.Join(t.TempDir(), "memif")
	a, err := CreateMemif(path, MemifConfig{RingSize: 512, Buffers: 1024, DataRoom: 512})
	if err != nil {
		t.Skipf("no shared memory: %v", err)
	}
	defer a.Close()
	cmd := exec.Command(os.Args[0], "-test.run", "^TestMemifChildEcho$", "-test.count=1")
	cmd.Env = append(os.Environ(), "PMD_MEMIF_CHILD="+path)
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	const total = 5000
	send := func(payload []byte) {
		for {
			m, err := a.Pool().Get()
			if err != nil {
				time.Sleep(time.Millisecond)
				continue
			}
			copy(m.Room(), payload)
			m.SetLen(len(payload))
			for a.TxBurst([]Mbuf{m}) == 0 {
				time.Sleep(10 * time.Microsecond)
			}
			return
		}
	}
	go func() {
		for i := 0; i < total; i++ {
			var b [8]byte
			binary.LittleEndian.PutUint64(b[:], uint64(i))
			send(b[:])
		}
	}()
	got := 0
	bufs := make([]Mbuf, Burst)
	var idle Idler
	deadline := time.Now().Add(20 * time.Second)
	for got < total && time.Now().Before(deadline) {
		n := a.RxBurst(bufs)
		if n == 0 {
			idle.Idle()
			continue
		}
		idle.Busy()
		for _, m := range bufs[:n] {
			d := m.Data()
			want := make([]byte, 8)
			binary.LittleEndian.PutUint64(want, uint64(got))
			want[0] ^= 0xff
			if !bytes.Equal(d, want) {
				t.Fatalf("echo %d: %x want %x", got, d, want)
			}
			got++
		}
		a.Pool().Put(bufs[:n]...)
	}
	send([]byte("quit"))
	cmd.Wait()
	if got != total {
		t.Fatalf("got %d of %d echoes; child output:\n%s", got, total, out.String())
	}
}

func key(seed byte) ed25519.PrivateKey {
	s := make([]byte, ed25519.SeedSize)
	s[0] = seed
	return ed25519.NewKeyFromSeed(s)
}

// The whole KTP stack runs over shared memory: an encrypted handshake and
// a 2 MiB stream transfer between two endpoints whose only link is memif.
func TestKTPOverMemif(t *testing.T) {
	a, b := memifPair(t, MemifConfig{RingSize: 1024, Buffers: 4096, DataRoom: 2048})
	cfg := func(seed byte) *ktp.Config {
		return &ktp.Config{Identity: key(seed), ClusterKey: []byte("memif"), HandshakeTimeout: 2 * time.Second}
	}
	epA, err := endpoint.New(NewPacketConn(a, "a", "b"), cfg(1), endpoint.Options{Resolve: Resolve})
	if err != nil {
		t.Fatal(err)
	}
	defer epA.Close()
	epB, err := endpoint.New(NewPacketConn(b, "b", "a"), cfg(2), endpoint.Options{Resolve: Resolve})
	if err != nil {
		t.Fatal(err)
	}
	defer epB.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	go func() {
		c, err := epB.Accept(ctx)
		if err != nil {
			return
		}
		st, err := c.AcceptStream(ctx)
		if err != nil {
			return
		}
		io.Copy(st, st) // echo
		st.Close()
	}()
	c, err := epA.Dial(ctx, "b")
	if err != nil {
		t.Fatal(err)
	}
	st, err := c.OpenStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	data := make([]byte, 2<<20)
	for i := range data {
		data[i] = byte(i * 7)
	}
	go func() { st.Write(data); st.Close() }()
	got, err := io.ReadAll(st)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("echoed %d bytes, mismatch", len(got))
	}
}

func TestSocketPort(t *testing.T) {
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Skip(err)
	}
	sp, err := NewSocketPort(pc, 64, 2048)
	if err != nil {
		t.Fatal(err)
	}
	defer sp.Close()
	peer, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	for i := 0; i < 10; i++ {
		peer.WriteTo([]byte(fmt.Sprintf("ping %d", i)), pc.LocalAddr())
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	l := &Lcore{Port: sp}
	echoed := 0
	l.Run(ctx, func(bufs []Mbuf) {
		for _, m := range bufs {
			copy(m.Room(), "pong")
		}
		echoed += sp.TxBurst(bufs)
		if echoed >= 10 {
			cancel()
		}
	})
	peer.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 64)
	for i := 0; i < 10; i++ {
		n, _, err := peer.ReadFrom(buf)
		if err != nil || string(buf[:4]) != "pong" {
			t.Fatalf("reply %d: %q %v", i, buf[:n], err)
		}
	}
	if st := l.Stats(); st.Packets != 10 {
		t.Fatalf("lcore stats %+v", st)
	}
}

func BenchmarkRingBurst(b *testing.B) {
	r := fabric.NewRegion(int(RingBytes(1024)))
	q, _ := FormatRing(r, 0, 1024)
	vals := make([]uint64, Burst)
	out := make([]uint64, Burst)
	b.SetBytes(Burst)
	for i := 0; i < b.N; i++ {
		q.Enqueue(vals)
		q.Dequeue(out)
	}
}

func BenchmarkMemifPackets(b *testing.B) {
	path := filepath.Join(b.TempDir(), "memif")
	a, err := CreateMemif(path, MemifConfig{})
	if err != nil {
		b.Skip(err)
	}
	defer a.Close()
	c, _ := OpenMemif(path, time.Second)
	defer c.Close()
	bufs := make([]Mbuf, Burst)
	rx := make([]Mbuf, Burst)
	b.ResetTimer()
	for i := 0; i < b.N; i += Burst {
		n := a.Pool().GetBurst(bufs)
		for _, m := range bufs[:n] {
			m.SetLen(64)
		}
		a.TxBurst(bufs[:n])
		k := c.RxBurst(rx)
		c.Pool().Put(rx[:k]...)
	}
}
