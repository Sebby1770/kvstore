package offload

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Sebby1770/kvstore/kvgw/netstack/endpoint"
	"github.com/Sebby1770/kvstore/kvgw/netstack/ktp"
	"github.com/Sebby1770/kvstore/kvgw/netstack/simnet"
)

// The encoding is the kernel's: "mov r0, 2; exit" is b7 00 00 00 02 00 00 00
// 95 00 00 00 00 00 00 00 in any eBPF disassembler.
func TestKernelEncoding(t *testing.T) {
	p := NewBuilder().Return(XDPPass).MustBuild()
	got := hex.EncodeToString(Encode(p))
	if want := "b7000000020000009500000000000000"; got != want {
		t.Fatalf("encoding %s, want %s", got, want)
	}
	// r1 = *(u64 *)(r6 + 8): ldxdw, dst 1, src 6 -> 79 61 08 00 00 00 00 00
	ld := NewBuilder().Load(DW, R1, R6, 8).MustBuild()
	if got := hex.EncodeToString(Encode(ld)); got != "7961080000000000" {
		t.Fatalf("ldxdw encoding %s", got)
	}
	// Map reference: lddw r1, map[0] (src = pseudo map fd) -> 18 11 ...
	mp := NewBuilder().LoadMap(R1, 0).MustBuild()
	if b := Encode(mp); b[0] != 0x18 || b[1] != 0x11 || len(b) != 16 {
		t.Fatalf("map load encoding %x", b)
	}
	for _, rl := range []func() (*Loaded, error){
		func() (*Loaded, error) { r, err := NewRateLimiter(10, 5); return r.Prog, err },
		func() (*Loaded, error) { m, err := NewMaglev(4); return m.Prog, err },
	} {
		l, err := rl()
		if err != nil {
			t.Fatal(err)
		}
		back, err := Decode(l.Bytecode())
		if err != nil || len(back) != len(l.prog.Insns) {
			t.Fatalf("decode: %v", err)
		}
		for i := range back {
			if back[i] != l.prog.Insns[i] {
				t.Fatalf("%s insn %d: %v != %v", l.Name(), i, back[i], l.prog.Insns[i])
			}
		}
	}
}

func TestVerifierRejects(t *testing.T) {
	m := NewArrayMap("m", 8, 1)
	cases := []struct {
		name  string
		insns []Insn
		maps  []Map
		want  string
	}{
		{"empty", nil, nil, "instructions"},
		{"backward jump", []Insn{{Op: ClassALU64 | uint8(Mov), Dst: R0}, {Op: ClassJMP | uint8(Ja), Off: -2}, {Op: ClassJMP | uint8(exit)}}, nil, "backward"},
		{"uninitialised read", NewBuilder().MovReg(R0, R3).Exit().MustBuild(), nil, "uninitialised"},
		{"exit without r0", NewBuilder().Exit().MustBuild(), nil, "r0"},
		{"write r10", NewBuilder().Mov(R10, 1).Return(0).MustBuild(), nil, "read-only"},
		{"div by zero", NewBuilder().Mov(R0, 1).ALU64Imm(Div, R0, 0).Exit().MustBuild(), nil, "zero"},
		{"wide shift", NewBuilder().Mov(R0, 1).ALU64Imm(Lsh, R0, 64).Exit().MustBuild(), nil, "shift"},
		{"unknown helper", NewBuilder().Call(9999).Return(0).MustBuild(), nil, "helper"},
		{"helper args", NewBuilder().Call(HelperMapLookup).Return(0).MustBuild(), nil, "uninitialised"},
		{"bad map", NewBuilder().LoadMap(R1, 3).Return(0).MustBuild(), []Map{m}, "map"},
		{"falls off", NewBuilder().Mov(R0, 1).MustBuild(), nil, "end"},
		{"jump out", []Insn{{Op: ClassALU64 | uint8(Mov), Dst: R0}, {Op: ClassJMP | uint8(Jeq), Dst: R0, Off: 5}, {Op: ClassJMP | uint8(exit)}}, nil, "end"},
		{"unreachable", []Insn{{Op: ClassALU64 | uint8(Mov), Dst: R0}, {Op: ClassJMP | uint8(exit)}, {Op: ClassJMP | uint8(exit)}}, nil, "unreachable"},
		{"clobbered by call", NewBuilder().Mov(R2, 1).Call(HelperKtimeGetNs).MovReg(R0, R2).Exit().MustBuild(), nil, "uninitialised"},
	}
	for _, c := range cases {
		err := (&Program{Insns: c.insns, Maps: c.maps}).Verify()
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: got %v, want an error mentioning %q", c.name, err, c.want)
		}
	}
	// Forward-only jumps also mean the assembler refuses loops.
	if _, err := NewBuilder().Label("top").Mov(R0, 0).Goto("top").Build(); err == nil {
		t.Fatal("assembler accepted a backward jump")
	}
}

func runProg(t *testing.T, b *Builder, pkt []byte, maps ...Map) Result {
	t.Helper()
	l, err := Load(&Program{Insns: b.MustBuild(), Maps: maps})
	if err != nil {
		t.Fatal(err)
	}
	return l.Run(&Input{Packet: pkt, Now: 1})
}

func TestVMSemantics(t *testing.T) {
	// Arithmetic, 32-bit wraparound, signed comparison, and the stack.
	b := NewBuilder().
		Mov(R1, -5).Mov(R2, 3).
		JumpReg(Jsgt, R1, R2, "bad").               // -5 > 3 signed? no
		JumpReg(Jle, R1, R2, "bad").                // as unsigned, -5 is huge
		ALU32Imm(Mov, R3, -1).ALU32Imm(Add, R3, 1). // wraps to 0
		JumpImm(Jne, R3, 0, "bad").
		Mov(R4, 7).Store(DW, R10, -8, R4).Load(DW, R5, R10, -8).
		Mov(R4, 5).AtomicAdd(DW, R10, -8, R4).Load(DW, R5, R10, -8).
		JumpImm(Jne, R5, 12, "bad").
		Mov(R0, 9).ALU64Imm(Mod, R0, 4).  // 1
		Mov(R1, 0).ALU64Reg(Div, R0, R1). // x/0 = 0
		JumpImm(Jne, R0, 0, "bad").
		Return(XDPPass).
		Label("bad").Return(XDPDrop)
	if r := runProg(t, b, nil); r.Action != XDPPass {
		t.Fatalf("semantics check returned %d", r.Action)
	}
	// Reading past the packet faults the run (aborted), it does not crash.
	pk := NewBuilder().Load(DW, R2, R1, CtxData).Load(W, R0, R2, 6).Exit()
	if r := runProg(t, pk, make([]byte, 8)); r.Action != XDPAborted {
		t.Fatalf("out-of-bounds packet read: action %d", r.Action)
	}
	// The context is read-only.
	ro := NewBuilder().StoreImm(DW, R1, 0, 1).Return(XDPPass)
	if r := runProg(t, ro, nil); r.Action != XDPAborted {
		t.Fatalf("context write: action %d", r.Action)
	}
	// Bounds-checked packet rewrite works.
	rw := NewBuilder().Load(DW, R2, R1, CtxData).Load(DW, R3, R1, CtxDataEnd).
		MovReg(R4, R2).ALU64Imm(Add, R4, 2).JumpReg(Jgt, R4, R3, "short").
		StoreImm(H, R2, 0, 0x4142).Label("short").Return(XDPPass)
	pkt := []byte{0, 0, 9}
	runProg(t, rw, pkt)
	if !bytes.Equal(pkt, []byte{0x42, 0x41, 9}) {
		t.Fatalf("rewrite: %x", pkt)
	}
	// Maps: update then lookup through a value pointer.
	hm := NewHashMap("h", 8, 8, 4, false)
	mb := NewBuilder().
		StoreImm(DW, R10, -8, 77).StoreImm(DW, R10, -16, 1234).
		LoadMap(R1, 0).MovReg(R2, R10).ALU64Imm(Add, R2, -8).MovReg(R3, R10).ALU64Imm(Add, R3, -16).Mov(R4, 0).
		Call(HelperMapUpdate).
		LoadMap(R1, 0).MovReg(R2, R10).ALU64Imm(Add, R2, -8).Call(HelperMapLookup).
		JumpImm(Jeq, R0, 0, "miss").
		Load(DW, R0, R0, 0).Exit().
		Label("miss").Return(-1)
	if r := runProg(t, mb, nil, hm); r.Action != 1234 {
		t.Fatalf("map round trip returned %d", r.Action)
	}
	if v, ok := hm.Lookup(U64(77)); !ok || binary.LittleEndian.Uint64(v) != 1234 {
		t.Fatalf("Go side sees %x %v", v, ok)
	}
}

// Random bytecode: whatever the verifier accepts runs to completion on any
// packet without panicking.
func FuzzVerifiedPrograms(f *testing.F) {
	rl, _ := NewRateLimiter(1, 1)
	f.Add(rl.Prog.Bytecode(), []byte{0x10, 1, 2, 3})
	f.Add(Encode(NewBuilder().Return(2).MustBuild()), []byte{})
	f.Fuzz(func(t *testing.T, code, pkt []byte) {
		insns, err := Decode(code[:len(code)&^7])
		if err != nil {
			return
		}
		p := &Program{Insns: insns, Maps: []Map{NewArrayMap("a", 16, 2), NewHashMap("h", 8, 16, 4, true), NewAddrMap("d", 2), NewQueueMap("q", 1, 1)}}
		if p.Verify() != nil {
			return
		}
		l, _ := Load(p)
		l.Run(&Input{Packet: append([]byte(nil), pkt...), PeerKey: 9, Now: 5})
	})
}

func TestRateLimiter(t *testing.T) {
	rl, err := NewRateLimiter(2, 3) // 2/s, burst 3; meters Initial and Raw
	if err != nil {
		t.Fatal(err)
	}
	initial := []byte{0x10, 1}
	stream := []byte{0x30, 1}
	at := func(now time.Duration, peer uint64, pkt []byte) int {
		return rl.Prog.Run(&Input{Packet: pkt, PeerKey: peer, Now: int64(time.Hour + now)}).Action
	}
	var verdicts []int
	for i := 0; i < 5; i++ {
		verdicts = append(verdicts, at(0, 1, initial))
	}
	if fmt.Sprint(verdicts) != fmt.Sprint([]int{XDPPass, XDPPass, XDPPass, XDPDrop, XDPDrop}) {
		t.Fatalf("burst of 5 from one peer: %v", verdicts)
	}
	if at(0, 2, initial) != XDPPass {
		t.Fatal("a second peer shares the first one's bucket")
	}
	if at(0, 1, stream) != XDPPass {
		t.Fatal("established-connection packets were metered")
	}
	if at(500*time.Millisecond, 1, initial) != XDPPass || at(500*time.Millisecond, 1, initial) != XDPDrop {
		t.Fatal("half a second at 2/s should refill exactly one token")
	}
	// A long silence refills to the burst, not beyond.
	for i := 0; i < 3; i++ {
		if at(time.Minute, 1, initial) != XDPPass {
			t.Fatalf("after a minute, packet %d dropped", i)
		}
	}
	if at(time.Minute, 1, initial) != XDPDrop {
		t.Fatal("bucket refilled past its burst")
	}
	// Members are exempt.
	rl.Exempt([]string{"member:7000"})
	member := PeerKeyString("member:7000")
	for i := 0; i < 10; i++ {
		if at(time.Minute, member, initial) != XDPPass {
			t.Fatal("an exempt member was metered")
		}
	}
	rl.Exempt(nil)
	for i := 0; i < 3; i++ {
		at(time.Minute, member, initial)
	}
	if at(time.Minute, member, initial) != XDPDrop {
		t.Fatal("a removed exemption still applies")
	}
	// The control plane resizes the per-node share as the cluster changes.
	rl.SetRate(Share(100, 3), 3)
	if r, _ := rl.Rate(); r != 34 {
		t.Fatalf("share of 100 across 3 nodes = %d", r)
	}
}

func TestCircuitBreaker(t *testing.T) {
	cb, err := NewCircuitBreaker(3, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	peer := "10.0.0.9:7000"
	key := PeerKeyString(peer)
	now := time.Hour
	cb.SetClock(func() int64 { return int64(now) })
	in := func(pkt []byte) { cb.Ingress.Run(&Input{Packet: pkt, PeerKey: key, Now: int64(now)}) }
	out := func() int {
		return cb.Egress.Run(&Input{Packet: []byte{0x30}, PeerKey: key, Egress: true, Now: int64(now)}).Action
	}
	reset := []byte{0x50, 0, 0}
	in(reset)
	in(reset)
	if out() != XDPPass || cb.Open(peer) {
		t.Fatal("tripped before the threshold")
	}
	cb.Failure(peer) // a host-reported failure is the third
	if !cb.Open(peer) || out() != XDPDrop {
		t.Fatal("circuit did not open at the threshold")
	}
	now += 999 * time.Millisecond
	if out() != XDPDrop {
		t.Fatal("probe let through before the cooldown")
	}
	now += 2 * time.Millisecond
	if out() != XDPPass {
		t.Fatal("no probe after the cooldown")
	}
	if s, _ := cb.State(peer); s != BreakerHalfOpen || out() != XDPDrop {
		t.Fatalf("after the probe: state %d (want half-open, others dropped)", s)
	}
	in([]byte{0x30, 1}) // the peer answers
	if s, f := cb.State(peer); s != BreakerClosed || f != 0 || out() != XDPPass {
		t.Fatalf("healthy reply did not close the circuit: state %d failures %d", s, f)
	}
	// Host successes close a half-open circuit too.
	for i := 0; i < 3; i++ {
		cb.Failure(peer)
	}
	now += 2 * time.Second
	out()
	cb.Success(peer)
	if s, _ := cb.State(peer); s != BreakerClosed {
		t.Fatalf("Success left state %d", s)
	}
}

func TestHeartbeatProgram(t *testing.T) {
	hb, err := NewHeartbeat([]byte("cluster secret"))
	if err != nil {
		t.Fatal(err)
	}
	now := int64(time.Hour)
	hb.SetClock(func() int64 { return now })
	hb.Touch()
	hb.SetLoad(420)
	probe := make([]byte, HeartbeatSize)
	probe[0] = hbProbe
	binary.LittleEndian.PutUint64(probe[8:], 7)
	binary.LittleEndian.PutUint64(probe[40:], hb.mac(probe))
	now += int64(250 * time.Millisecond)
	pkt := append([]byte(nil), probe...)
	if r := hb.Prog.Run(&Input{Packet: pkt, Now: now}); r.Action != XDPTX {
		t.Fatalf("valid probe: action %d", r.Action)
	}
	if pkt[0] != hbAck || binary.LittleEndian.Uint64(pkt[24:]) != uint64(250*time.Millisecond) ||
		binary.LittleEndian.Uint64(pkt[32:]) != 420 || binary.LittleEndian.Uint64(pkt[40:]) != hb.mac(pkt) {
		t.Fatalf("ack fields wrong: %x", pkt)
	}
	forged := append([]byte(nil), probe...)
	forged[9] ^= 1
	if r := hb.Prog.Run(&Input{Packet: forged, Now: now}); r.Action != XDPDrop {
		t.Fatalf("forged probe: action %d", r.Action)
	}
	// The ack is steered to the prober's queue, not the KTP stack.
	if r := hb.Prog.Run(&Input{Packet: pkt, Now: now}); r.Action != XDPRedirect || r.Queue == nil {
		t.Fatalf("ack: %+v", r)
	}
	if r := hb.Prog.Run(&Input{Packet: []byte{0x30, 1, 2}, Now: now}); r.Action != XDPPass {
		t.Fatal("ordinary packets must pass")
	}
}

func TestMaglevTable(t *testing.T) {
	names := []string{"b0", "b1", "b2", "b3", "b4"}
	t1 := MaglevTable(names)
	counts := make([]int, len(names))
	for _, o := range t1 {
		counts[o]++
	}
	for i, c := range counts {
		if c < MaglevSize/len(names)-2 || c > MaglevSize/len(names)+2 {
			t.Fatalf("backend %d owns %d slots, want ~%d", i, c, MaglevSize/len(names))
		}
	}
	// Remove b2: slots owned by the others mostly stay put.
	t2 := MaglevTable([]string{"b0", "b1", "b3", "b4"})
	moved := 0
	for slot, o := range t1 {
		if names[o] == "b2" {
			continue
		}
		if newName := []string{"b0", "b1", "b3", "b4"}[t2[slot]]; newName != names[o] {
			moved++
		}
	}
	if moved > MaglevSize/10 {
		t.Fatalf("removing one of five backends moved %d other slots", moved)
	}
}

// ---- end to end over simnet ----

func key(seed byte) ed25519.PrivateKey {
	s := make([]byte, ed25519.SeedSize)
	s[0] = seed
	return ed25519.NewKeyFromSeed(s)
}

func newEP(t *testing.T, n *simnet.Network, addr string, seed byte) *endpoint.Endpoint {
	t.Helper()
	pc, err := n.Listen(addr)
	if err != nil {
		t.Fatal(err)
	}
	ep, err := endpoint.New(pc, &ktp.Config{Identity: key(seed), ClusterKey: []byte("k"), HandshakeTimeout: time.Second},
		endpoint.Options{Resolve: simnet.Resolve})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ep.Close() })
	return ep
}

// A node answers heartbeats from its data path. When its host process
// stops checking in, it keeps answering -- and the acks say how stale it is.
func TestHeartbeatEndToEnd(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		n := simnet.New(3)
		n.SetDefaultLink(simnet.Link{Latency: 5 * time.Millisecond})
		a, b := newEP(t, n, "a", 1), newEP(t, n, "b", 2)
		hbA, _ := NewHeartbeat([]byte("k"))
		hbB, _ := NewHeartbeat([]byte("k"))
		for ep, hb := range map[*endpoint.Endpoint]*Heartbeat{a: hbA, b: hbB} {
			h := NewHook()
			h.AttachIngress(hb.Prog)
			ep.SetFilter(h)
		}
		p := NewProber(hbA, a.WriteRaw)
		defer p.Close()
		ack, err := p.Probe("b", time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if ack.RTT != 10*time.Millisecond || ack.Staleness > 10*time.Millisecond {
			t.Fatalf("fresh node: %+v", ack)
		}
		time.Sleep(3 * time.Second) // b's host stops touching
		ack, err = p.Probe("b", time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if ack.Staleness < 3*time.Second {
			t.Fatalf("stale host not reported: %+v", ack)
		}
		if st := b.FilterStats(); st.TX != 2 {
			t.Fatalf("responder TX count %+v", st)
		}
		// A probe keyed for another cluster gets no answer.
		other, _ := NewHeartbeat([]byte("someone else"))
		hook := NewHook()
		hook.AttachIngress(other.Prog)
		a.SetFilter(hook)
		p2 := NewProber(other, a.WriteRaw)
		defer p2.Close()
		if _, err := p2.Probe("b", 200*time.Millisecond); !errors.Is(err, ErrNoAck) {
			t.Fatalf("foreign probe: %v", err)
		}
	})
}

// A client dials a virtual address; the balancer steers the connection to a
// backend, which completes the KTP handshake and answers directly.
func TestMaglevDirectServerReturn(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		n := simnet.New(5)
		n.SetDefaultLink(simnet.Link{Latency: 2 * time.Millisecond})
		vip := newEP(t, n, "vip", 10)
		lb, err := NewMaglev(4)
		if err != nil {
			t.Fatal(err)
		}
		var names []string
		var addrs []net.Addr
		backends := map[string]*endpoint.Endpoint{}
		for i := 0; i < 3; i++ {
			name := fmt.Sprintf("backend%d", i)
			ep := newEP(t, n, name, byte(20+i))
			ep.TrustEncapsulation("vip")
			backends[name] = ep
			names = append(names, name)
			addrs = append(addrs, simnet.Addr(name))
		}
		lb.SetBackends(names, addrs)
		h := NewHook()
		h.AttachIngress(lb.Prog)
		vip.SetFilter(h)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		for name, ep := range backends {
			go func() {
				for {
					c, err := ep.Accept(ctx)
					if err != nil {
						return
					}
					go func() {
						st, err := c.AcceptStream(ctx)
						if err != nil {
							return
						}
						io.WriteString(st, "served by "+name)
						st.Close()
					}()
				}
			}()
		}
		served := map[string]bool{}
		for i := 0; i < 6; i++ {
			client := newEP(t, n, fmt.Sprintf("client%d", i), byte(40+i))
			c, err := client.Dial(ctx, "vip")
			if err != nil {
				t.Fatalf("client %d: dial through the balancer: %v", i, err)
			}
			st, _ := c.OpenStream(ctx)
			st.Write([]byte("hi"))
			reply, _ := io.ReadAll(st)
			want := "served by " + lb.Backend(fmt.Sprintf("client%d", i))
			if string(reply) != want {
				t.Fatalf("client %d got %q, want %q", i, reply, want)
			}
			served[string(reply)] = true
		}
		if len(served) < 2 {
			t.Fatalf("six clients all landed on one backend: %v", served)
		}
		if st := vip.FilterStats(); st.Redirected == 0 {
			t.Fatalf("balancer redirected nothing: %+v", st)
		}
		for _, ep := range backends {
			_ = ep.FilterStats()
		}
	})
}

func TestRandomProgramsTerminate(t *testing.T) {
	// Assembled random straight-line-with-forward-branches programs: the
	// verifier's acceptance means bounded execution.
	rng := rand.New(rand.NewPCG(9, 9))
	accepted := 0
	for i := 0; i < 2000; i++ {
		b := NewBuilder().Mov(R0, 0).Mov(R2, int32(rng.IntN(100)))
		for j := 0; j < 20; j++ {
			switch rng.IntN(4) {
			case 0:
				b.ALU64Imm(ALUOp(rng.IntN(13)<<4), R0, int32(rng.IntN(63)+1))
			case 1:
				b.JumpImm(JumpOp((rng.IntN(13)+1)<<4), R0, int32(rng.IntN(10)), "end")
			case 2:
				b.Load(B, R3, R1, int16(rng.IntN(48)))
			default:
				b.ALU64Reg(Add, R0, R2)
			}
		}
		insns, err := b.Label("end").Exit().Build()
		if err != nil {
			continue
		}
		l, err := Load(&Program{Insns: insns})
		if err != nil {
			continue
		}
		accepted++
		res := l.Run(&Input{Packet: []byte{1, 2, 3}})
		_ = res
		if st := l.Stats(); st.Insns > uint64(len(insns)) {
			t.Fatalf("ran %d instructions of a %d-instruction program", st.Insns, len(insns))
		}
	}
	if accepted < 100 {
		t.Fatalf("only %d random programs verified", accepted)
	}
}
