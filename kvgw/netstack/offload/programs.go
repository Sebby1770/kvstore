package offload

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"hash/fnv"
	"net"
	"sort"
	"sync"
	"time"
)

// typeNibble loads the packet's type nibble (first byte >> 4) into dst, or
// jumps to empty if the packet has no bytes. Uses r2, r3.
func typeNibble(b *Builder, ctx, dst Reg, empty string) {
	b.Load(DW, R2, ctx, CtxData).
		Load(DW, R3, ctx, CtxDataEnd).
		MovReg(dst, R2).ALU64Imm(Add, dst, 1).
		JumpReg(Jgt, dst, R3, empty).
		Load(B, dst, R2, 0).
		ALU64Imm(Rsh, dst, 4)
}

// ---- distributed rate limiter ----

// RateLimiter drops packets from peers that exceed a token-bucket rate. It
// meters only the packet types in its mask (by default Initial and Raw, i.e.
// new-connection attempts and connectionless gossip -- what a flood is made
// of), so established connections are never throttled mid-stream.
//
// "Distributed": a cluster-wide budget is enforced by giving each node a
// share of it; SetRate is meant to be called by the control plane whenever
// membership changes (see Share). The buckets themselves live in an LRU map
// in the data path, so a flood of spoofed sources evicts old entries rather
// than exhausting memory.
type RateLimiter struct {
	Prog    *Loaded
	config  *ArrayMap // [0] rate per second, [8] burst, [16] type mask
	buckets *HashMap  // peer key -> [0] milli-tokens, [8] last refill ns
}

// Packet types (wire.PacketType values) a RateLimiter meters by default.
const (
	typeInitial = 1
	typeRaw     = 4
	typeReset   = 5
)

// NewRateLimiter builds the limiter program. types are packet type nibbles
// to meter (default Initial and Raw); rate is packets per second per peer,
// burst the bucket depth (at least 1).
func NewRateLimiter(rate, burst uint64, types ...int) (*RateLimiter, error) {
	rl := &RateLimiter{config: NewArrayMap("rl_config", 24, 1), buckets: NewHashMap("rl_buckets", 8, 16, 65536, true)}
	if len(types) == 0 {
		types = []int{typeInitial, typeRaw}
	}
	var mask uint64
	for _, t := range types {
		mask |= 1 << uint(t&0xf)
	}
	rl.config.SetU64(0, 16, mask)
	rl.SetRate(rate, burst)

	b := NewBuilder()
	b.MovReg(R6, R1)
	typeNibble(b, R6, R7, "pass")
	// config
	b.StoreImm(W, R10, -4, 0).
		LoadMap(R1, 0).MovReg(R2, R10).ALU64Imm(Add, R2, -4).Call(HelperMapLookup).
		JumpImm(Jeq, R0, 0, "pass").
		MovReg(R8, R0).
		Load(DW, R1, R8, 16).ALU64Reg(Rsh, R1, R7).ALU64Imm(And, R1, 1).
		JumpImm(Jeq, R1, 0, "pass")
	// this peer's bucket
	b.Load(DW, R1, R6, CtxSrcHash).Store(DW, R10, -16, R1).
		LoadMap(R1, 1).MovReg(R2, R10).ALU64Imm(Add, R2, -16).Call(HelperMapLookup).
		Load(DW, R9, R6, CtxNow).
		JumpImm(Jne, R0, 0, "have")
	// first packet from this peer: a full bucket, minus this packet
	b.Load(DW, R1, R8, 8).ALU64Imm(Mul, R1, 1000).ALU64Imm(Sub, R1, 1000).
		Store(DW, R10, -32, R1).Store(DW, R10, -24, R9).
		LoadMap(R1, 1).MovReg(R2, R10).ALU64Imm(Add, R2, -16).
		MovReg(R3, R10).ALU64Imm(Add, R3, -32).Mov(R4, UpdateAny).Call(HelperMapUpdate).
		Goto("pass")
	// refill: elapsed (capped at 10s) * rate / 1e6 milli-tokens, capped at burst
	b.Label("have").
		MovReg(R7, R0).
		Load(DW, R1, R7, 8).
		MovReg(R2, R9).ALU64Reg(Sub, R2, R1).
		LoadImm64(R3, uint64(10*time.Second)).
		JumpReg(Jle, R2, R3, "nocap").
		MovReg(R2, R3).
		Label("nocap").
		Load(DW, R1, R8, 0).ALU64Reg(Mul, R2, R1).ALU64Imm(Div, R2, 1000000).
		Load(DW, R3, R7, 0).ALU64Reg(Add, R3, R2).
		Load(DW, R4, R8, 8).ALU64Imm(Mul, R4, 1000).
		JumpReg(Jle, R3, R4, "capped").
		MovReg(R3, R4).
		Label("capped").
		Store(DW, R7, 8, R9).
		JumpImm(Jlt, R3, 1000, "drop").
		ALU64Imm(Sub, R3, 1000).
		Store(DW, R7, 0, R3).
		Label("pass").Return(XDPPass).
		Label("drop").Store(DW, R7, 0, R3).Return(XDPDrop)
	insns, err := b.Build()
	if err != nil {
		return nil, err
	}
	rl.Prog, err = Load(&Program{Name: "rate_limiter", Insns: insns, Maps: []Map{rl.config, rl.buckets}})
	return rl, err
}

// SetRate changes the per-peer rate and burst; buckets keep their tokens.
func (rl *RateLimiter) SetRate(rate, burst uint64) {
	rl.config.SetU64(0, 0, rate)
	rl.config.SetU64(0, 8, max(burst, 1))
}

// Rate returns the current rate and burst.
func (rl *RateLimiter) Rate() (rate, burst uint64) { return rl.config.U64(0, 0), rl.config.U64(0, 8) }

// Tracked is how many peers currently have a bucket.
func (rl *RateLimiter) Tracked() int { return rl.buckets.Len() }

// Share divides a cluster-wide rate among nodes, rounding up so the
// cluster never enforces less than the budget.
func Share(global uint64, nodes int) uint64 {
	if nodes <= 1 {
		return global
	}
	return (global + uint64(nodes) - 1) / uint64(nodes)
}

// ---- circuit breaker ----

// Breaker states (the first word of a breaker entry).
const (
	BreakerClosed   = 0
	BreakerOpen     = 1
	BreakerHalfOpen = 2
)

// CircuitBreaker sheds traffic to peers that keep failing. Failures are
// counted per peer in a shared map -- by the ingress program, from stateless
// resets (the peer has lost our connection state), and by the host, from
// call errors (Failure). At Threshold the circuit opens and the egress
// program drops every packet to that peer; once Cooldown has passed it lets
// a single packet through as a probe (half-open), and any sign of health --
// a non-reset packet from the peer, or a host Success -- closes it again.
// The host can consult Open before even trying, so callers fail fast.
type CircuitBreaker struct {
	Ingress, Egress *Loaded
	config          *ArrayMap // [0] threshold, [8] cooldown ns
	state           *HashMap  // peer key -> [0] state, [8] failures, [16] opened/probed at ns
	clock           func() int64
}

// NewCircuitBreaker builds the breaker programs.
func NewCircuitBreaker(threshold uint64, cooldown time.Duration) (*CircuitBreaker, error) {
	cb := &CircuitBreaker{config: NewArrayMap("cb_config", 16, 1), state: NewHashMap("cb_state", 8, 24, 16384, true),
		clock: func() int64 { return time.Now().UnixNano() }}
	cb.config.SetU64(0, 0, max(threshold, 1))
	cb.config.SetU64(0, 8, uint64(cooldown))

	// Ingress: observe.
	b := NewBuilder()
	b.MovReg(R6, R1)
	typeNibble(b, R6, R7, "pass")
	b.Load(DW, R1, R6, CtxSrcHash).Store(DW, R10, -8, R1).
		LoadMap(R1, 1).MovReg(R2, R10).ALU64Imm(Add, R2, -8).Call(HelperMapLookup).
		JumpImm(Jne, R7, typeReset, "healthy").
		// A stateless reset: one more failure.
		JumpImm(Jne, R0, 0, "count").
		StoreImm(DW, R10, -32, BreakerClosed).StoreImm(DW, R10, -24, 0).StoreImm(DW, R10, -16, 0).
		LoadMap(R1, 1).MovReg(R2, R10).ALU64Imm(Add, R2, -8).
		MovReg(R3, R10).ALU64Imm(Add, R3, -32).Mov(R4, UpdateAny).Call(HelperMapUpdate).
		LoadMap(R1, 1).MovReg(R2, R10).ALU64Imm(Add, R2, -8).Call(HelperMapLookup).
		JumpImm(Jeq, R0, 0, "pass").
		Label("count").
		MovReg(R8, R0).
		Load(DW, R9, R8, 8).ALU64Imm(Add, R9, 1).Store(DW, R8, 8, R9).
		StoreImm(W, R10, -36, 0).
		LoadMap(R1, 0).MovReg(R2, R10).ALU64Imm(Add, R2, -36).Call(HelperMapLookup).
		JumpImm(Jeq, R0, 0, "pass").
		Load(DW, R1, R0, 0).
		JumpReg(Jlt, R9, R1, "pass").
		Load(DW, R1, R8, 0).JumpImm(Jne, R1, BreakerClosed, "pass").
		StoreImm(DW, R8, 0, BreakerOpen).
		Load(DW, R1, R6, CtxNow).Store(DW, R8, 16, R1).
		Goto("pass").
		// Any other packet: the peer is alive and has our state.
		Label("healthy").
		JumpImm(Jeq, R0, 0, "pass").
		StoreImm(DW, R0, 8, 0).
		Load(DW, R1, R0, 0).JumpImm(Jne, R1, BreakerHalfOpen, "pass").
		StoreImm(DW, R0, 0, BreakerClosed).
		Label("pass").Return(XDPPass)
	in, err := b.Build()
	if err != nil {
		return nil, err
	}
	if cb.Ingress, err = Load(&Program{Name: "breaker_observe", Insns: in, Maps: []Map{cb.config, cb.state}}); err != nil {
		return nil, err
	}

	// Egress: enforce.
	e := NewBuilder()
	e.MovReg(R6, R1).
		Load(DW, R1, R6, CtxSrcHash).Store(DW, R10, -8, R1).
		LoadMap(R1, 1).MovReg(R2, R10).ALU64Imm(Add, R2, -8).Call(HelperMapLookup).
		JumpImm(Jeq, R0, 0, "pass").
		MovReg(R7, R0).
		Load(DW, R1, R7, 0).JumpImm(Jeq, R1, BreakerClosed, "pass").
		// Open or half-open: drop until a cooldown has passed since the
		// circuit opened or the last probe went out, then probe once.
		Load(DW, R8, R6, CtxNow).
		MovReg(R2, R8).Load(DW, R3, R7, 16).ALU64Reg(Sub, R2, R3).
		StoreImm(W, R10, -12, 0).
		MovReg(R9, R2).
		LoadMap(R1, 0).MovReg(R2, R10).ALU64Imm(Add, R2, -12).Call(HelperMapLookup).
		JumpImm(Jeq, R0, 0, "pass").
		Load(DW, R4, R0, 8).
		JumpReg(Jlt, R9, R4, "drop").
		StoreImm(DW, R7, 0, BreakerHalfOpen).
		Store(DW, R7, 16, R8).
		Label("pass").Return(XDPPass).
		Label("drop").Return(XDPDrop)
	eg, err := e.Build()
	if err != nil {
		return nil, err
	}
	cb.Egress, err = Load(&Program{Name: "breaker_enforce", Insns: eg, Maps: []Map{cb.config, cb.state}})
	return cb, err
}

// SetClock sets the time source for host-side transitions (tests).
func (cb *CircuitBreaker) SetClock(f func() int64) { cb.clock = f }

func (cb *CircuitBreaker) entry(addr string) []byte {
	return cb.state.lookupLocked(U64(PeerKeyString(addr)))
}

// Failure records a host-observed failure (a call error) for addr.
func (cb *CircuitBreaker) Failure(addr string) {
	key := U64(PeerKeyString(addr))
	cb.state.mu.Lock()
	defer cb.state.mu.Unlock()
	v := cb.state.lookupLocked(key)
	if v == nil {
		cb.state.updateLocked(key, make([]byte, 24), UpdateAny)
		v = cb.state.lookupLocked(key)
	}
	fails := binary.LittleEndian.Uint64(v[8:]) + 1
	binary.LittleEndian.PutUint64(v[8:], fails)
	if fails >= cb.config.U64(0, 0) && binary.LittleEndian.Uint64(v) == BreakerClosed {
		binary.LittleEndian.PutUint64(v, BreakerOpen)
		binary.LittleEndian.PutUint64(v[16:], uint64(cb.clock()))
	}
}

// Success records a host-observed success: failures reset and a half-open
// circuit closes.
func (cb *CircuitBreaker) Success(addr string) {
	cb.state.mu.Lock()
	defer cb.state.mu.Unlock()
	if v := cb.entry(addr); v != nil {
		binary.LittleEndian.PutUint64(v[8:], 0)
		if binary.LittleEndian.Uint64(v) == BreakerHalfOpen {
			binary.LittleEndian.PutUint64(v, BreakerClosed)
		}
	}
}

// State reports addr's circuit state and failure count.
func (cb *CircuitBreaker) State(addr string) (state, failures uint64) {
	cb.state.mu.Lock()
	defer cb.state.mu.Unlock()
	if v := cb.entry(addr); v != nil {
		return binary.LittleEndian.Uint64(v), binary.LittleEndian.Uint64(v[8:])
	}
	return BreakerClosed, 0
}

// Open reports whether calls to addr should fail fast now.
func (cb *CircuitBreaker) Open(addr string) bool {
	s, _ := cb.State(addr)
	return s == BreakerOpen
}

// ---- heartbeat responder ----

// Heartbeat probes are 48-byte datagrams outside the KTP packet space
// (type nibble 0xE):
//
//	 0  1  0xE0 probe | 0xE1 ack
//	 1  7  zero
//	 8  8  sequence number
//	16  8  prober's send time (ns)
//	24  8  ack: how long ago (ns) the responder's host process last checked in
//	32  8  ack: responder load (permille)
//	40  8  MAC64 over bytes 0..40 (the ack fields are zero in a probe)
const (
	HeartbeatSize = 48
	hbProbe       = 0xE0
	hbAck         = 0xE1
)

// Heartbeat answers authenticated liveness probes from the data path,
// before the KTP stack, and steers acks for this node's own Prober to a user
// queue. Because the reply comes from the packet path it measures the
// node's network reachability; because it carries the host process's
// staleness it also says whether the application is still making progress.
// A node that answers promptly but reports a stale host is hung, not dead --
// the distinction timeout-based detectors cannot draw.
type Heartbeat struct {
	Prog   *Loaded
	key    *ArrayMap // [0] 32-byte MAC key
	status *ArrayMap // [0] host last check-in ns, [8] load permille
	acks   *QueueMap
	clock  func() int64
}

// NewHeartbeat builds the responder; secret keys the probe MACs (use the
// cluster key, so only members can probe).
func NewHeartbeat(secret []byte) (*Heartbeat, error) {
	k := sha256.Sum256(append([]byte("kvstore heartbeat v1|"), secret...))
	hb := &Heartbeat{key: NewArrayMap("hb_key", 32, 1), status: NewArrayMap("hb_status", 16, 1),
		acks: NewQueueMap("hb_acks", 1, 256), clock: func() int64 { return time.Now().UnixNano() }}
	hb.key.Update(U32(0), k[:])
	hb.Touch()

	b := NewBuilder()
	b.MovReg(R6, R1).
		Load(DW, R7, R6, CtxData).Load(DW, R3, R6, CtxDataEnd).
		MovReg(R4, R7).ALU64Imm(Add, R4, HeartbeatSize).
		JumpReg(Jne, R4, R3, "pass").
		Load(B, R1, R7, 0).
		JumpImm(Jne, R1, hbProbe, "acks").
		// A probe: authenticate it, fill in the ack, re-MAC, send it back.
		Load(DW, R8, R7, 40).
		LoadMap(R1, 0).MovReg(R2, R7).Mov(R3, 40).Call(HelperMAC64).
		JumpReg(Jne, R0, R8, "drop").
		StoreImm(W, R10, -4, 0).
		LoadMap(R1, 1).MovReg(R2, R10).ALU64Imm(Add, R2, -4).Call(HelperMapLookup).
		JumpImm(Jeq, R0, 0, "drop").
		Load(DW, R1, R0, 0).Load(DW, R2, R6, CtxNow).ALU64Reg(Sub, R2, R1).Store(DW, R7, 24, R2).
		Load(DW, R1, R0, 8).Store(DW, R7, 32, R1).
		StoreImm(B, R7, 0, hbAck).
		LoadMap(R1, 0).MovReg(R2, R7).Mov(R3, 40).Call(HelperMAC64).
		Store(DW, R7, 40, R0).
		Return(XDPTX).
		// An ack for our prober: authenticate, then steer to the user queue.
		Label("acks").
		JumpImm(Jne, R1, hbAck, "pass").
		Load(DW, R8, R7, 40).
		LoadMap(R1, 0).MovReg(R2, R7).Mov(R3, 40).Call(HelperMAC64).
		JumpReg(Jne, R0, R8, "drop").
		LoadMap(R1, 2).Mov(R2, 0).Mov(R3, XDPDrop).Call(HelperRedirectMap).
		Exit().
		Label("drop").Return(XDPDrop).
		Label("pass").Return(XDPPass)
	insns, err := b.Build()
	if err != nil {
		return nil, err
	}
	hb.Prog, err = Load(&Program{Name: "heartbeat", Insns: insns, Maps: []Map{hb.key, hb.status, hb.acks}})
	return hb, err
}

// SetClock sets the time source (tests); the program's own clock is the
// hook's.
func (hb *Heartbeat) SetClock(f func() int64) { hb.clock = f }

// Touch records that the host process is making progress. Call it from the
// loop whose liveness matters (the node's housekeeping loop, say).
func (hb *Heartbeat) Touch() { hb.status.SetU64(0, 0, uint64(hb.clock())) }

// SetLoad publishes the host's load (permille) in acks.
func (hb *Heartbeat) SetLoad(permille uint64) { hb.status.SetU64(0, 8, min(permille, 1000)) }

func (hb *Heartbeat) mac(p []byte) uint64 {
	k, _ := hb.key.Lookup(U32(0))
	return MAC64(k, p[:40])
}

// Ack is a decoded heartbeat reply.
type Ack struct {
	Seq       uint64
	RTT       time.Duration
	Staleness time.Duration // how long since the peer's host checked in
	Load      uint64
	From      net.Addr
}

// Prober sends heartbeat probes through an endpoint and matches the acks
// the Heartbeat program steers to its queue.
type Prober struct {
	hb    *Heartbeat
	send  func(pkt []byte, addr string) error
	mu    sync.Mutex
	seq   uint64
	wait  map[uint64]chan Ack
	done  chan struct{}
	once  sync.Once
	clock func() int64
}

// NewProber starts a prober; send writes a raw datagram (endpoint.WriteRaw).
func NewProber(hb *Heartbeat, send func(pkt []byte, addr string) error) *Prober {
	p := &Prober{hb: hb, send: send, wait: map[uint64]chan Ack{}, done: make(chan struct{}), clock: hb.clock}
	go p.loop()
	return p
}

func (p *Prober) loop() {
	q := p.hb.acks.Queue(0)
	for {
		select {
		case <-p.done:
			return
		case pkt := <-q:
			d := pkt.Data
			if len(d) != HeartbeatSize {
				continue
			}
			a := Ack{Seq: binary.LittleEndian.Uint64(d[8:]), Staleness: time.Duration(binary.LittleEndian.Uint64(d[24:])),
				Load: binary.LittleEndian.Uint64(d[32:]), From: pkt.From,
				RTT: time.Duration(p.clock() - int64(binary.LittleEndian.Uint64(d[16:])))}
			p.mu.Lock()
			ch := p.wait[a.Seq]
			delete(p.wait, a.Seq)
			p.mu.Unlock()
			if ch != nil {
				ch <- a
			}
		}
	}
}

// ErrNoAck reports a probe that went unanswered.
var ErrNoAck = errors.New("offload: heartbeat probe not answered")

// Probe sends one probe to addr and waits up to timeout for the ack.
func (p *Prober) Probe(addr string, timeout time.Duration) (Ack, error) {
	p.mu.Lock()
	p.seq++
	seq := p.seq
	ch := make(chan Ack, 1)
	p.wait[seq] = ch
	p.mu.Unlock()
	pkt := make([]byte, HeartbeatSize)
	pkt[0] = hbProbe
	binary.LittleEndian.PutUint64(pkt[8:], seq)
	binary.LittleEndian.PutUint64(pkt[16:], uint64(p.clock()))
	binary.LittleEndian.PutUint64(pkt[40:], p.hb.mac(pkt))
	defer func() {
		p.mu.Lock()
		delete(p.wait, seq)
		p.mu.Unlock()
	}()
	if err := p.send(pkt, addr); err != nil {
		return Ack{}, err
	}
	t := time.NewTimer(timeout)
	defer t.Stop()
	select {
	case a := <-ch:
		return a, nil
	case <-t.C:
		return Ack{}, ErrNoAck
	case <-p.done:
		return Ack{}, ErrNoAck
	}
}

// Close stops the prober.
func (p *Prober) Close() { p.once.Do(func() { close(p.done) }) }

// ---- Maglev load balancer ----

// MaglevSize is the lookup table size (prime, as Maglev requires).
const MaglevSize = 1021

// Maglev steers connections arriving at a virtual address to backends with
// Maglev consistent hashing (Eisenbud et al., NSDI 2016): a lookup table in
// which every backend owns a near-equal share of slots, filled from
// per-backend permutations so that adding or removing a backend moves
// almost only the slots it gains or loses. The data-path program hashes the
// flow (the client's address) into the table and redirects the packet --
// encapsulated, so the backend learns the client and answers it directly
// (direct server return). Backends must TrustEncapsulation the balancer.
type Maglev struct {
	Prog     *Loaded
	table    *ArrayMap // slot -> backend index
	backends *AddrMap
	mu       sync.Mutex
	names    []string
}

// NewMaglev builds the balancer for up to maxBackends backends.
func NewMaglev(maxBackends int) (*Maglev, error) {
	m := &Maglev{table: NewArrayMap("maglev_table", 8, MaglevSize), backends: NewAddrMap("maglev_backends", maxBackends)}
	b := NewBuilder()
	b.MovReg(R6, R1)
	typeNibble(b, R6, R7, "pass")
	// Steer KTP connection packets (Initial, Handshake, 1-RTT); leave gossip
	// and everything else to the local stack.
	b.JumpImm(Jeq, R7, 0, "pass").JumpImm(Jgt, R7, 3, "pass").
		Load(DW, R1, R6, CtxSrcHash).ALU64Imm(Mod, R1, MaglevSize).Store(W, R10, -4, R1).
		LoadMap(R1, 0).MovReg(R2, R10).ALU64Imm(Add, R2, -4).Call(HelperMapLookup).
		JumpImm(Jeq, R0, 0, "pass").
		Load(DW, R2, R0, 0).
		LoadMap(R1, 1).Mov(R3, XDPPass).Call(HelperRedirectMap).
		Exit().
		Label("pass").Return(XDPPass)
	insns, err := b.Build()
	if err != nil {
		return nil, err
	}
	m.Prog, err = Load(&Program{Name: "maglev", Insns: insns, Maps: []Map{m.table, m.backends}})
	return m, err
}

func maglevHash(s string, seed byte) uint64 {
	h := fnv.New64a()
	h.Write([]byte{seed})
	h.Write([]byte(s))
	return h.Sum64()
}

// MaglevTable computes the lookup table for named backends (the paper's
// population algorithm): entry i is the index of the backend owning slot i.
func MaglevTable(names []string) []int {
	t := make([]int, MaglevSize)
	for i := range t {
		t[i] = -1
	}
	if len(names) == 0 {
		return t
	}
	n := len(names)
	offset, skip, next := make([]uint64, n), make([]uint64, n), make([]uint64, n)
	for i, name := range names {
		offset[i] = maglevHash(name, 1) % MaglevSize
		skip[i] = maglevHash(name, 2)%(MaglevSize-1) + 1
	}
	filled := 0
	for {
		for i := 0; i < n; i++ {
			c := (offset[i] + next[i]*skip[i]) % MaglevSize
			for t[c] >= 0 {
				next[i]++
				c = (offset[i] + next[i]*skip[i]) % MaglevSize
			}
			t[c] = i
			next[i]++
			filled++
			if filled == MaglevSize {
				return t
			}
		}
	}
}

// SetBackends installs the backend set (names must be stable identities;
// the table is a function of them, not of their order).
func (m *Maglev) SetBackends(names []string, addrs []net.Addr) error {
	if len(names) != len(addrs) || len(names) > len(m.backends.addrs) {
		return errors.New("offload: backend names and addresses mismatch or exceed the table")
	}
	idx := make([]int, len(names))
	for i := range idx {
		idx[i] = i
	}
	sort.Slice(idx, func(a, b int) bool { return names[idx[a]] < names[idx[b]] })
	sorted := make([]string, len(names))
	for i, j := range idx {
		sorted[i] = names[j]
	}
	table := MaglevTable(sorted)
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := range m.backends.addrs {
		m.backends.Set(i, nil)
	}
	for i, j := range idx {
		m.backends.Set(i, addrs[j])
	}
	for slot, owner := range table {
		v := make([]byte, 8)
		if owner < 0 {
			binary.LittleEndian.PutUint64(v, ^uint64(0)) // no backend: redirect falls back to PASS
		} else {
			binary.LittleEndian.PutUint64(v, uint64(owner))
		}
		m.table.Update(U32(uint32(slot)), v)
	}
	m.names = sorted
	return nil
}

// Backend returns the backend name a peer address steers to.
func (m *Maglev) Backend(peer string) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.names) == 0 {
		return ""
	}
	v, _ := m.table.Lookup(U32(uint32(PeerKeyString(peer) % MaglevSize)))
	i := binary.LittleEndian.Uint64(v)
	if i >= uint64(len(m.names)) {
		return ""
	}
	return m.names[i]
}
