package offload

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"sort"
	"sync"
)

// MaxInsns bounds a program (the kernel's classic limit).
const MaxInsns = 4096

// StackSize is the program stack, addressed downward from r10.
const StackSize = 512

// Context field offsets (the program's view of r1; read-only, 8-byte
// fields). data / data_end are packet pointers, as in XDP.
const (
	CtxData    = 0
	CtxDataEnd = 8
	CtxSrcHash = 16 // hash of the peer address (source on ingress, destination on egress)
	CtxNow     = 24 // nanoseconds, the same value ktime_get_ns returns
	CtxLen     = 32
	CtxDir     = 40 // 0 ingress, 1 egress
	ctxSize    = 48
)

// Program is unverified bytecode plus the maps it references by index.
type Program struct {
	Name  string
	Insns []Insn
	Maps  []Map
}

// helper arity: how many argument registers must be initialised.
var helperArgs = map[int32]int{
	HelperMapLookup: 2, HelperMapUpdate: 4, HelperMapDelete: 2,
	HelperKtimeGetNs: 0, HelperPrandomU32: 0, HelperRedirectMap: 3, HelperMAC64: 3,
}

// VerifyError locates a rejected instruction.
type VerifyError struct {
	PC  int
	Msg string
}

func (e *VerifyError) Error() string {
	return fmt.Sprintf("offload: verifier: insn %d: %s", e.PC, e.Msg)
}

const allRegs = 1<<11 - 1

// Verify checks that the program terminates and is well formed. See the
// package documentation for what it proves and what the interpreter checks
// at run time instead.
func (p *Program) Verify() error {
	n := len(p.Insns)
	if n == 0 || n > MaxInsns {
		return &VerifyError{0, fmt.Sprintf("program has %d instructions (1..%d)", n, MaxInsns)}
	}
	bad := func(pc int, f string, a ...any) error { return &VerifyError{pc, fmt.Sprintf(f, a...)} }
	// init[pc]: registers initialised on every path reaching pc; reached
	// marks instructions with at least one predecessor.
	init := make([]uint16, n)
	reached := make([]bool, n)
	second := make([]bool, n) // second slot of an LD_IMM64
	init[0], reached[0] = 1<<R1|1<<R10, true
	flow := func(from, to int, regs uint16) error {
		if to >= n {
			return bad(from, "falls off the end of the program")
		}
		if to <= from {
			return bad(from, "backward jump (loops are not allowed)")
		}
		if second[to] {
			return bad(from, "jump into the middle of a 64-bit load")
		}
		if reached[to] {
			init[to] &= regs
		} else {
			init[to], reached[to] = regs, true
		}
		return nil
	}
	for pc := 0; pc < n; pc++ {
		if second[pc] {
			continue
		}
		in := p.Insns[pc]
		if !reached[pc] {
			return bad(pc, "unreachable instruction")
		}
		regs := init[pc]
		has := func(r Reg) bool { return r <= R10 && regs&(1<<r) != 0 }
		if in.Dst > R10 || in.Src > R10 {
			return bad(pc, "no such register")
		}
		writes := func(r Reg) error {
			if r == R10 {
				return bad(pc, "r10 is read-only")
			}
			regs |= 1 << r
			return nil
		}
		next := true
		switch in.class() {
		case ClassLD:
			if in.Op != opLdImm64 {
				return bad(pc, "only 64-bit immediate loads are supported in class LD")
			}
			if pc+1 >= n {
				return bad(pc, "truncated 64-bit load")
			}
			if hi := p.Insns[pc+1]; hi.Op != 0 || hi.Dst != 0 || hi.Src != 0 || hi.Off != 0 {
				return bad(pc+1, "malformed second half of a 64-bit load")
			}
			switch in.Src {
			case 0:
			case pseudoMapFD:
				if in.Imm < 0 || int(in.Imm) >= len(p.Maps) || p.Maps[in.Imm] == nil {
					return bad(pc, "reference to map %d, program has %d", in.Imm, len(p.Maps))
				}
			default:
				return bad(pc, "unknown 64-bit load source %d", in.Src)
			}
			if err := writes(in.Dst); err != nil {
				return err
			}
			second[pc+1] = true
			if err := flow(pc, pc+2, regs); err != nil {
				return err
			}
			next = false
		case ClassALU, ClassALU64:
			op := ALUOp(in.Op & 0xf0)
			if op > Arsh || in.Op&0x08 != 0 && op == Neg {
				return bad(pc, "invalid ALU operation %#x", in.Op)
			}
			if in.Op&srcX != 0 && op != Neg && !has(in.Src) {
				return bad(pc, "read of uninitialised r%d", in.Src)
			}
			if op != Mov && !has(in.Dst) {
				return bad(pc, "read of uninitialised r%d", in.Dst)
			}
			if in.Op&srcX == 0 {
				if (op == Div || op == Mod) && in.Imm == 0 {
					return bad(pc, "division by constant zero")
				}
				width := int32(64)
				if in.class() == ClassALU {
					width = 32
				}
				if (op == Lsh || op == Rsh || op == Arsh) && (in.Imm < 0 || in.Imm >= width) {
					return bad(pc, "shift by %d", in.Imm)
				}
			}
			if err := writes(in.Dst); err != nil {
				return err
			}
		case ClassLDX:
			if in.Op&0xe0 != ModeMEM {
				return bad(pc, "unsupported load mode %#x", in.Op)
			}
			if !has(in.Src) {
				return bad(pc, "load through uninitialised r%d", in.Src)
			}
			if err := writes(in.Dst); err != nil {
				return err
			}
		case ClassST:
			if in.Op&0xe0 != ModeMEM {
				return bad(pc, "unsupported store mode %#x", in.Op)
			}
			if !has(in.Dst) {
				return bad(pc, "store through uninitialised r%d", in.Dst)
			}
		case ClassSTX:
			switch in.Op & 0xe0 {
			case ModeMEM:
			case ModeATOMIC:
				if sz := Size(in.Op & 0x18); (sz != W && sz != DW) || in.Imm != int32(Add) {
					return bad(pc, "only 32/64-bit atomic add is supported")
				}
			default:
				return bad(pc, "unsupported store mode %#x", in.Op)
			}
			if !has(in.Dst) || !has(in.Src) {
				return bad(pc, "store with uninitialised registers")
			}
		case ClassJMP, ClassJMP32:
			op := JumpOp(in.Op & 0xf0)
			switch {
			case op == exit:
				if in.class() != ClassJMP {
					return bad(pc, "exit in JMP32 class")
				}
				if !has(R0) {
					return bad(pc, "exit with r0 uninitialised")
				}
				next = false
			case op == call:
				args, ok := helperArgs[in.Imm]
				if !ok || in.class() != ClassJMP {
					return bad(pc, "unknown helper %d", in.Imm)
				}
				for r := R1; r <= Reg(args); r++ {
					if !has(r) {
						return bad(pc, "helper %d called with r%d uninitialised", in.Imm, r)
					}
				}
				regs &^= 1<<R1 | 1<<R2 | 1<<R3 | 1<<R4 | 1<<R5
				regs |= 1 << R0
			case op == Ja:
				if in.class() != ClassJMP {
					return bad(pc, "ja in JMP32 class")
				}
				if err := flow(pc, pc+1+int(in.Off), regs); err != nil {
					return err
				}
				next = false
			case op > Jsle:
				return bad(pc, "invalid jump %#x", in.Op)
			default:
				if !has(in.Dst) || in.Op&srcX != 0 && !has(in.Src) {
					return bad(pc, "comparison of uninitialised registers")
				}
				if err := flow(pc, pc+1+int(in.Off), regs); err != nil {
					return err
				}
			}
		default:
			return bad(pc, "unknown instruction class")
		}
		if next {
			if err := flow(pc, pc+1, regs); err != nil {
				return err
			}
		}
	}
	return nil
}

// ---- execution ----

// Loaded is a verified program ready to run.
type Loaded struct {
	prog  *Program
	maps  []*mapBase // sorted, for deadlock-free locking
	mu    sync.Mutex
	rng   *rand.Rand
	Clock func() int64 // nanoseconds; the run's ktime
	stats RunStats
}

// RunStats counts a program's executions.
type RunStats struct {
	Runs     uint64            `json:"runs"`
	Verdicts map[string]uint64 `json:"verdicts"`
	Faults   uint64            `json:"faults"` // runtime memory faults (aborted runs)
	Insns    uint64            `json:"insns"`  // instructions executed
}

// Load verifies p and prepares it to run.
func Load(p *Program) (*Loaded, error) {
	if err := p.Verify(); err != nil {
		return nil, err
	}
	l := &Loaded{prog: p, rng: rand.New(rand.NewPCG(uint64(len(p.Insns)), 1)),
		stats: RunStats{Verdicts: map[string]uint64{}}}
	seen := map[*mapBase]bool{}
	for _, m := range p.Maps {
		if b := m.base(); !seen[b] {
			seen[b] = true
			l.maps = append(l.maps, b)
		}
	}
	sort.Slice(l.maps, func(i, j int) bool { return l.maps[i].id < l.maps[j].id })
	return l, nil
}

// Name is the program's name.
func (l *Loaded) Name() string { return l.prog.Name }

// Bytecode is the program in kernel encoding.
func (l *Loaded) Bytecode() []byte { return Encode(l.prog.Insns) }

// Stats snapshots the run counters.
func (l *Loaded) Stats() RunStats {
	l.mu.Lock()
	defer l.mu.Unlock()
	s := l.stats
	s.Verdicts = make(map[string]uint64, len(l.stats.Verdicts))
	for k, v := range l.stats.Verdicts {
		s.Verdicts[k] = v
	}
	return s
}

// Input is one packet presented to a program.
type Input struct {
	Packet  []byte // may be rewritten in place
	PeerKey uint64 // CtxSrcHash
	Egress  bool
	Now     int64 // nanoseconds (0 = use Clock)
}

// Result is a program's decision.
type Result struct {
	Action   int      // XDP action from r0
	Redirect net.Addr // AddrMap target, for XDPRedirect
	Queue    *QueueMap
	QueueIdx uint32
}

// errFault is a runtime memory or helper fault: the run aborts.
var errFault = errors.New("offload: memory fault")

// pointer tags (bits 32..47 of a register holding a pointer)
const (
	tagCtx   = 1
	tagPkt   = 2
	tagStack = 3
	tagMap   = 4
	tagVal   = 8 // tagVal+k: the k-th map value obtained in this run
	maxVals  = 64
)

func mkptr(tag, off uint64) uint64 { return tag<<32 | off&0xffffffff }

type run struct {
	l     *Loaded
	in    *Input
	ctx   [ctxSize]byte
	stack [StackSize]byte
	vals  [][]byte
	res   Result
	now   int64
}

func (r *run) region(tag uint64) []byte {
	switch {
	case tag == tagCtx:
		return r.ctx[:]
	case tag == tagPkt:
		return r.in.Packet
	case tag == tagStack:
		return r.stack[:]
	case tag >= tagVal && tag-tagVal < uint64(len(r.vals)):
		return r.vals[tag-tagVal]
	}
	return nil
}

func (r *run) mem(addr uint64, n int, write bool) ([]byte, error) {
	tag, off := addr>>32, addr&0xffffffff
	if write && tag == tagCtx {
		return nil, errFault // the context is read-only
	}
	reg := r.region(tag)
	if reg == nil || off+uint64(n) > uint64(len(reg)) {
		return nil, errFault
	}
	return reg[off : off+uint64(n)], nil
}

func load(b []byte) uint64 {
	switch len(b) {
	case 1:
		return uint64(b[0])
	case 2:
		return uint64(binary.LittleEndian.Uint16(b))
	case 4:
		return uint64(binary.LittleEndian.Uint32(b))
	}
	return binary.LittleEndian.Uint64(b)
}

func store(b []byte, v uint64) {
	switch len(b) {
	case 1:
		b[0] = byte(v)
	case 2:
		binary.LittleEndian.PutUint16(b, uint16(v))
	case 4:
		binary.LittleEndian.PutUint32(b, uint32(v))
	default:
		binary.LittleEndian.PutUint64(b, v)
	}
}

// Run executes the program on one packet.
func (l *Loaded) Run(in *Input) Result {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, m := range l.maps {
		m.mu.Lock()
	}
	defer func() {
		for _, m := range l.maps {
			m.mu.Unlock()
		}
	}()
	r := &run{l: l, in: in, now: in.Now}
	if r.now == 0 && l.Clock != nil {
		r.now = l.Clock()
	}
	binary.LittleEndian.PutUint64(r.ctx[CtxData:], mkptr(tagPkt, 0))
	binary.LittleEndian.PutUint64(r.ctx[CtxDataEnd:], mkptr(tagPkt, uint64(len(in.Packet))))
	binary.LittleEndian.PutUint64(r.ctx[CtxSrcHash:], in.PeerKey)
	binary.LittleEndian.PutUint64(r.ctx[CtxNow:], uint64(r.now))
	binary.LittleEndian.PutUint64(r.ctx[CtxLen:], uint64(len(in.Packet)))
	if in.Egress {
		binary.LittleEndian.PutUint64(r.ctx[CtxDir:], 1)
	}
	action, steps, err := r.exec()
	l.stats.Runs++
	l.stats.Insns += uint64(steps)
	if err != nil {
		l.stats.Faults++
		action = XDPAborted
	}
	r.res.Action = action
	l.stats.Verdicts[actionName(action)]++
	return r.res
}

func actionName(a int) string {
	switch a {
	case XDPAborted:
		return "aborted"
	case XDPDrop:
		return "drop"
	case XDPPass:
		return "pass"
	case XDPTX:
		return "tx"
	case XDPRedirect:
		return "redirect"
	}
	return fmt.Sprintf("action_%d", a)
}

func (r *run) exec() (action int, steps int, err error) {
	var reg [11]uint64
	reg[R1] = mkptr(tagCtx, 0)
	reg[R10] = mkptr(tagStack, StackSize)
	insns := r.l.prog.Insns
	for pc := 0; pc < len(insns); pc++ {
		steps++
		in := insns[pc]
		switch in.class() {
		case ClassLD: // LD_IMM64
			hi := insns[pc+1]
			if in.Src == pseudoMapFD {
				reg[in.Dst] = mkptr(tagMap, uint64(in.Imm))
			} else {
				reg[in.Dst] = uint64(uint32(in.Imm)) | uint64(uint32(hi.Imm))<<32
			}
			pc++
		case ClassALU, ClassALU64:
			is64 := in.class() == ClassALU64
			src := uint64(int64(in.Imm))
			if in.Op&srcX != 0 {
				src = reg[in.Src]
			}
			dst := reg[in.Dst]
			if !is64 {
				src, dst = uint64(uint32(src)), uint64(uint32(dst))
			}
			width := uint64(64)
			if !is64 {
				width = 32
			}
			switch ALUOp(in.Op & 0xf0) {
			case Add:
				dst += src
			case Sub:
				dst -= src
			case Mul:
				dst *= src
			case Div:
				if src == 0 {
					dst = 0 // eBPF semantics: x / 0 = 0
				} else {
					dst /= src
				}
			case Mod:
				if src != 0 { // x % 0 = x
					dst %= src
				}
			case Or:
				dst |= src
			case And:
				dst &= src
			case Xor:
				dst ^= src
			case Lsh:
				dst <<= src % width
			case Rsh:
				dst >>= src % width
			case Arsh:
				if is64 {
					dst = uint64(int64(dst) >> (src % 64))
				} else {
					dst = uint64(uint32(int32(uint32(dst)) >> (src % 32)))
				}
			case Neg:
				dst = -dst
			case Mov:
				dst = src
			}
			if !is64 {
				dst = uint64(uint32(dst))
			}
			reg[in.Dst] = dst
		case ClassLDX:
			b, e := r.mem(reg[in.Src]+uint64(int64(in.Off)), Size(in.Op&0x18).bytes(), false)
			if e != nil {
				return 0, steps, e
			}
			reg[in.Dst] = load(b)
		case ClassST:
			b, e := r.mem(reg[in.Dst]+uint64(int64(in.Off)), Size(in.Op&0x18).bytes(), true)
			if e != nil {
				return 0, steps, e
			}
			store(b, uint64(int64(in.Imm)))
		case ClassSTX:
			b, e := r.mem(reg[in.Dst]+uint64(int64(in.Off)), Size(in.Op&0x18).bytes(), true)
			if e != nil {
				return 0, steps, e
			}
			if in.Op&0xe0 == ModeATOMIC {
				store(b, load(b)+reg[in.Src])
			} else {
				store(b, reg[in.Src])
			}
		case ClassJMP, ClassJMP32:
			op := JumpOp(in.Op & 0xf0)
			switch op {
			case exit:
				return int(int32(reg[R0])), steps, nil
			case call:
				if e := r.call(in.Imm, &reg); e != nil {
					return 0, steps, e
				}
				continue
			case Ja:
				pc += int(in.Off)
				continue
			}
			a, b := reg[in.Dst], uint64(int64(in.Imm))
			if in.Op&srcX != 0 {
				b = reg[in.Src]
			}
			if in.class() == ClassJMP32 {
				a, b = uint64(uint32(a)), uint64(uint32(b))
			}
			sa, sb := int64(a), int64(b)
			if in.class() == ClassJMP32 {
				sa, sb = int64(int32(a)), int64(int32(b))
			}
			var take bool
			switch op {
			case Jeq:
				take = a == b
			case Jne:
				take = a != b
			case Jgt:
				take = a > b
			case Jge:
				take = a >= b
			case Jlt:
				take = a < b
			case Jle:
				take = a <= b
			case Jset:
				take = a&b != 0
			case Jsgt:
				take = sa > sb
			case Jsge:
				take = sa >= sb
			case Jslt:
				take = sa < sb
			case Jsle:
				take = sa <= sb
			}
			if take {
				pc += int(in.Off)
			}
		}
	}
	return 0, steps, errFault // unreachable for verified programs
}

func (r *run) mapArg(v uint64) (Map, error) {
	if v>>32 != tagMap || int(v&0xffffffff) >= len(r.l.prog.Maps) {
		return nil, errFault
	}
	return r.l.prog.Maps[v&0xffffffff], nil
}

// errno maps a map error to the kernel's negative errno convention.
func errno(err error) uint64 {
	neg := func(e int64) uint64 { return uint64(-e) }
	switch err {
	case nil:
		return 0
	case ErrNoEntry:
		return neg(2) // ENOENT
	case ErrExists:
		return neg(17) // EEXIST
	case ErrMapFull:
		return neg(7) // E2BIG
	}
	return neg(22) // EINVAL
}

func (r *run) call(helper int32, reg *[11]uint64) error {
	defer func() { reg[R1], reg[R2], reg[R3], reg[R4], reg[R5] = 0, 0, 0, 0, 0 }()
	switch helper {
	case HelperMapLookup:
		m, err := r.mapArg(reg[R1])
		if err != nil {
			return err
		}
		key, err := r.mem(reg[R2], m.KeySize(), false)
		if err != nil {
			return err
		}
		v := m.lookupLocked(key)
		if v == nil || len(r.vals) >= maxVals {
			reg[R0] = 0
			return nil
		}
		r.vals = append(r.vals, v)
		reg[R0] = mkptr(tagVal+uint64(len(r.vals)-1), 0)
	case HelperMapUpdate:
		m, err := r.mapArg(reg[R1])
		if err != nil {
			return err
		}
		key, err := r.mem(reg[R2], m.KeySize(), false)
		if err != nil {
			return err
		}
		val, err := r.mem(reg[R3], m.ValueSize(), false)
		if err != nil {
			return err
		}
		reg[R0] = errno(m.updateLocked(append([]byte(nil), key...), append([]byte(nil), val...), reg[R4]))
	case HelperMapDelete:
		m, err := r.mapArg(reg[R1])
		if err != nil {
			return err
		}
		key, err := r.mem(reg[R2], m.KeySize(), false)
		if err != nil {
			return err
		}
		reg[R0] = errno(m.deleteLocked(key))
	case HelperKtimeGetNs:
		reg[R0] = uint64(r.now)
	case HelperPrandomU32:
		reg[R0] = uint64(r.l.rng.Uint32())
	case HelperRedirectMap:
		m, err := r.mapArg(reg[R1])
		if err != nil {
			return err
		}
		key := uint32(reg[R2])
		fallback := reg[R3] & 0x7
		switch t := m.(type) {
		case *AddrMap:
			if a := t.target(key); a != nil {
				r.res.Redirect = a
				reg[R0] = XDPRedirect
				return nil
			}
		case *QueueMap:
			if int(key) < len(t.qs) {
				r.res.Queue, r.res.QueueIdx = t, key
				reg[R0] = XDPRedirect
				return nil
			}
		}
		reg[R0] = fallback
	case HelperMAC64:
		m, err := r.mapArg(reg[R1])
		if err != nil {
			return err
		}
		km, ok := m.(*ArrayMap)
		if !ok || len(km.vals) == 0 {
			return errFault
		}
		data, err := r.mem(reg[R2], int(reg[R3]&0xffff), false)
		if err != nil {
			return err
		}
		reg[R0] = MAC64(km.vals[0], data)
	default:
		return errFault
	}
	return nil
}

// MAC64 is HelperMAC64's function: the first 8 bytes (little-endian) of
// HMAC-SHA256(key, data).
func MAC64(key, data []byte) uint64 {
	h := hmac.New(sha256.New, key)
	h.Write(data)
	return binary.LittleEndian.Uint64(h.Sum(nil))
}
