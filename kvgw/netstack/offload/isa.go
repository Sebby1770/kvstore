// Package offload runs network functions in the data path: verified,
// bounded programs in the eBPF instruction set, attached at the edge of a
// KTP endpoint (package endpoint's PacketFilter, the XDP/TC hook of this
// userspace stack), with maps shared between the programs and the control
// plane.
//
// The bytecode is real eBPF: the same 64-bit instruction encoding, register
// file (r0-r10, r10 the read-only frame pointer, 512-byte stack), ALU/JMP/
// LDX/STX/ATOMIC opcodes, map references through LD_IMM64 with the
// pseudo-map-fd source, and helper calls by number (map_lookup_elem = 1,
// map_update_elem = 2, ktime_get_ns = 5, redirect_map = 51, ...). Program
// bytes produced here are what the Linux bpf() syscall accepts, and the
// built-in programs use only kernel-standard helpers except where noted.
//
// The verifier is simpler than the kernel's and leans on the interpreter
// for memory safety: it proves termination structurally (every jump is
// forward, so a program of n instructions runs at most n of them), checks
// every opcode, register, jump target, map reference and helper, and tracks
// which registers are initialised along every path. Memory safety is then
// enforced at run time: every pointer is tagged with the region it points
// into (context, packet, stack, map value) and every load and store is
// bounds-checked against that region. A program can drop a packet; it
// cannot crash the stack, loop, or read memory it was not given.
//
// Built-in network functions (programs.go): a distributed rate limiter
// (per-source token buckets whose rate the control plane resizes as the
// cluster changes), a circuit breaker (trips on stateless resets and
// host-reported failures, fails fast on egress, half-opens after a cooldown),
// a heartbeat responder (answers authenticated liveness probes straight
// from the data path and reports how stale the host process is -- the
// signal that separates a dead node from a hung one), and a Maglev load
// balancer (consistent-hash steering of connections to backends, forwarded
// encapsulated for direct server return).
//
// On a SmartNIC or DPU these programs would run on the card; under Linux
// the same bytecode could be attached at XDP. Here they run in the
// endpoint's receive path, which is where this stack's "NIC" is.
package offload

import (
	"encoding/binary"
	"fmt"
)

// Reg is an eBPF register.
type Reg uint8

const (
	R0 Reg = iota // return value, helper result
	R1            // first argument (the context on entry)
	R2
	R3
	R4
	R5
	R6 // r6-r9 survive helper calls
	R7
	R8
	R9
	R10 // frame pointer (read-only): the top of the 512-byte stack
)

// Instruction classes (op & 0x07).
const (
	ClassLD    = 0x00
	ClassLDX   = 0x01
	ClassST    = 0x02
	ClassSTX   = 0x03
	ClassALU   = 0x04
	ClassJMP   = 0x05
	ClassJMP32 = 0x06
	ClassALU64 = 0x07
)

// Sizes for loads and stores (op & 0x18).
type Size uint8

const (
	W  Size = 0x00 // 4 bytes
	H  Size = 0x08 // 2 bytes
	B  Size = 0x10 // 1 byte
	DW Size = 0x18 // 8 bytes
)

func (s Size) bytes() int {
	switch s {
	case W:
		return 4
	case H:
		return 2
	case B:
		return 1
	}
	return 8
}

// Modes (op & 0xe0).
const (
	ModeIMM    = 0x00
	ModeMEM    = 0x60
	ModeATOMIC = 0xc0
)

// ALU operations (op & 0xf0).
type ALUOp uint8

const (
	Add  ALUOp = 0x00
	Sub  ALUOp = 0x10
	Mul  ALUOp = 0x20
	Div  ALUOp = 0x30
	Or   ALUOp = 0x40
	And  ALUOp = 0x50
	Lsh  ALUOp = 0x60
	Rsh  ALUOp = 0x70
	Neg  ALUOp = 0x80
	Mod  ALUOp = 0x90
	Xor  ALUOp = 0xa0
	Mov  ALUOp = 0xb0
	Arsh ALUOp = 0xc0
)

// Jump operations (op & 0xf0).
type JumpOp uint8

const (
	Ja   JumpOp = 0x00
	Jeq  JumpOp = 0x10
	Jgt  JumpOp = 0x20
	Jge  JumpOp = 0x30
	Jset JumpOp = 0x40
	Jne  JumpOp = 0x50
	Jsgt JumpOp = 0x60
	Jsge JumpOp = 0x70
	call JumpOp = 0x80
	exit JumpOp = 0x90
	Jlt  JumpOp = 0xa0
	Jle  JumpOp = 0xb0
	Jslt JumpOp = 0xc0
	Jsle JumpOp = 0xd0
)

const (
	srcK = 0x00 // immediate operand
	srcX = 0x08 // register operand

	pseudoMapFD = 1 // LD_IMM64 src: imm is a map reference
	opLdImm64   = ClassLD | ModeIMM | uint8(DW)
)

// Helper function numbers (kernel numbering where one exists).
const (
	HelperMapLookup   = 1
	HelperMapUpdate   = 2
	HelperMapDelete   = 3
	HelperKtimeGetNs  = 5
	HelperPrandomU32  = 7
	HelperRedirectMap = 51
	// HelperMAC64 is not a kernel helper: it is this stack's "SmartNIC"
	// primitive, a keyed 64-bit MAC (truncated HMAC-SHA256) over packet
	// bytes, with the key held in an array map the program cannot read.
	HelperMAC64 = 0x10001
)

// XDP actions, the values a program returns in r0.
const (
	XDPAborted  = 0
	XDPDrop     = 1
	XDPPass     = 2
	XDPTX       = 3
	XDPRedirect = 4
)

// Insn is one eBPF instruction.
type Insn struct {
	Op  uint8
	Dst Reg
	Src Reg
	Off int16
	Imm int32
}

func (i Insn) class() uint8 { return i.Op & 0x07 }

func (i Insn) String() string {
	return fmt.Sprintf("op=%#02x dst=r%d src=r%d off=%d imm=%d", i.Op, i.Dst, i.Src, i.Off, i.Imm)
}

// Encode returns the kernel's byte encoding of a program: 8 bytes per
// instruction, little-endian, registers packed dst-low / src-high.
func Encode(insns []Insn) []byte {
	out := make([]byte, 8*len(insns))
	for k, i := range insns {
		b := out[8*k:]
		b[0] = i.Op
		b[1] = byte(i.Dst&0x0f) | byte(i.Src&0x0f)<<4
		binary.LittleEndian.PutUint16(b[2:], uint16(i.Off))
		binary.LittleEndian.PutUint32(b[4:], uint32(i.Imm))
	}
	return out
}

// Decode parses kernel-encoded bytecode.
func Decode(b []byte) ([]Insn, error) {
	if len(b)%8 != 0 {
		return nil, fmt.Errorf("offload: bytecode length %d is not a multiple of 8", len(b))
	}
	out := make([]Insn, len(b)/8)
	for k := range out {
		x := b[8*k:]
		out[k] = Insn{Op: x[0], Dst: Reg(x[1] & 0x0f), Src: Reg(x[1] >> 4),
			Off: int16(binary.LittleEndian.Uint16(x[2:])), Imm: int32(binary.LittleEndian.Uint32(x[4:]))}
	}
	return out, nil
}

// ---- assembler ----

// Builder assembles a program with symbolic, forward-only jump labels.
type Builder struct {
	insns  []Insn
	fixups []fixup
	labels map[string]int
	err    error
}

type fixup struct {
	at    int
	label string
}

// NewBuilder starts an empty program.
func NewBuilder() *Builder { return &Builder{labels: map[string]int{}} }

func (b *Builder) emit(i ...Insn) *Builder {
	b.insns = append(b.insns, i...)
	return b
}

// Label marks the next instruction.
func (b *Builder) Label(name string) *Builder {
	if _, dup := b.labels[name]; dup && b.err == nil {
		b.err = fmt.Errorf("offload: duplicate label %q", name)
	}
	b.labels[name] = len(b.insns)
	return b
}

// ALU64 dst = dst <op> imm.
func (b *Builder) ALU64Imm(op ALUOp, dst Reg, imm int32) *Builder {
	return b.emit(Insn{Op: ClassALU64 | uint8(op) | srcK, Dst: dst, Imm: imm})
}

// ALU64Reg dst = dst <op> src.
func (b *Builder) ALU64Reg(op ALUOp, dst, src Reg) *Builder {
	return b.emit(Insn{Op: ClassALU64 | uint8(op) | srcX, Dst: dst, Src: src})
}

// ALU32Imm is the 32-bit form (the result is zero-extended).
func (b *Builder) ALU32Imm(op ALUOp, dst Reg, imm int32) *Builder {
	return b.emit(Insn{Op: ClassALU | uint8(op) | srcK, Dst: dst, Imm: imm})
}

// Mov dst = imm (sign-extended to 64 bits).
func (b *Builder) Mov(dst Reg, imm int32) *Builder { return b.ALU64Imm(Mov, dst, imm) }

// MovReg dst = src.
func (b *Builder) MovReg(dst, src Reg) *Builder { return b.ALU64Reg(Mov, dst, src) }

// LoadImm64 dst = imm (a full 64-bit constant: two instruction slots).
func (b *Builder) LoadImm64(dst Reg, v uint64) *Builder {
	return b.emit(Insn{Op: opLdImm64, Dst: dst, Imm: int32(uint32(v))}, Insn{Imm: int32(uint32(v >> 32))})
}

// LoadMap dst = a reference to the program's map number idx.
func (b *Builder) LoadMap(dst Reg, idx int) *Builder {
	return b.emit(Insn{Op: opLdImm64, Dst: dst, Src: pseudoMapFD, Imm: int32(idx)}, Insn{})
}

// Load dst = *(size *)(src + off).
func (b *Builder) Load(size Size, dst, src Reg, off int16) *Builder {
	return b.emit(Insn{Op: ClassLDX | ModeMEM | uint8(size), Dst: dst, Src: src, Off: off})
}

// Store *(size *)(dst + off) = src.
func (b *Builder) Store(size Size, dst Reg, off int16, src Reg) *Builder {
	return b.emit(Insn{Op: ClassSTX | ModeMEM | uint8(size), Dst: dst, Src: src, Off: off})
}

// StoreImm *(size *)(dst + off) = imm.
func (b *Builder) StoreImm(size Size, dst Reg, off int16, imm int32) *Builder {
	return b.emit(Insn{Op: ClassST | ModeMEM | uint8(size), Dst: dst, Off: off, Imm: imm})
}

// AtomicAdd *(size *)(dst + off) += src (W or DW).
func (b *Builder) AtomicAdd(size Size, dst Reg, off int16, src Reg) *Builder {
	return b.emit(Insn{Op: ClassSTX | ModeATOMIC | uint8(size), Dst: dst, Src: src, Off: off, Imm: int32(Add)})
}

func (b *Builder) jump(i Insn, label string) *Builder {
	b.fixups = append(b.fixups, fixup{at: len(b.insns), label: label})
	return b.emit(i)
}

// JumpImm goes to label if dst <op> imm.
func (b *Builder) JumpImm(op JumpOp, dst Reg, imm int32, label string) *Builder {
	return b.jump(Insn{Op: ClassJMP | uint8(op) | srcK, Dst: dst, Imm: imm}, label)
}

// JumpReg goes to label if dst <op> src.
func (b *Builder) JumpReg(op JumpOp, dst, src Reg, label string) *Builder {
	return b.jump(Insn{Op: ClassJMP | uint8(op) | srcX, Dst: dst, Src: src}, label)
}

// Goto jumps unconditionally.
func (b *Builder) Goto(label string) *Builder {
	return b.jump(Insn{Op: ClassJMP | uint8(Ja)}, label)
}

// Call invokes a helper; arguments in r1-r5, result in r0; r1-r5 are
// clobbered.
func (b *Builder) Call(helper int32) *Builder {
	return b.emit(Insn{Op: ClassJMP | uint8(call), Imm: helper})
}

// Exit returns r0.
func (b *Builder) Exit() *Builder { return b.emit(Insn{Op: ClassJMP | uint8(exit)}) }

// Return sets r0 = v and exits.
func (b *Builder) Return(v int32) *Builder { return b.Mov(R0, v).Exit() }

// Build resolves labels. Backward jumps are rejected here already: the
// verifier would refuse them anyway.
func (b *Builder) Build() ([]Insn, error) {
	if b.err != nil {
		return nil, b.err
	}
	out := append([]Insn(nil), b.insns...)
	for _, f := range b.fixups {
		at, ok := b.labels[f.label]
		if !ok {
			return nil, fmt.Errorf("offload: undefined label %q", f.label)
		}
		off := at - f.at - 1
		if off < 0 {
			return nil, fmt.Errorf("offload: jump to %q goes backwards", f.label)
		}
		if off > 1<<15-1 {
			return nil, fmt.Errorf("offload: jump to %q too far", f.label)
		}
		out[f.at].Off = int16(off)
	}
	return out, nil
}

// MustBuild is Build for programs known at compile time.
func (b *Builder) MustBuild() []Insn {
	p, err := b.Build()
	if err != nil {
		panic(err)
	}
	return p
}
