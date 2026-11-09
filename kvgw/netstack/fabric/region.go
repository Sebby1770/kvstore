// Package fabric is a single-address-space memory fabric: nodes register
// regions of memory and peers read, write and atomically update them with
// one-sided verbs -- addressed by global pointers, served without any
// application code running on the owner, and carried as fixed-layout
// records that are read in place rather than serialised.
//
// It is the software shape of RDMA and CXL.mem:
//
//   - A Region is registered memory: a heap arena, or (MapFile) a file
//     mapped MAP_SHARED, which two processes on one host can map at once and
//     then share with plain loads, stores and atomics -- no messages, no
//     kernel, no peer CPU involvement at all. That is the CXL-style case.
//   - Across hosts, verbs (READ, WRITE, CAS, FETCH_ADD, and CHASE, which
//     follows a linked structure on the owner) travel as KTP datagrams. The
//     owner executes them in its endpoint's packet path, the moment the
//     datagram is decrypted, against registered memory -- no goroutine is
//     woken, no handler scheduled, no request decoded into objects. That is
//     the part an RDMA NIC does in hardware; here the "NIC" is the
//     endpoint's receive path.
//   - Remote keys (rkeys) and per-region permissions gate every verb, and
//     every access is bounds-checked: a peer can touch only what was
//     registered for it, in the ways it was registered.
//   - Verbs are unreliable datagrams retried by the requester; the owner
//     keeps recent responses to non-idempotent verbs so a retry is answered,
//     not re-executed (RDMA RC's responder resources do the same).
//
// Pointer chasing -- walking a remote linked list or hash chain -- costs one
// round trip per hop with READ alone; CHASE runs the walk on the owner and
// returns the node it ends on, the way research NICs (StRoM, RMC) offload
// traversals. Index (rhash.go) builds a lock-free remote hash index on these
// verbs in the style of FaRM and Pilaf: lookups are one READ plus one CHASE,
// inserts a FETCH_ADD, a WRITE and a CAS, and the owner's application never
// runs.
//
// What software cannot reproduce is the hardware's latency (hundreds of
// nanoseconds, not a UDP round trip) and true CPU bypass on a remote host:
// the owner's CPU still runs the endpoint. The architecture, wire format and
// failure semantics are those of the real thing, so the same code maps onto
// verbs/libfabric or CXL memory when the hardware exists.
package fabric

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sync/atomic"
	"unsafe"
)

// Perm is what peers may do with a region.
type Perm uint8

const (
	PermRead   Perm = 1 << iota // READ, CHASE
	PermWrite                   // WRITE
	PermAtomic                  // CAS, FETCH_ADD
	PermAll    = PermRead | PermWrite | PermAtomic
)

// Ptr is a global pointer: a region ID in the top 16 bits and a byte offset
// in the low 48. It is meaningful on every node that knows the region, which
// is what makes the fabric one address space.
type Ptr uint64

// MakePtr builds a pointer into region id at offset off.
func MakePtr(id uint16, off uint64) Ptr { return Ptr(uint64(id)<<48 | off&offMask) }

const offMask = 1<<48 - 1

// Region is the region ID.
func (p Ptr) Region() uint16 { return uint16(p >> 48) }

// Offset is the byte offset within the region.
func (p Ptr) Offset() uint64 { return uint64(p) & offMask }

// Add advances the pointer by n bytes within its region.
func (p Ptr) Add(n uint64) Ptr { return MakePtr(p.Region(), p.Offset()+n) }

func (p Ptr) String() string { return fmt.Sprintf("%d:%#x", p.Region(), p.Offset()) }

// Errors.
var (
	ErrBounds    = errors.New("fabric: access outside the region")
	ErrAlign     = errors.New("fabric: atomic access must be 8-byte aligned")
	ErrKey       = errors.New("fabric: bad region or remote key")
	ErrPerm      = errors.New("fabric: operation not permitted on this region")
	ErrTimeout   = errors.New("fabric: no response from the region's owner")
	ErrTooLarge  = errors.New("fabric: transfer too large for one verb")
	ErrMalformed = errors.New("fabric: malformed verb")
)

// Region is registered memory. Its bytes are accessed in place: Bytes
// returns a slice aliasing the region, and the word operations act on the
// memory itself with the CPU's atomic instructions, so a region mapped by
// two processes is coherent between them.
type Region struct {
	mem   []byte
	words []uint64 // keeps a heap region's backing array alive (and aligned)
	unmap func() error
}

// NewRegion allocates a zeroed heap region of size bytes (rounded up to a
// multiple of 8). Its base is 8-byte aligned, as atomics require.
func NewRegion(size int) *Region {
	n := (size + 7) / 8
	if n == 0 {
		n = 1
	}
	words := make([]uint64, n)
	mem := unsafe.Slice((*byte)(unsafe.Pointer(&words[0])), n*8)
	return &Region{mem: mem, words: words}
}

// Size is the region's length in bytes.
func (r *Region) Size() uint64 { return uint64(len(r.mem)) }

// Close unmaps a mapped region. Heap regions need no closing.
func (r *Region) Close() error {
	if r.unmap != nil {
		u := r.unmap
		r.unmap = nil
		return u()
	}
	return nil
}

func (r *Region) check(off, n uint64) error {
	if n > r.Size() || off > r.Size()-n {
		return ErrBounds
	}
	return nil
}

// Bytes returns the region's bytes [off, off+n) in place (no copy).
func (r *Region) Bytes(off, n uint64) ([]byte, error) {
	if err := r.check(off, n); err != nil {
		return nil, err
	}
	return r.mem[off : off+n : off+n], nil
}

// ReadAt copies bytes out of the region.
func (r *Region) ReadAt(p []byte, off uint64) error {
	b, err := r.Bytes(off, uint64(len(p)))
	if err != nil {
		return err
	}
	copy(p, b)
	return nil
}

// WriteAt copies bytes into the region.
func (r *Region) WriteAt(p []byte, off uint64) error {
	b, err := r.Bytes(off, uint64(len(p)))
	if err != nil {
		return err
	}
	copy(b, p)
	return nil
}

func (r *Region) word(off uint64) (*uint64, error) {
	if off%8 != 0 {
		return nil, ErrAlign
	}
	if err := r.check(off, 8); err != nil {
		return nil, err
	}
	return (*uint64)(unsafe.Pointer(&r.mem[off])), nil
}

// Load64 atomically loads the word at off (8-byte aligned).
func (r *Region) Load64(off uint64) (uint64, error) {
	w, err := r.word(off)
	if err != nil {
		return 0, err
	}
	return atomic.LoadUint64(w), nil
}

// Store64 atomically stores v at off.
func (r *Region) Store64(off, v uint64) error {
	w, err := r.word(off)
	if err != nil {
		return err
	}
	atomic.StoreUint64(w, v)
	return nil
}

// CompareAndSwap64 atomically replaces old with new at off. It returns the
// value found (old on success).
func (r *Region) CompareAndSwap64(off, old, new uint64) (prev uint64, swapped bool, err error) {
	w, err := r.word(off)
	if err != nil {
		return 0, false, err
	}
	for {
		if atomic.CompareAndSwapUint64(w, old, new) {
			return old, true, nil
		}
		if cur := atomic.LoadUint64(w); cur != old {
			return cur, false, nil
		}
	}
}

// FetchAdd64 atomically adds delta at off and returns the previous value.
func (r *Region) FetchAdd64(off, delta uint64) (uint64, error) {
	w, err := r.word(off)
	if err != nil {
		return 0, err
	}
	return atomic.AddUint64(w, delta) - delta, nil
}

// ChaseSpec describes a walk along a linked structure: each node holds the
// offset of the next node (0 = end) at LinkOff, and the walk stops early at
// a node whose 8-byte word at KeyOff equals Key (when Match is set).
type ChaseSpec struct {
	LinkOff uint32
	KeyOff  uint32
	Key     uint64
	Match   bool
	MaxHops uint32 // bound on the walk (required)
	ReadLen uint32 // bytes of the final node to return
}

// ChaseResult is where a walk ended.
type ChaseResult struct {
	Off   uint64 // offset of the node the walk ended on (0 = ran off the end)
	Hops  uint32
	Found bool   // Match was set and the key matched
	Data  []byte // ReadLen bytes of that node (copied)
}

// Chase walks the structure starting at node offset start.
func (r *Region) Chase(start uint64, s ChaseSpec) (ChaseResult, error) {
	var res ChaseResult
	off := start
	for off != 0 && res.Hops < s.MaxHops {
		if s.Match {
			k, err := r.Load64(off + uint64(s.KeyOff))
			if err != nil {
				return res, err
			}
			if k == s.Key {
				res.Found = true
				break
			}
		}
		next, err := r.Load64(off + uint64(s.LinkOff))
		if err != nil {
			return res, err
		}
		res.Hops++
		if next == 0 {
			break
		}
		off = next
	}
	if s.Match && !res.Found {
		off = 0
	}
	res.Off = off
	if off != 0 && s.ReadLen > 0 {
		// The node's length is often unknown to the requester: return up to
		// ReadLen bytes, stopping at the end of the region.
		n := min(uint64(s.ReadLen), r.Size()-min(off, r.Size()))
		b, err := r.Bytes(off, n)
		if err != nil {
			return res, err
		}
		res.Data = append([]byte(nil), b...)
	}
	return res, nil
}

var le = binary.LittleEndian
