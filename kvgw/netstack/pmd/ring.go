// Package pmd is a userspace packet data plane in the style of DPDK: packets
// live in preallocated buffers (mbufs) drawn from a pool, move between
// stages through lock-free rings in bursts, and are polled by dedicated
// run-to-completion loops (lcores) instead of being delivered by interrupts
// and syscalls.
//
//   - Ring is DPDK's rte_ring algorithm (producer and consumer head/tail
//     pairs, CAS reservation, in-order commit), multi-producer and
//     multi-consumer, laid out in a fabric.Region so it works the same in
//     heap memory and in memory shared between processes.
//   - Pool is an mbuf pool: fixed-size buffers in one region and a Ring of
//     free buffer indices. Nothing is allocated per packet.
//   - Memif is a shared-memory interface between two processes on one host
//     (DPDK's memif / virtio-user idea): both map one file holding the pool
//     and a ring per direction. Sending is "enqueue a buffer index"; the
//     receiver reads the packet where the sender wrote it. No syscall, no
//     kernel, no copy -- this is the kernel-bypass path that needs no
//     special hardware.
//   - SocketPort drives the same Port API from an ordinary socket (the
//     kernel path) for peers that are not co-located.
//   - Lcore is a poll-mode loop pinned to an OS thread: burst receive,
//     process, burst transmit, with adaptive idle back-off (spin, yield,
//     then short sleeps) in place of DPDK's interrupt mode.
//   - RSS is the Toeplitz receive-side-scaling hash NICs use to spread flows
//     across queues; Steer picks a queue so one flow stays on one lcore.
//   - PacketConn adapts any Port to net.PacketConn, so the KTP endpoint (and
//     everything above it) runs over memif unchanged.
//
// What needs real DPDK -- binding a physical NIC to a userspace driver and
// polling its descriptor rings over PCIe -- needs C, hugepages and Linux.
// The structures here are the same ones DPDK uses between its own stages,
// and memif is a real zero-copy, syscall-free transport between processes.
package pmd

import (
	"errors"
	"runtime"
	"sync/atomic"

	"github.com/Sebby1770/kvstore/kvgw/netstack/fabric"
)

// Ring layout at base in a region. Each index sits on its own 64-byte cache
// line so producers and consumers do not false-share:
//
//	+0    producer head    (next slot a producer may reserve)
//	+64   producer tail    (slots below it are committed and readable)
//	+128  consumer head
//	+192  consumer tail
//	+256  slots[size] (8 bytes each)
//
// Indices are free-running 64-bit counters (they never wrap in practice),
// so "used = prodTail - consTail" needs no modular arithmetic.
const ringHeader = 256

// RingBytes is the region space a ring of size slots needs.
func RingBytes(size int) uint64 { return ringHeader + 8*uint64(size) }

// Ring is a bounded lock-free multi-producer, multi-consumer queue of
// 64-bit values (buffer indices, in this package).
type Ring struct {
	prodHead, prodTail, consHead, consTail *uint64
	slots                                  []uint64
	size, mask                             uint64
}

// ErrRingSize reports a ring size that is not a power of two.
var ErrRingSize = errors.New("pmd: ring size must be a power of two")

// FormatRing initialises an empty ring at base (it must not be in use).
func FormatRing(r *fabric.Region, base uint64, size int) (*Ring, error) {
	q, err := OpenRing(r, base, size)
	if err != nil {
		return nil, err
	}
	atomic.StoreUint64(q.prodHead, 0)
	atomic.StoreUint64(q.prodTail, 0)
	atomic.StoreUint64(q.consHead, 0)
	atomic.StoreUint64(q.consTail, 0)
	return q, nil
}

// OpenRing attaches to a ring someone else formatted (possibly in another
// process, through a shared mapping).
func OpenRing(r *fabric.Region, base uint64, size int) (*Ring, error) {
	if size <= 0 || size&(size-1) != 0 {
		return nil, ErrRingSize
	}
	q := &Ring{size: uint64(size), mask: uint64(size - 1)}
	var err error
	ptr := func(off uint64) *uint64 {
		w, e := r.Word(base + off)
		if e != nil && err == nil {
			err = e
		}
		return w
	}
	q.prodHead, q.prodTail, q.consHead, q.consTail = ptr(0), ptr(64), ptr(128), ptr(192)
	if err != nil {
		return nil, err
	}
	if q.slots, err = r.Words(base+ringHeader, size); err != nil {
		return nil, err
	}
	return q, nil
}

// Cap is the ring's capacity.
func (q *Ring) Cap() int { return int(q.size) }

// Len is how many values are committed and not yet consumed (a snapshot).
func (q *Ring) Len() int {
	return int(atomic.LoadUint64(q.prodTail) - atomic.LoadUint64(q.consTail))
}

// Enqueue adds as many of vals as fit (a burst) and returns how many.
// Producers reserve slots by CAS on the producer head, fill them, then
// commit in reservation order by advancing the producer tail -- which is
// what makes a slot visible to consumers only once it is written.
func (q *Ring) Enqueue(vals []uint64) int {
	var head, n uint64
	for {
		head = atomic.LoadUint64(q.prodHead)
		free := q.size - (head - atomic.LoadUint64(q.consTail))
		n = min(uint64(len(vals)), free)
		if n == 0 {
			return 0
		}
		if atomic.CompareAndSwapUint64(q.prodHead, head, head+n) {
			break
		}
	}
	for i := uint64(0); i < n; i++ {
		atomic.StoreUint64(&q.slots[(head+i)&q.mask], vals[i])
	}
	// Wait for producers that reserved earlier slots to commit first.
	for spins := 0; atomic.LoadUint64(q.prodTail) != head; spins++ {
		if spins > 64 {
			runtime.Gosched()
		}
	}
	atomic.StoreUint64(q.prodTail, head+n)
	return int(n)
}

// Dequeue removes up to len(out) values (a burst) and returns how many.
func (q *Ring) Dequeue(out []uint64) int {
	var head, n uint64
	for {
		head = atomic.LoadUint64(q.consHead)
		avail := atomic.LoadUint64(q.prodTail) - head
		n = min(uint64(len(out)), avail)
		if n == 0 {
			return 0
		}
		if atomic.CompareAndSwapUint64(q.consHead, head, head+n) {
			break
		}
	}
	for i := uint64(0); i < n; i++ {
		out[i] = atomic.LoadUint64(&q.slots[(head+i)&q.mask])
	}
	for spins := 0; atomic.LoadUint64(q.consTail) != head; spins++ {
		if spins > 64 {
			runtime.Gosched()
		}
	}
	atomic.StoreUint64(q.consTail, head+n)
	return int(n)
}
