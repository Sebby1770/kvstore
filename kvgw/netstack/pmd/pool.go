package pmd

import (
	"errors"

	"github.com/Sebby1770/kvstore/kvgw/netstack/fabric"
)

// Pool is an mbuf pool: count fixed-size buffers in a region plus a ring of
// the indices that are free. Get and Put are a ring dequeue and enqueue, so
// they are lock-free and allocation-free, and -- when the region is shared
// -- usable from either process.
//
// Each buffer starts with a 16-byte header (data length, flags, and an aux
// word the owner may use) followed by the data room.
type Pool struct {
	r       *fabric.Region
	free    *Ring
	base    uint64
	bufSize uint64
	count   int
}

const (
	mbufHeader = 16
	// MbufAlign is the buffer stride alignment (a cache line).
	MbufAlign = 64
)

// ErrPoolEmpty reports that no buffer is free.
var ErrPoolEmpty = errors.New("pmd: mbuf pool exhausted")

func ceilPow2(n int) int {
	p := 1
	for p < n {
		p <<= 1
	}
	return p
}

func stride(dataRoom int) uint64 {
	return uint64(mbufHeader+dataRoom+MbufAlign-1) &^ (MbufAlign - 1)
}

// PoolBytes is the region space a pool needs.
func PoolBytes(count, dataRoom int) uint64 {
	return RingBytes(ceilPow2(count)) + uint64(count)*stride(dataRoom)
}

// FormatPool lays out a pool at base and marks every buffer free.
func FormatPool(r *fabric.Region, base uint64, count, dataRoom int) (*Pool, error) {
	p, err := openPool(r, base, count, dataRoom, true)
	if err != nil {
		return nil, err
	}
	idx := make([]uint64, count)
	for i := range idx {
		idx[i] = uint64(i)
	}
	if p.free.Enqueue(idx) != count {
		return nil, errors.New("pmd: free ring too small")
	}
	return p, nil
}

// OpenPool attaches to a pool formatted elsewhere (e.g. by the peer process).
func OpenPool(r *fabric.Region, base uint64, count, dataRoom int) (*Pool, error) {
	return openPool(r, base, count, dataRoom, false)
}

func openPool(r *fabric.Region, base uint64, count, dataRoom int, format bool) (*Pool, error) {
	if count <= 0 || dataRoom <= 0 {
		return nil, errors.New("pmd: pool needs buffers and data room")
	}
	if base%MbufAlign != 0 {
		return nil, errors.New("pmd: pool base must be cache-line aligned")
	}
	if base+PoolBytes(count, dataRoom) > r.Size() {
		return nil, fabric.ErrBounds
	}
	rs := ceilPow2(count)
	var free *Ring
	var err error
	if format {
		free, err = FormatRing(r, base, rs)
	} else {
		free, err = OpenRing(r, base, rs)
	}
	if err != nil {
		return nil, err
	}
	bufBase := (base + RingBytes(rs) + MbufAlign - 1) &^ (MbufAlign - 1)
	if bufBase+uint64(count)*stride(dataRoom) > r.Size() {
		return nil, fabric.ErrBounds
	}
	return &Pool{r: r, free: free, base: bufBase, bufSize: stride(dataRoom), count: count}, nil
}

// Mbuf is one packet buffer, named by its index in the pool. It is a value
// (two words), so passing mbufs around allocates nothing.
type Mbuf struct {
	p   *Pool
	idx uint32
}

// Index is the buffer's index in its pool -- what travels through rings.
func (m Mbuf) Index() uint32 { return m.idx }

// Valid reports whether m refers to a buffer.
func (m Mbuf) Valid() bool { return m.p != nil }

func (m Mbuf) off() uint64 { return m.p.base + uint64(m.idx)*m.p.bufSize }

// Room is the whole data room, in place.
func (m Mbuf) Room() []byte {
	b, _ := m.p.r.Bytes(m.off()+mbufHeader, m.p.bufSize-mbufHeader)
	return b
}

// Len is the packet length.
func (m Mbuf) Len() int {
	v, _ := m.p.r.Load64(m.off())
	return int(uint32(v))
}

// SetLen sets the packet length (clamped to the data room).
func (m Mbuf) SetLen(n int) {
	n = min(max(n, 0), int(m.p.bufSize-mbufHeader))
	m.p.r.Store64(m.off(), uint64(n))
}

// Data is the packet, in place.
func (m Mbuf) Data() []byte { return m.Room()[:m.Len()] }

// Aux is a free word for the owner (SocketPort keeps the peer address
// slot there).
func (m Mbuf) Aux() uint64 {
	v, _ := m.p.r.Load64(m.off() + 8)
	return v
}

// SetAux sets the aux word.
func (m Mbuf) SetAux(v uint64) { m.p.r.Store64(m.off()+8, v) }

// Get takes a free buffer.
func (p *Pool) Get() (Mbuf, error) {
	var idx [1]uint64
	if p.free.Dequeue(idx[:]) == 0 {
		return Mbuf{}, ErrPoolEmpty
	}
	m := Mbuf{p: p, idx: uint32(idx[0])}
	m.SetLen(0)
	return m, nil
}

// GetBurst fills out with free buffers and returns how many it got.
func (p *Pool) GetBurst(out []Mbuf) int {
	idx := make([]uint64, len(out))
	n := p.free.Dequeue(idx)
	for i := 0; i < n; i++ {
		out[i] = Mbuf{p: p, idx: uint32(idx[i])}
		out[i].SetLen(0)
	}
	return n
}

// Put returns buffers to the pool.
func (p *Pool) Put(ms ...Mbuf) {
	var stack [32]uint64
	idx := stack[:0]
	for _, m := range ms {
		idx = append(idx, uint64(m.idx))
	}
	for len(idx) > 0 {
		n := p.free.Enqueue(idx)
		idx = idx[n:]
	}
}

// Free is how many buffers are free (a snapshot).
func (p *Pool) Free() int { return p.free.Len() }

// Count is the pool's size.
func (p *Pool) Count() int { return p.count }

// mbuf rebuilds a buffer handle from an index read off a ring.
func (p *Pool) mbuf(idx uint64) (Mbuf, bool) {
	if idx >= uint64(p.count) {
		return Mbuf{}, false // a corrupt index from a misbehaving peer
	}
	return Mbuf{p: p, idx: uint32(idx)}, true
}
