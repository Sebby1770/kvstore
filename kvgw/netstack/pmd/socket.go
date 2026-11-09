package pmd

import (
	"net"
	"sync"
	"sync/atomic"

	"github.com/Sebby1770/kvstore/kvgw/netstack/fabric"
)

// SocketPort drives the Port API from an ordinary packet socket -- the
// kernel path, for peers that are not on this host. A reader goroutine
// receives straight into pool buffers and queues them on a ring that
// RxBurst polls; each mbuf's aux word names its peer in a small address
// table, and TxBurst sends each buffer back to the peer its aux names.
type SocketPort struct {
	pc   net.PacketConn
	pool *Pool
	rx   *Ring

	amu   sync.RWMutex
	addrs []net.Addr
	index map[string]uint64

	done  chan struct{}
	wg    sync.WaitGroup
	drops atomic.Uint64
}

// NewSocketPort wraps pc with a pool of buffers buffers.
func NewSocketPort(pc net.PacketConn, buffers, dataRoom int) (*SocketPort, error) {
	rs := ceilPow2(buffers)
	r := fabric.NewRegion(int(RingBytes(rs)+PoolBytes(buffers, dataRoom)) + 2*MbufAlign)
	rx, err := FormatRing(r, 0, rs)
	if err != nil {
		return nil, err
	}
	base := (RingBytes(rs) + MbufAlign - 1) &^ (MbufAlign - 1)
	pool, err := FormatPool(r, base, buffers, dataRoom)
	if err != nil {
		return nil, err
	}
	s := &SocketPort{pc: pc, pool: pool, rx: rx, index: map[string]uint64{}, done: make(chan struct{})}
	s.wg.Add(1)
	go s.readLoop()
	return s, nil
}

func (s *SocketPort) addrIndex(a net.Addr) uint64 {
	k := a.String()
	s.amu.RLock()
	i, ok := s.index[k]
	s.amu.RUnlock()
	if ok {
		return i
	}
	s.amu.Lock()
	defer s.amu.Unlock()
	if i, ok := s.index[k]; ok {
		return i
	}
	i = uint64(len(s.addrs))
	s.addrs = append(s.addrs, a)
	s.index[k] = i
	return i
}

// Addr returns the peer address an mbuf's aux word names.
func (s *SocketPort) Addr(m Mbuf) net.Addr {
	s.amu.RLock()
	defer s.amu.RUnlock()
	if i := m.Aux(); i < uint64(len(s.addrs)) {
		return s.addrs[i]
	}
	return nil
}

// SetAddr points an mbuf at a peer before TxBurst.
func (s *SocketPort) SetAddr(m Mbuf, a net.Addr) { m.SetAux(s.addrIndex(a)) }

func (s *SocketPort) readLoop() {
	defer s.wg.Done()
	for {
		m, err := s.pool.Get()
		if err != nil {
			// No buffer: receive into scratch and drop, like a NIC with an
			// empty descriptor ring.
			var scratch [65536]byte
			if _, _, err := s.pc.ReadFrom(scratch[:]); err != nil {
				return
			}
			s.drops.Add(1)
			continue
		}
		n, from, err := s.pc.ReadFrom(m.Room())
		if err != nil {
			s.pool.Put(m)
			select {
			case <-s.done:
				return
			default:
			}
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}
			return
		}
		m.SetLen(n)
		m.SetAux(s.addrIndex(from))
		if s.rx.Enqueue([]uint64{uint64(m.idx)}) == 0 {
			s.pool.Put(m)
			s.drops.Add(1)
		}
	}
}

// RxBurst returns received packets.
func (s *SocketPort) RxBurst(bufs []Mbuf) int {
	idx := make([]uint64, len(bufs))
	n := s.rx.Dequeue(idx)
	for i := 0; i < n; i++ {
		bufs[i], _ = s.pool.mbuf(idx[i])
	}
	return n
}

// TxBurst sends each buffer to the peer its aux word names and frees it.
func (s *SocketPort) TxBurst(bufs []Mbuf) int {
	for _, m := range bufs {
		if a := s.Addr(m); a != nil {
			s.pc.WriteTo(m.Data(), a)
		}
		s.pool.Put(m)
	}
	return len(bufs)
}

// Pool is the port's buffer pool.
func (s *SocketPort) Pool() *Pool { return s.pool }

// Drops counts packets dropped for want of a buffer or ring slot.
func (s *SocketPort) Drops() uint64 { return s.drops.Load() }

// Close closes the socket and stops the reader.
func (s *SocketPort) Close() error {
	close(s.done)
	err := s.pc.Close()
	s.wg.Wait()
	return err
}
