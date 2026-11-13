package pmd

import (
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/Sebby1770/kvstore/kvgw/netstack/fabric"
)

// Port is a packet interface driven by polling.
type Port interface {
	// RxBurst fills bufs with received packets and returns how many. The
	// caller owns them and must Free them (or send them on).
	RxBurst(bufs []Mbuf) int
	// TxBurst queues bufs for sending and returns how many it took; the
	// rest still belong to the caller.
	TxBurst(bufs []Mbuf) int
	// Pool is where packets to send are allocated.
	Pool() *Pool
	Close() error
}

// MemifConfig sizes a shared-memory interface.
type MemifConfig struct {
	RingSize int // descriptors per direction (power of two, default 1024)
	Buffers  int // mbufs shared by both sides (default 4096)
	DataRoom int // bytes per mbuf (default 2048)
}

func (c *MemifConfig) defaults() {
	if c.RingSize <= 0 {
		c.RingSize = 1024
	}
	if c.Buffers <= 0 {
		c.Buffers = 4096
	}
	if c.DataRoom <= 0 {
		c.DataRoom = 2048
	}
}

// Memif file layout:
//
//	   0  magic (written last by the creator: the peer may attach once it is set)
//	   8  version
//	  16  ring size
//	  24  buffer count
//	  32  data room
//	  40  creator heartbeat counter   48  attacher heartbeat counter
//	4096  ring creator->attacher, then ring attacher->creator, then the pool
const (
	memifMagic   = 0x31464d454d5f4b56 // "VK_MEMF1"
	memifVersion = 1
	memifHeader  = 4096
)

// MemifPort is one side of a shared-memory interface.
type MemifPort struct {
	r       *fabric.Region
	tx, rx  *Ring
	pool    *Pool
	creator bool
	idx     []uint64
	stats   PortStats
}

// PortStats counts a port's traffic.
type PortStats struct {
	RxPackets, TxPackets, TxDropped, RxBadIndex uint64
}

func memifLayout(c MemifConfig) (ringA, ringB, pool, total uint64) {
	ringA = memifHeader
	ringB = ringA + RingBytes(c.RingSize)
	pool = (ringB + RingBytes(c.RingSize) + MbufAlign - 1) &^ (MbufAlign - 1)
	total = pool + PoolBytes(c.Buffers, c.DataRoom) + MbufAlign
	return
}

// CreateMemif creates (or reformats) the interface file at path and returns
// the creator's side.
func CreateMemif(path string, cfg MemifConfig) (*MemifPort, error) {
	cfg.defaults()
	if cfg.RingSize&(cfg.RingSize-1) != 0 {
		return nil, ErrRingSize
	}
	a, b, p, total := memifLayout(cfg)
	os.Remove(path)
	r, err := fabric.MapFile(path, int(total))
	if err != nil {
		return nil, err
	}
	m := &MemifPort{r: r, creator: true}
	if m.tx, err = FormatRing(r, a, cfg.RingSize); err == nil {
		if m.rx, err = FormatRing(r, b, cfg.RingSize); err == nil {
			m.pool, err = FormatPool(r, p, cfg.Buffers, cfg.DataRoom)
		}
	}
	if err != nil {
		r.Close()
		return nil, err
	}
	r.Store64(8, memifVersion)
	r.Store64(16, uint64(cfg.RingSize))
	r.Store64(24, uint64(cfg.Buffers))
	r.Store64(32, uint64(cfg.DataRoom))
	r.Store64(0, memifMagic) // publish: everything above is now visible
	return m, nil
}

// OpenMemif attaches to an interface another process created, waiting up to
// timeout for the creator to publish it.
func OpenMemif(path string, timeout time.Duration) (*MemifPort, error) {
	deadline := time.Now().Add(timeout)
	for {
		m, err := tryOpenMemif(path)
		if err == nil || time.Now().After(deadline) {
			return m, err
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func tryOpenMemif(path string) (*MemifPort, error) {
	st, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if st.Size() < memifHeader {
		return nil, errors.New("pmd: memif file too small")
	}
	r, err := fabric.MapFile(path, int(st.Size()))
	if err != nil {
		return nil, err
	}
	fail := func(err error) (*MemifPort, error) { r.Close(); return nil, err }
	if v, _ := r.Load64(0); v != memifMagic {
		return fail(errors.New("pmd: memif not published yet"))
	}
	if v, _ := r.Load64(8); v != memifVersion {
		return fail(fmt.Errorf("pmd: memif version %d", v))
	}
	rs, _ := r.Load64(16)
	nb, _ := r.Load64(24)
	dr, _ := r.Load64(32)
	cfg := MemifConfig{RingSize: int(rs), Buffers: int(nb), DataRoom: int(dr)}
	a, b, p, total := memifLayout(cfg)
	if total > r.Size() {
		return fail(errors.New("pmd: memif file shorter than its layout"))
	}
	m := &MemifPort{r: r}
	// The attacher sends on the creator's receive ring and vice versa.
	if m.rx, err = OpenRing(r, a, cfg.RingSize); err == nil {
		if m.tx, err = OpenRing(r, b, cfg.RingSize); err == nil {
			m.pool, err = OpenPool(r, p, cfg.Buffers, cfg.DataRoom)
		}
	}
	if err != nil {
		return fail(err)
	}
	return m, nil
}

// Pool is the shared mbuf pool.
func (m *MemifPort) Pool() *Pool { return m.pool }

// RxBurst receives packets in place: the returned mbufs are the very
// buffers the peer wrote.
func (m *MemifPort) RxBurst(bufs []Mbuf) int {
	if cap(m.idx) < len(bufs) {
		m.idx = make([]uint64, len(bufs))
	}
	idx := m.idx[:len(bufs)]
	n := m.rx.Dequeue(idx)
	out := 0
	for _, i := range idx[:n] {
		mb, ok := m.pool.mbuf(i)
		if !ok {
			atomic.AddUint64(&m.stats.RxBadIndex, 1)
			continue
		}
		bufs[out] = mb
		out++
	}
	atomic.AddUint64(&m.stats.RxPackets, uint64(out))
	return out
}

// TxBurst hands buffers to the peer by enqueuing their indices.
func (m *MemifPort) TxBurst(bufs []Mbuf) int {
	var stack [64]uint64
	idx := stack[:0]
	for _, b := range bufs {
		idx = append(idx, uint64(b.idx))
	}
	n := m.tx.Enqueue(idx)
	atomic.AddUint64(&m.stats.TxPackets, uint64(n))
	atomic.AddUint64(&m.stats.TxDropped, uint64(len(bufs)-n))
	return n
}

// Stats snapshots the port counters.
func (m *MemifPort) Stats() PortStats {
	return PortStats{RxPackets: atomic.LoadUint64(&m.stats.RxPackets), TxPackets: atomic.LoadUint64(&m.stats.TxPackets),
		TxDropped: atomic.LoadUint64(&m.stats.TxDropped), RxBadIndex: atomic.LoadUint64(&m.stats.RxBadIndex)}
}

// Close unmaps this side.
func (m *MemifPort) Close() error { return m.r.Close() }
