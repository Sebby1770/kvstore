package pmd

import (
	"context"
	"runtime"
	"sync/atomic"
	"time"
)

// Burst is the default number of packets moved per poll (DPDK's usual 32).
const Burst = 32

// Idler is adaptive idle back-off for a polling loop: busy-poll while
// traffic flows (lowest latency), then yield, then sleep in growing steps
// up to Max once the port has been quiet -- DPDK's answer to burning a core
// on an idle queue. Any packet resets it.
type Idler struct {
	Spin  int           // empty polls before yielding (default 256)
	Yield int           // further empty polls with runtime.Gosched (default 64)
	Max   time.Duration // longest sleep (default 1ms)
	empty int
	sleep time.Duration
}

// Busy records a productive poll.
func (d *Idler) Busy() { d.empty, d.sleep = 0, 0 }

// Idle backs off after an empty poll.
func (d *Idler) Idle() {
	if d.Spin == 0 {
		d.Spin = 256
	}
	if d.Yield == 0 {
		d.Yield = 64
	}
	if d.Max == 0 {
		d.Max = time.Millisecond
	}
	d.empty++
	switch {
	case d.empty <= d.Spin:
	case d.empty <= d.Spin+d.Yield:
		runtime.Gosched()
	default:
		if d.sleep == 0 {
			d.sleep = 10 * time.Microsecond
		} else {
			d.sleep = min(d.sleep*2, d.Max)
		}
		time.Sleep(d.sleep)
	}
}

// LcoreStats counts a poll loop's work.
type LcoreStats struct {
	Polls, EmptyPolls, Packets, Bursts uint64
}

// Lcore is a run-to-completion poll loop over one port, pinned to an OS
// thread for its lifetime.
type Lcore struct {
	Port  Port
	Burst int
	Idle  Idler
	stats LcoreStats
}

// Run polls until ctx ends, handing each non-empty burst to handle. handle
// owns the mbufs it is given (send them on, or Free them).
func (l *Lcore) Run(ctx context.Context, handle func(bufs []Mbuf)) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	burst := l.Burst
	if burst <= 0 {
		burst = Burst
	}
	bufs := make([]Mbuf, burst)
	for ctx.Err() == nil {
		atomic.AddUint64(&l.stats.Polls, 1)
		n := l.Port.RxBurst(bufs)
		if n == 0 {
			atomic.AddUint64(&l.stats.EmptyPolls, 1)
			l.Idle.Idle()
			continue
		}
		l.Idle.Busy()
		atomic.AddUint64(&l.stats.Bursts, 1)
		atomic.AddUint64(&l.stats.Packets, uint64(n))
		handle(bufs[:n])
	}
}

// Stats snapshots the loop counters.
func (l *Lcore) Stats() LcoreStats {
	return LcoreStats{Polls: atomic.LoadUint64(&l.stats.Polls), EmptyPolls: atomic.LoadUint64(&l.stats.EmptyPolls),
		Packets: atomic.LoadUint64(&l.stats.Packets), Bursts: atomic.LoadUint64(&l.stats.Bursts)}
}
