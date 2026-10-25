package simnet

import (
	"math/rand/v2"
	"time"
)

// Link describes one direction of a simulated path. The zero Link is a perfect
// wire: no delay, no loss, unlimited bandwidth, no MTU.
type Link struct {
	// Latency is the minimum one-way propagation delay.
	Latency time.Duration
	// Jitter adds a uniformly random extra delay in [0, Jitter]. Because each
	// packet draws its own jitter, jitter alone already reorders packets that
	// are sent closer together than Jitter.
	Jitter time.Duration

	// Loss is the probability that a packet is dropped.
	Loss float64
	// Duplicate is the probability that a surviving packet is delivered
	// twice (each copy draws its own delay).
	Duplicate float64
	// Reorder is the probability that a copy is held back for an extra
	// random delay comparable to Latency+Jitter, so that packets sent after it
	// overtake it.
	Reorder float64
	// Corrupt is the probability that a packet has one random bit flipped.
	Corrupt float64

	// Bandwidth in bytes per second; 0 means unlimited. A limited link
	// transmits one packet at a time, so packets queue behind each other and
	// a burst of N packets takes N×size/Bandwidth to drain.
	Bandwidth int
	// QueueBytes bounds the backlog waiting to be transmitted on a
	// bandwidth-limited link (a router's buffer); packets arriving to a full
	// backlog are tail-dropped. 0 means unbounded. Ignored without Bandwidth.
	QueueBytes int
	// MTU is the largest packet (in bytes) the link carries; larger packets
	// are dropped. 0 means unlimited.
	MTU int
}

// Fate is the pure fault model: it decides what happens to one packet of the
// given size on this link, drawing randomness only from rng. It returns the
// propagation delay of each copy that will be delivered — none if the packet
// is lost or exceeds the MTU, two if it is duplicated — and whether the packet
// is corrupted. Serialisation delay (Bandwidth) is stateful and is not included;
// see TransmitTime.
//
// The number of RNG draws depends only on the link's parameters and on earlier
// draws, never on wall time, so a simulator replaying the same seed reproduces
// the same fates.
func (l Link) Fate(rng *rand.Rand, size int) (copies []time.Duration, corrupt bool) {
	if l.MTU > 0 && size > l.MTU {
		return nil, false
	}
	if l.Loss > 0 && rng.Float64() < l.Loss {
		return nil, false
	}
	n := 1
	if l.Duplicate > 0 && rng.Float64() < l.Duplicate {
		n = 2
	}
	copies = make([]time.Duration, n)
	for i := range copies {
		d := l.Latency
		if l.Jitter > 0 {
			d += time.Duration(rng.Int64N(int64(l.Jitter) + 1))
		}
		if l.Reorder > 0 && rng.Float64() < l.Reorder {
			// Hold the copy back by half to one-and-a-half "path times" so
			// that packets sent shortly after it arrive first. With no
			// latency at all, a millisecond is still enough to be overtaken.
			hold := l.Latency + l.Jitter
			if hold < time.Millisecond {
				hold = time.Millisecond
			}
			d += hold/2 + time.Duration(rng.Int64N(int64(hold)+1))
		}
		copies[i] = d
	}
	corrupt = l.Corrupt > 0 && rng.Float64() < l.Corrupt
	return copies, corrupt
}

// TransmitTime is how long a packet of size bytes occupies the link at its
// Bandwidth (0 for unlimited links).
func (l Link) TransmitTime(size int) time.Duration {
	if l.Bandwidth <= 0 || size <= 0 {
		return 0
	}
	return time.Duration(int64(size) * int64(time.Second) / int64(l.Bandwidth))
}

// CorruptPacket flips one uniformly chosen bit of b in place (no-op on an
// empty slice). It is the corruption Fate's corrupt result calls for.
func CorruptPacket(rng *rand.Rand, b []byte) {
	if len(b) == 0 {
		return
	}
	bit := rng.IntN(len(b) * 8)
	b[bit/8] ^= 1 << (bit % 8)
}
