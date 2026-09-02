package cluster

import (
	"sync/atomic"
	"time"

	"github.com/Sebby1770/kvstore/kvgw/internal/kv"
)

// CAS is compare-and-set: succeed only if the current winning version equals
// expectedVersion (use 0 to mean "must not exist"), then Put with a fresh
// version. Expired TTL keys count as absent. The per-key lock serializes
// concurrent CAS/Mutate on the same key so two coordinators-in-one-process
// cannot both observe the same version and both write.
func (c *Cluster) CAS(key []byte, expectedVersion uint64, val []byte) (WriteMeta, error) {
	return c.cas(key, expectedVersion, val, 0)
}

// CASTTL is CAS with a native expiry. ttl <= 0 means immortal.
func (c *Cluster) CASTTL(key []byte, expectedVersion uint64, val []byte, ttl time.Duration) (WriteMeta, error) {
	var exp uint64
	if ttl > 0 {
		exp = uint64(c.Now().Add(ttl).Unix())
	}
	return c.cas(key, expectedVersion, val, exp)
}

func (c *Cluster) cas(key []byte, expectedVersion uint64, val []byte, expiresAt uint64) (WriteMeta, error) {
	start := time.Now()
	defer func() { c.observePut(time.Since(start)) }()
	unlock := c.locks.lock(string(key))
	defer unlock()

	addrs, nodes := c.replicas(key)
	reads := gatherReads(addrs, nodes, key)

	var best kv.ReadResult
	haveBest := false
	responders := 0
	for _, rd := range reads {
		if rd.err != nil {
			continue
		}
		responders++
		c.clock.witness(rd.res.Version)
		if !haveBest || rd.res.Version > best.Version {
			best = rd.res
			haveBest = true
		}
	}
	if responders < c.R() {
		atomic.AddUint64(&c.mQuorumFails, 1)
		return WriteMeta{Replicas: addrs, Required: c.W()}, ErrQuorum
	}

	current := uint64(0)
	if haveBest && best.State == kv.Present && !c.resultExpired(best) {
		current = best.Version
	}
	if current != expectedVersion {
		return WriteMeta{Replicas: addrs, Version: current, Required: c.W()}, ErrConflict
	}

	atomic.AddUint64(&c.mPuts, 1)
	version := c.clock.tick(best.Version)
	return c.broadcastPut(addrs, nodes, key, val, version, expiresAt)
}
