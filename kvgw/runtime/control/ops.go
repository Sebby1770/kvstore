package control

import (
	"fmt"
	"slices"
)

// ---- ring ----

// validateRing checks a proposed ring. It returns nil when the ring is
// acceptable, otherwise the failing Result (CodeInvalid).
func validateRing(nodes []string, rf, r, w int) *Result {
	bad := func(format string, args ...any) *Result {
		res := fail(CodeInvalid, format, args...)
		return &res
	}
	if len(nodes) == 0 {
		return bad("ring needs at least one node")
	}
	if len(nodes) > MaxRingNodes {
		return bad("ring has %d nodes, limit %d", len(nodes), MaxRingNodes)
	}
	seen := make(map[string]bool, len(nodes))
	for _, n := range nodes {
		if n == "" || len(n) > MaxNameLen {
			return bad("ring node name %q is empty or too long", truncate(n, 64))
		}
		if seen[n] {
			return bad("ring node %q listed twice", truncate(n, 64))
		}
		seen[n] = true
	}
	if rf < 1 || rf > len(nodes) {
		return bad("rf=%d must be in [1, %d]", rf, len(nodes))
	}
	if r < 1 || r > rf || w < 1 || w > rf {
		return bad("r=%d and w=%d must be in [1, rf=%d]", r, w, rf)
	}
	return nil
}

func (f *FSM) setRing(idx uint64, c *Command) Result {
	if c.ExpectEpoch != nil && *c.ExpectEpoch != f.ring.Epoch {
		r := fail(CodeConflict, "ring epoch is %d, expected %d", f.ring.Epoch, *c.ExpectEpoch)
		cur := f.ring.clone()
		r.Ring = &cur
		return r
	}
	if bad := validateRing(c.Nodes, c.RF, c.R, c.W); bad != nil {
		return *bad
	}
	f.ring = RingConfig{
		Epoch: f.ring.Epoch + 1, Nodes: slices.Clone(c.Nodes),
		RF: c.RF, R: c.R, W: c.W, Index: idx, UpdatedAt: c.Now,
	}
	out := f.ring.clone()
	return Result{Ring: &out}
}

// ---- leases ----
//
// A lease is judged only by the time carried in the command being applied,
// never by the replica's own clock, so every replica reaches the same verdict.
// The fencing token is the Raft index of the grant: it increases with every
// new grant of a name (a re-acquire or renew by the current holder keeps it),
// so a resource that remembers the highest token it has seen can reject a
// holder that was paused past its expiry and woke up still believing it holds
// the lease.

func (f *FSM) leaseAcquire(idx uint64, c *Command) Result {
	if r := checkName("name", c.Name, MaxNameLen); r != nil {
		return *r
	}
	if r := checkName("holder", c.Holder, MaxNameLen); r != nil {
		return *r
	}
	if r := checkNow(c.Now); r != nil {
		return *r
	}
	if r := checkMs("ttl_ms", c.TTLms, 1, MaxLeaseTTL); r != nil {
		return *r
	}
	if l, ok := f.leases[c.Name]; ok && c.Now < l.ExpiresAt {
		if l.Holder != c.Holder {
			r := fail(CodeConflict, "lease %q is held by %q", truncate(c.Name, 64), truncate(l.Holder, 64))
			cp := *l
			r.Lease = &cp
			return r
		}
		l.TTLms = c.TTLms
		l.ExpiresAt = addMs(c.Now, c.TTLms)
		l.Renewals++
		cp := *l
		return Result{Lease: &cp, Existing: true}
	}
	if _, ok := f.leases[c.Name]; !ok && len(f.leases) >= MaxLeases {
		return fail(CodeFull, "%d leases, limit %d", len(f.leases), MaxLeases)
	}
	l := &Lease{
		Name: c.Name, Holder: c.Holder, Token: idx, TTLms: c.TTLms,
		AcquiredAt: c.Now, ExpiresAt: addMs(c.Now, c.TTLms),
	}
	f.leases[c.Name] = l
	cp := *l
	return Result{Lease: &cp}
}

// heldBy checks that c names the current holder and grant of lease l.
func heldBy(l *Lease, c *Command) *Result {
	if l.Holder != c.Holder {
		r := fail(CodeNotHolder, "lease %q is held by %q", truncate(l.Name, 64), truncate(l.Holder, 64))
		cp := *l
		r.Lease = &cp
		return &r
	}
	if c.Token != l.Token {
		r := fail(CodeStaleToken, "token %d is not the current grant %d", c.Token, l.Token)
		cp := *l
		r.Lease = &cp
		return &r
	}
	return nil
}

func (f *FSM) leaseRenew(_ uint64, c *Command) Result {
	if r := checkName("name", c.Name, MaxNameLen); r != nil {
		return *r
	}
	if r := checkNow(c.Now); r != nil {
		return *r
	}
	if r := checkMs("ttl_ms", c.TTLms, 0, MaxLeaseTTL); r != nil {
		return *r
	}
	l, ok := f.leases[c.Name]
	if !ok {
		return fail(CodeNotFound, "no lease %q", truncate(c.Name, 64))
	}
	if r := heldBy(l, c); r != nil {
		return *r
	}
	if c.Now >= l.ExpiresAt {
		r := fail(CodeExpired, "lease %q expired", truncate(c.Name, 64))
		cp := *l
		r.Lease = &cp
		return r
	}
	if c.TTLms > 0 {
		l.TTLms = c.TTLms
	}
	l.ExpiresAt = addMs(c.Now, l.TTLms)
	l.Renewals++
	cp := *l
	return Result{Lease: &cp}
}

func (f *FSM) leaseRelease(_ uint64, c *Command) Result {
	if r := checkName("name", c.Name, MaxNameLen); r != nil {
		return *r
	}
	l, ok := f.leases[c.Name]
	if !ok {
		return fail(CodeNotFound, "no lease %q", truncate(c.Name, 64))
	}
	if r := heldBy(l, c); r != nil {
		return *r
	}
	delete(f.leases, c.Name)
	cp := *l
	return Result{Lease: &cp}
}

// ---- registry ----

func (f *FSM) regPut(idx uint64, c *Command) Result {
	if r := checkName("key", c.Key, MaxKeyLen); r != nil {
		return *r
	}
	if len(c.Value) > MaxValueLen {
		return fail(CodeInvalid, "value is %d bytes, limit %d", len(c.Value), MaxValueLen)
	}
	cur, exists := f.registry[c.Key]
	if c.ExpectVersion != nil {
		want := *c.ExpectVersion
		have := uint64(0)
		if exists {
			have = cur.Version
		}
		if want != have {
			r := fail(CodeConflict, "key %q is at version %d, expected %d", truncate(c.Key, 64), have, want)
			if exists {
				cp := cur.clone()
				r.Entry = &cp
			}
			return r
		}
	}
	if !exists {
		if len(f.registry) >= MaxRegistryKeys {
			return fail(CodeFull, "%d registry keys, limit %d", len(f.registry), MaxRegistryKeys)
		}
		cur = &RegistryEntry{Key: c.Key, CreateIndex: idx}
		f.registry[c.Key] = cur
	}
	cur.Value = slices.Clone(c.Value)
	cur.Version = idx
	cur.UpdatedAt = c.Now
	cp := cur.clone()
	return Result{Entry: &cp}
}

func (f *FSM) regDelete(_ uint64, c *Command) Result {
	if r := checkName("key", c.Key, MaxKeyLen); r != nil {
		return *r
	}
	cur, exists := f.registry[c.Key]
	if !exists {
		return fail(CodeNotFound, "no key %q", truncate(c.Key, 64))
	}
	if c.ExpectVersion != nil && *c.ExpectVersion != cur.Version {
		r := fail(CodeConflict, "key %q is at version %d, expected %d", truncate(c.Key, 64), cur.Version, *c.ExpectVersion)
		cp := cur.clone()
		r.Entry = &cp
		return r
	}
	delete(f.registry, c.Key)
	cp := cur.clone()
	return Result{Entry: &cp}
}

func (e RegistryEntry) String() string {
	return fmt.Sprintf("%s@%d", e.Key, e.Version)
}
