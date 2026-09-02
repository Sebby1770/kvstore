package cluster

import (
	"bytes"
	"strconv"

	"github.com/Sebby1770/kvstore/kvgw/internal/kv"
)

// Native TTL lives in the C engine (expires_at on the slot). These envelopes
// remain so values written before v3 still decode:
//
//	KVTTL1\n<unix-seconds>\n<raw value>
//	TGTTL1\n<unix-seconds>\n<raw value>   (legacy)
const ttlMagic = "KVTTL1\n"
const ttlMagicLegacy = "TGTTL1\n"

// EncodeTTL wraps val with an expiry timestamp (unix seconds). New writes
// prefer the native engine field; this remains for tests and old blobs.
func EncodeTTL(val []byte, expiresAt int64) []byte {
	head := ttlMagic + strconv.FormatInt(expiresAt, 10) + "\n"
	out := make([]byte, 0, len(head)+len(val))
	out = append(out, head...)
	out = append(out, val...)
	return out
}

func decodeEnvelope(raw []byte, magic string) (val []byte, expiresAt int64, hasTTL bool) {
	if !bytes.HasPrefix(raw, []byte(magic)) {
		return raw, 0, false
	}
	rest := raw[len(magic):]
	nl := bytes.IndexByte(rest, '\n')
	if nl < 0 {
		return raw, 0, false
	}
	exp, err := strconv.ParseInt(string(rest[:nl]), 10, 64)
	if err != nil {
		return raw, 0, false
	}
	return rest[nl+1:], exp, true
}

// DecodeValue unwraps a TTL envelope. Values that do not start with a known
// magic prefix (or that are malformed) are returned unchanged with hasTTL=false.
func DecodeValue(raw []byte) (val []byte, expiresAt int64, hasTTL bool) {
	if val, exp, has := decodeEnvelope(raw, ttlMagic); has {
		return val, exp, true
	}
	if val, exp, has := decodeEnvelope(raw, ttlMagicLegacy); has {
		return val, exp, true
	}
	return raw, 0, false
}

func (c *Cluster) expired(expiresAt int64) bool {
	return expiresAt > 0 && expiresAt <= c.Now().Unix()
}

func (c *Cluster) valueExpired(raw []byte) bool {
	_, exp, has := DecodeValue(raw)
	return has && c.expired(exp)
}

func (c *Cluster) resultExpired(r kv.ReadResult) bool {
	if r.State != kv.Present {
		return false
	}
	if r.ExpiresAt != 0 && c.expired(int64(r.ExpiresAt)) {
		return true
	}
	return c.valueExpired(r.Value)
}

// unwrapPresent returns the decoded value and native-or-envelope expiry.
func (c *Cluster) unwrapPresent(r kv.ReadResult) (val []byte, expiresAt int64, expired bool) {
	if r.State != kv.Present {
		return nil, 0, false
	}
	if r.ExpiresAt != 0 && c.expired(int64(r.ExpiresAt)) {
		return nil, int64(r.ExpiresAt), true
	}
	val, exp, hasTTL := DecodeValue(r.Value)
	if hasTTL && c.expired(exp) {
		return val, exp, true
	}
	if hasTTL {
		return val, exp, false
	}
	return r.Value, int64(r.ExpiresAt), false
}
