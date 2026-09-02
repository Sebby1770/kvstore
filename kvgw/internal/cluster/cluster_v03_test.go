package cluster

import (
	"bytes"
	"errors"
	"testing"
	"time"
)

func TestCASSuccessAndConflict(t *testing.T) {
	c, _ := build(3, 3, 2, 2)

	meta, err := c.CAS([]byte("k"), 0, []byte("first"))
	if err != nil {
		t.Fatalf("create CAS: %v", err)
	}
	if meta.Version == 0 {
		t.Fatal("expected a non-zero version after create")
	}

	val, found, got, err := c.Get([]byte("k"))
	if err != nil || !found || string(val) != "first" {
		t.Fatalf("get after create: found=%v val=%q err=%v", found, val, err)
	}

	next, err := c.CAS([]byte("k"), got.Version, []byte("second"))
	if err != nil {
		t.Fatalf("update CAS: %v", err)
	}
	if next.Version <= got.Version {
		t.Fatalf("CAS version did not advance: %d -> %d", got.Version, next.Version)
	}
	val, found, _, err = c.Get([]byte("k"))
	if err != nil || !found || string(val) != "second" {
		t.Fatalf("get after update: found=%v val=%q err=%v", found, val, err)
	}

	if _, err := c.CAS([]byte("k"), got.Version, []byte("stale")); !errors.Is(err, ErrConflict) {
		t.Fatalf("expected ErrConflict on stale version, got %v", err)
	}
	val, _, _, _ = c.Get([]byte("k"))
	if string(val) != "second" {
		t.Fatalf("conflict overwrote value: %q", val)
	}

	if _, err := c.CAS([]byte("k"), 0, []byte("create-again")); !errors.Is(err, ErrConflict) {
		t.Fatalf("expected ErrConflict creating an existing key, got %v", err)
	}

	if _, err := c.CAS([]byte("missing"), 7, []byte("nope")); !errors.Is(err, ErrConflict) {
		t.Fatalf("expected ErrConflict on missing key with nonzero expected, got %v", err)
	}
}

func TestTTLPutThenGetAfterExpiry(t *testing.T) {
	c, _ := build(3, 3, 2, 2)
	now := time.Unix(1_700_000_000, 0)
	c.SetNow(func() time.Time { return now })

	if _, err := c.PutTTL([]byte("ephem"), []byte("soon"), 10*time.Second); err != nil {
		t.Fatalf("PutTTL: %v", err)
	}
	val, found, meta, err := c.Get([]byte("ephem"))
	if err != nil || !found || string(val) != "soon" {
		t.Fatalf("get before expiry: found=%v val=%q err=%v", found, val, err)
	}
	if meta.ExpiresAt != now.Add(10*time.Second).Unix() {
		t.Fatalf("ExpiresAt = %d, want %d", meta.ExpiresAt, now.Add(10*time.Second).Unix())
	}
	rows, _ := c.Scan()
	if len(rows) != 1 || rows[0].Value != "soon" || rows[0].ExpiresAt == nil {
		t.Fatalf("scan before expiry: %+v", rows)
	}

	now = now.Add(10 * time.Second) // expiresAt <= now → gone
	val, found, _, err = c.Get([]byte("ephem"))
	if err != nil || found {
		t.Fatalf("get after expiry: found=%v val=%q err=%v", found, val, err)
	}
	rows, _ = c.Scan()
	if len(rows) != 0 {
		t.Fatalf("scan after expiry returned %d rows: %+v", len(rows), rows)
	}
}

func TestExportImportRoundTrip(t *testing.T) {
	src, _ := build(3, 3, 2, 2)
	now := time.Unix(1_800_000_000, 0)
	src.SetNow(func() time.Time { return now })

	if _, err := src.Put([]byte("immortal"), []byte("hello")); err != nil {
		t.Fatal(err)
	}
	if _, err := src.PutTTL([]byte("lived"), []byte("world"), time.Hour); err != nil {
		t.Fatal(err)
	}
	if _, err := src.PutTTL([]byte("dead"), []byte("gone"), time.Second); err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Second) // dead expired; lived still good

	doc, errs := src.Export()
	if len(errs) != 0 {
		t.Fatalf("export errors: %v", errs)
	}
	if len(doc.Entries) != 2 {
		t.Fatalf("export entries = %d, want 2 (immortal + lived): %+v", len(doc.Entries), doc.Entries)
	}
	for _, e := range doc.Entries {
		if bytes.Contains([]byte(e.Value), []byte("TGTTL1")) || bytes.Contains([]byte(e.Value), []byte("KVTTL1")) {
			t.Fatalf("export leaked envelope: %+v", e)
		}
		if e.Key == "dead" {
			t.Fatal("export included expired key")
		}
	}

	// Import into a fresh cluster, plus an already-expired row that must skip.
	dst, _ := build(3, 3, 2, 2)
	dst.SetNow(func() time.Time { return now })
	expired := now.Add(-time.Second).Unix()
	doc.Entries = append(doc.Entries, ExportEntry{Key: "skip-me", Value: "x", ExpiresAt: &expired})

	imported, skipped, ierrs := dst.Import(doc)
	if len(ierrs) != 0 {
		t.Fatalf("import errors: %v", ierrs)
	}
	if imported != 2 || skipped != 1 {
		t.Fatalf("imported=%d skipped=%d, want 2/1", imported, skipped)
	}
	val, found, _, err := dst.Get([]byte("immortal"))
	if err != nil || !found || string(val) != "hello" {
		t.Fatalf("imported immortal: found=%v val=%q err=%v", found, val, err)
	}
	val, found, meta, err := dst.Get([]byte("lived"))
	if err != nil || !found || string(val) != "world" {
		t.Fatalf("imported lived: found=%v val=%q err=%v", found, val, err)
	}
	if meta.ExpiresAt == 0 {
		t.Fatal("imported TTL key lost its expiry")
	}
	_, found, _, err = dst.Get([]byte("skip-me"))
	if err != nil || found {
		t.Fatalf("expired import should be skipped, found=%v err=%v", found, err)
	}
}

func TestPrefixScan(t *testing.T) {
	c, _ := build(4, 3, 2, 2)
	for _, k := range []string{"foo", "foobar", "bar", "fo"} {
		if _, err := c.Put([]byte(k), []byte("v-"+k)); err != nil {
			t.Fatal(err)
		}
	}
	rows, errs := c.ScanPrefix("foo")
	if len(errs) != 0 {
		t.Fatalf("scan errors: %v", errs)
	}
	if len(rows) != 2 {
		t.Fatalf("prefix foo: got %d rows %+v, want 2", len(rows), rows)
	}
	if rows[0].Key != "foo" || rows[1].Key != "foobar" {
		t.Fatalf("order/keys: %+v", rows)
	}
	all, _ := c.ScanPrefix("")
	if len(all) != 4 {
		t.Fatalf("empty prefix should return all 4, got %d", len(all))
	}
	none, _ := c.ScanPrefix("zzz")
	if len(none) != 0 {
		t.Fatalf("zzz prefix: %+v", none)
	}
}

func TestEnvelopeDoesNotBreakRawValues(t *testing.T) {
	cases := [][]byte{
		[]byte("hello"),
		[]byte("world"),
		[]byte(""),
		[]byte("TGTTL1"),
		[]byte("TGTTL1\n"),
		[]byte("TGTTL1\nnotanumber\npayload"),
		[]byte("TGTTL1\n\n"),
		[]byte("KVTTL1"),
		[]byte("KVTTL1\n"),
		[]byte("KVTTL1\nnotanumber\npayload"),
		append([]byte("plain-"), 0x00, 0xff),
	}
	for _, raw := range cases {
		val, exp, has := DecodeValue(raw)
		if has {
			t.Fatalf("raw %q treated as envelope (exp=%d val=%q)", raw, exp, val)
		}
		if !bytes.Equal(val, raw) {
			t.Fatalf("raw %q decoded as %q", raw, val)
		}
	}

	enc := EncodeTTL([]byte("world"), 42)
	val, exp, has := DecodeValue(enc)
	if !has || exp != 42 || string(val) != "world" {
		t.Fatalf("round-trip: has=%v exp=%d val=%q", has, exp, val)
	}
	if !bytes.Contains(enc, []byte(ttlMagic)) {
		t.Fatal("encoded form missing magic")
	}

	// Empty payload + TTL, and a payload that itself contains the magic.
	enc = EncodeTTL([]byte("TGTTL1\n999\ninner"), 7)
	val, exp, has = DecodeValue(enc)
	if !has || exp != 7 || string(val) != "TGTTL1\n999\ninner" {
		t.Fatalf("nested-looking payload: has=%v exp=%d val=%q", has, exp, val)
	}

	legacy := []byte("TGTTL1\n42\nworld")
	val, exp, has = DecodeValue(legacy)
	if !has || exp != 42 || string(val) != "world" {
		t.Fatalf("legacy envelope: has=%v exp=%d val=%q", has, exp, val)
	}

	c, _ := build(3, 3, 2, 2)
	if _, err := c.Put([]byte("hello"), []byte("world")); err != nil {
		t.Fatal(err)
	}
	got, found, _, err := c.Get([]byte("hello"))
	if err != nil || !found || string(got) != "world" {
		t.Fatalf("legacy put/get: found=%v val=%q err=%v", found, got, err)
	}
	rows, _ := c.Scan()
	if len(rows) != 1 || rows[0].Value != "world" || rows[0].ExpiresAt != nil {
		t.Fatalf("legacy scan leaked envelope or ttl: %+v", rows)
	}
}

func TestCASTreatsExpiredAsAbsent(t *testing.T) {
	c, _ := build(3, 3, 2, 2)
	now := time.Unix(2_000_000_000, 0)
	c.SetNow(func() time.Time { return now })
	if _, err := c.PutTTL([]byte("k"), []byte("old"), time.Second); err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Second)
	if _, err := c.CAS([]byte("k"), 0, []byte("new")); err != nil {
		t.Fatalf("CAS 0 on expired key should succeed: %v", err)
	}
	val, found, _, err := c.Get([]byte("k"))
	if err != nil || !found || string(val) != "new" {
		t.Fatalf("after CAS-on-expired: found=%v val=%q err=%v", found, val, err)
	}
}
