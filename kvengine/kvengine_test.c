// Smoke + regression tests for the v3 storage engine: last-writer-wins,
// versioned tombstones, native TTL, lazy expiry, rehash/GC, auto-grow, and
// header CRC detection.

#include "kvengine.h"

#include <assert.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static int count_cb(const void *k, uint32_t klen,
                    const void *v, uint32_t vlen,
                    uint64_t version, uint64_t expires_at, void *ctx) {
    (void)k; (void)klen; (void)v; (void)vlen; (void)version; (void)expires_at;
    int *n = (int *)ctx;
    (*n)++;
    return 0;
}

typedef struct { int live, tombs; } all_ctx;

static int all_cb(const void *k, uint32_t klen,
                  const void *v, uint32_t vlen,
                  uint64_t version, uint64_t expires_at,
                  int tombstone, void *ctx) {
    (void)k; (void)klen; (void)v; (void)vlen; (void)version; (void)expires_at;
    all_ctx *c = (all_ctx *)ctx;
    if (tombstone) c->tombs++;
    else           c->live++;
    return 0;
}

static void expect_val(kv_db *db, const char *key, const char *want, uint64_t want_ver) {
    char buf[KV_VALUE_MAX];
    uint32_t n = 0;
    uint64_t ver = 0, exp = 0;
    assert(kv_get(db, key, (uint32_t)strlen(key), buf, sizeof buf, &n, &ver, &exp) == KV_OK);
    assert(n == (uint32_t)strlen(want) && memcmp(buf, want, n) == 0);
    assert(ver == want_ver);
}

int main(void) {
    const char *path = "/tmp/kvstore_kv_test.db";
    unlink(path);

    kv_db *db = NULL;
    assert(kv_open(path, 64, &db) == KV_OK);

    assert(kv_put(db, "alpha", 5, "one",   3, 1) == KV_OK);
    assert(kv_put(db, "beta",  4, "two",   3, 1) == KV_OK);
    assert(kv_put(db, "gamma", 5, "three", 5, 1) == KV_OK);

    char buf[KV_VALUE_MAX];
    uint32_t n;
    uint64_t ver, exp;
    assert(kv_get(db, "beta", 4, buf, sizeof buf, &n, &ver, &exp) == KV_OK);
    assert(n == 3 && memcmp(buf, "two", 3) == 0 && ver == 1 && exp == 0);

    // Overwrite with a higher version wins.
    assert(kv_put(db, "beta", 4, "TWO!", 4, 2) == KV_OK);
    assert(kv_get(db, "beta", 4, buf, sizeof buf, &n, &ver, &exp) == KV_OK);
    assert(n == 4 && memcmp(buf, "TWO!", 4) == 0 && ver == 2);

    // Last-writer-wins: a stale (lower-version) write is a no-op.
    assert(kv_put(db, "beta", 4, "old", 3, 1) == KV_OK);
    assert(kv_get(db, "beta", 4, buf, sizeof buf, &n, &ver, &exp) == KV_OK);
    assert(n == 4 && memcmp(buf, "TWO!", 4) == 0 && ver == 2);

    // Idempotent replay: re-applying the same version is not an error.
    assert(kv_put(db, "beta", 4, "TWO!", 4, 2) == KV_OK);
    assert(kv_count(db) == 3);

    // Versioned tombstone: delete, then a NEWER put revives the key.
    assert(kv_del(db, "alpha", 5, 5) == KV_OK);
    assert(kv_get(db, "alpha", 5, buf, sizeof buf, &n, &ver, &exp) == KV_DELETED);
    assert(ver == 5);
    assert(kv_put(db, "alpha", 5, "back", 4, 6) == KV_OK);
    expect_val(db, "alpha", "back", 6);

    // A stale put (older than the tombstone) must NOT revive the key.
    assert(kv_del(db, "gamma", 5, 10) == KV_OK);
    assert(kv_put(db, "gamma", 5, "nope", 4, 9) == KV_OK);
    assert(kv_get(db, "gamma", 5, buf, sizeof buf, &n, &ver, &exp) == KV_DELETED);

    int seen = 0;
    assert(kv_scan(db, count_cb, &seen) == KV_OK);
    assert(seen == 2);  // alpha (revived) + beta; gamma is tombstoned

    all_ctx ac = {0, 0};
    assert(kv_scan_all(db, all_cb, &ac) == KV_OK);
    assert(ac.live == 2 && ac.tombs == 1);
    assert(kv_tombstones(db) == 1);

    assert(kv_max_version(db) == 10);
    kv_close(db);

    // Reopen and verify persistence (and that the header CRC validates).
    db = NULL;
    assert(kv_open(path, 64, &db) == KV_OK);
    assert(kv_get(db, "beta", 4, buf, sizeof buf, &n, &ver, &exp) == KV_OK);
    assert(n == 4 && memcmp(buf, "TWO!", 4) == 0 && ver == 2);
    assert(kv_get(db, "gamma", 5, buf, sizeof buf, &n, &ver, &exp) == KV_DELETED);
    kv_close(db);

    // --- Native TTL: inject a clock, expire, lazy-tombstone. ---
    unlink(path);
    db = NULL;
    assert(kv_open(path, 32, &db) == KV_OK);
    kv_set_time(db, 1000);
    assert(kv_put_ttl(db, "ephem", 5, "soon", 4, 1, 1010) == KV_OK);
    assert(kv_put(db, "stay", 4, "forever", 7, 1) == KV_OK);
    assert(kv_get(db, "ephem", 5, buf, sizeof buf, &n, &ver, &exp) == KV_OK);
    assert(n == 4 && memcmp(buf, "soon", 4) == 0 && exp == 1010);

    kv_set_time(db, 1010); // now >= expires_at
    assert(kv_get(db, "ephem", 5, buf, sizeof buf, &n, &ver, &exp) == KV_DELETED);
    assert(ver == 1);
    assert(kv_get(db, "stay", 4, buf, sizeof buf, &n, &ver, &exp) == KV_OK);

    seen = 0;
    assert(kv_scan(db, count_cb, &seen) == KV_OK);
    assert(seen == 1);

    // A strictly older put must not revive an expired (now tombstoned) key.
    assert(kv_put(db, "ephem", 5, "nope", 4, 0) == KV_OK);
    assert(kv_get(db, "ephem", 5, buf, sizeof buf, &n, &ver, &exp) == KV_DELETED);

    // A newer version revives it.
    assert(kv_put_ttl(db, "ephem", 5, "later", 5, 2, 2000) == KV_OK);
    assert(kv_get(db, "ephem", 5, buf, sizeof buf, &n, &ver, &exp) == KV_OK);
    assert(n == 5 && memcmp(buf, "later", 5) == 0 && exp == 2000);
    kv_close(db);

    // --- Rehash preserves live keys and reclaims tombstones. ---
    unlink(path);
    db = NULL;
    assert(kv_open(path, 16, &db) == KV_OK);
    kv_set_time(db, 50);
    assert(kv_put(db, "keep-a", 6, "A", 1, 1) == KV_OK);
    assert(kv_put(db, "keep-b", 6, "B", 1, 2) == KV_OK);
    assert(kv_put_ttl(db, "dead", 4, "x", 1, 3, 40) == KV_OK); // already expired
    assert(kv_put(db, "gone", 4, "z", 1, 4) == KV_OK);
    assert(kv_del(db, "gone", 4, 5) == KV_OK);
    assert(kv_tombstones(db) >= 1);

    assert(kv_rehash(db, 64) == KV_OK);
    assert(kv_capacity(db) == 64);
    assert(kv_tombstones(db) == 0);
    assert(kv_count(db) == 2);
    expect_val(db, "keep-a", "A", 1);
    expect_val(db, "keep-b", "B", 2);
    assert(kv_get(db, "gone", 4, buf, sizeof buf, &n, &ver, &exp) == KV_NOT_FOUND);
    assert(kv_get(db, "dead", 4, buf, sizeof buf, &n, &ver, &exp) == KV_NOT_FOUND);

    // LWW still holds after rehash (strictly older version is ignored).
    assert(kv_put(db, "keep-a", 6, "stale", 5, 0) == KV_OK);
    expect_val(db, "keep-a", "A", 1);
    assert(kv_put(db, "keep-a", 6, "AA", 2, 9) == KV_OK);
    expect_val(db, "keep-a", "AA", 9);
    kv_close(db);

    db = NULL;
    assert(kv_open(path, 16, &db) == KV_OK); // capacity arg ignored; file has 64
    assert(kv_capacity(db) == 64);
    expect_val(db, "keep-a", "AA", 9);
    kv_close(db);

    // --- Auto-grow: inserting past ~70% load doubles capacity. ---
    unlink(path);
    db = NULL;
    assert(kv_open(path, 8, &db) == KV_OK);
    char kbuf[2] = {0, 0};
    for (int i = 0; i < 7; i++) {
        kbuf[0] = (char)('a' + i);
        assert(kv_put(db, kbuf, 1, "v", 1, (uint64_t)(i + 1)) == KV_OK);
    }
    assert(kv_capacity(db) >= 16);
    assert(kv_count(db) == 7);
    for (int i = 0; i < 7; i++) {
        kbuf[0] = (char)('a' + i);
        expect_val(db, kbuf, "v", (uint64_t)(i + 1));
    }
    kv_close(db);

    // --- CRC / magic: a torn or forged header is KV_BAD_FILE. ---
    unlink(path);
    db = NULL;
    assert(kv_open(path, 8, &db) == KV_OK);
    assert(kv_put(db, "k", 1, "v", 1, 1) == KV_OK);
    kv_close(db);

    int fd = open(path, O_RDWR);
    assert(fd >= 0);
    uint32_t bogus = 0xDeadBeef;
    assert(lseek(fd, 16, SEEK_SET) == 16); // header.count
    assert(write(fd, &bogus, 4) == 4);
    close(fd);

    db = NULL;
    assert(kv_open(path, 8, &db) == KV_BAD_FILE);

    unlink(path);
    fd = open(path, O_RDWR | O_CREAT, 0644);
    assert(fd >= 0);
    char page[4096];
    memset(page, 0, sizeof page);
    memcpy(page, "NOPE0000", 8);
    assert(write(fd, page, sizeof page) == (ssize_t)sizeof page);
    close(fd);
    db = NULL;
    assert(kv_open(path, 8, &db) == KV_BAD_FILE);

    unlink(path);
    printf("kvengine: ok\n");
    return 0;
}
