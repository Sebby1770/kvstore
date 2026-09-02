// KVStore storage engine — mmap-backed open-addressed hash table.
//
// File layout:
//   [4096-byte header][N slots]
// Header is a fixed page so users can read it with `xxd` and recognize a DB.
// Each slot is fixed-size and carries a version + optional expires_at, so the
// cluster above can do last-writer-wins conflict resolution and native TTL.
// Writes are last-writer-wins and idempotent, which makes replication and
// read-repair safe to replay.
//
// Durability: a slot is msync'd before the header that counts it, and the
// header carries a CRC32 so a torn header page is detected on open
// (KV_BAD_FILE) rather than silently trusted.
//
// Format v3 (MAGIC KVST0003): slots gained expires_at (unix seconds, 0 = never).
// Expired occupied slots are lazily tombstoned on get/scan. kv_rehash drops
// tombstones (GC) and copies only live non-expired keys. kv_compact rehashes
// at the current capacity (GC without grow). Inserts that would return KV_FULL
// double capacity via rehash and retry once.

#include "kvengine.h"

#include <fcntl.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#define HEADER_SIZE   4096u
#define MAGIC         "KVST0003"
#define MAGIC_LEN     8
#define FORMAT_VER    3u
#define SLOT_EMPTY    0u
#define SLOT_OCCUPIED 1u
#define SLOT_TOMB     2u
#define MAX_LOAD_NUM  7
#define MAX_LOAD_DEN  10

struct header {
    char     magic[MAGIC_LEN];
    uint32_t version;       // file format version
    uint32_t capacity;
    uint32_t count;
    uint32_t tombstones;
    uint32_t crc;           // CRC32 of every header byte before this field
};

struct slot {
    uint32_t state;
    uint32_t key_len;
    uint32_t value_len;
    uint32_t _pad;          // keep version 8-byte aligned for a stable layout
    uint64_t version;       // logical version for last-writer-wins
    uint64_t expires_at;    // unix seconds; 0 = never
    uint8_t  key[KV_KEY_MAX];
    uint8_t  value[KV_VALUE_MAX];
};

struct rec {
    uint32_t key_len;
    uint32_t value_len;
    uint64_t version;
    uint64_t expires_at;
    uint8_t  key[KV_KEY_MAX];
    uint8_t  value[KV_VALUE_MAX];
};

struct kv_db {
    int            fd;
    size_t         map_size;
    void          *map;
    struct header *hdr;
    struct slot   *slots;
    uint64_t       clock;   // 0 = use time(NULL)
};

// CRC32 (IEEE 802.3), bitwise — small and dependency-free.
static uint32_t crc32(const void *data, size_t len) {
    const uint8_t *p = (const uint8_t *)data;
    uint32_t crc = 0xFFFFFFFFu;
    for (size_t i = 0; i < len; i++) {
        crc ^= p[i];
        for (int b = 0; b < 8; b++) {
            uint32_t mask = -(crc & 1u);
            crc = (crc >> 1) ^ (0xEDB88320u & mask);
        }
    }
    return ~crc;
}

static uint32_t header_crc(const struct header *h) {
    return crc32(h, offsetof(struct header, crc));
}

static void sync_header(kv_db *db) {
    db->hdr->crc = header_crc(db->hdr);
    msync(db->hdr, sizeof(*db->hdr), MS_SYNC);
}

static uint64_t now_sec(const kv_db *db) {
    if (db->clock != 0) return db->clock;
    time_t t = time(NULL);
    if (t < 0) return 0;
    return (uint64_t)t;
}

void kv_set_time(kv_db *db, uint64_t unix_sec) {
    if (db) db->clock = unix_sec;
}

static uint64_t fnv1a(const void *data, uint32_t len) {
    const uint8_t *p = (const uint8_t *)data;
    uint64_t h = 1469598103934665603ull;
    for (uint32_t i = 0; i < len; i++) {
        h ^= p[i];
        h *= 1099511628211ull;
    }
    return h;
}

static int keys_eq(const struct slot *s, const void *key, uint32_t key_len) {
    return s->key_len == key_len && memcmp(s->key, key, key_len) == 0;
}

static int slot_expired(const kv_db *db, const struct slot *s) {
    if (s->state != SLOT_OCCUPIED) return 0;
    if (s->expires_at == 0) return 0;
    return now_sec(db) >= s->expires_at;
}

static void lazy_tombstone(kv_db *db, struct slot *s) {
    if (s->state != SLOT_OCCUPIED) return;
    s->state = SLOT_TOMB;
    s->value_len = 0;
    msync(s, sizeof(*s), MS_SYNC);
    if (db->hdr->count > 0) db->hdr->count--;
    db->hdr->tombstones++;
    sync_header(db);
}

static int remap(kv_db *db, size_t size) {
    void *map = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, db->fd, 0);
    if (map == MAP_FAILED) return -1;
    db->map      = map;
    db->map_size = size;
    db->hdr      = (struct header *)map;
    db->slots    = (struct slot *)((uint8_t *)map + HEADER_SIZE);
    return 0;
}

int kv_open(const char *path, uint32_t capacity, kv_db **out) {
    if (!path || !out || capacity == 0) return KV_ERROR;

    int fd = open(path, O_RDWR | O_CREAT, 0644);
    if (fd < 0) return KV_ERROR;

    struct stat st;
    if (fstat(fd, &st) != 0) { close(fd); return KV_ERROR; }

    int initializing = (st.st_size == 0);
    size_t want = HEADER_SIZE + (size_t)capacity * sizeof(struct slot);

    if (initializing) {
        if (ftruncate(fd, (off_t)want) != 0) { close(fd); return KV_ERROR; }
    } else if ((size_t)st.st_size < HEADER_SIZE) {
        close(fd);
        return KV_BAD_FILE;
    }

    void *hmap = mmap(NULL, HEADER_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (hmap == MAP_FAILED) { close(fd); return KV_ERROR; }

    struct header *h = (struct header *)hmap;
    if (initializing) {
        memcpy(h->magic, MAGIC, MAGIC_LEN);
        h->version    = FORMAT_VER;
        h->capacity   = capacity;
        h->count      = 0;
        h->tombstones = 0;
        h->crc        = header_crc(h);
        msync(hmap, HEADER_SIZE, MS_SYNC);
    } else {
        if (memcmp(h->magic, MAGIC, MAGIC_LEN) != 0 || h->version != FORMAT_VER ||
            h->crc != header_crc(h)) {
            munmap(hmap, HEADER_SIZE);
            close(fd);
            return KV_BAD_FILE;
        }
        capacity = h->capacity;
        want = HEADER_SIZE + (size_t)capacity * sizeof(struct slot);
        if ((size_t)st.st_size < want) {
            munmap(hmap, HEADER_SIZE);
            close(fd);
            return KV_BAD_FILE;
        }
    }
    munmap(hmap, HEADER_SIZE);

    void *map = mmap(NULL, want, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (map == MAP_FAILED) { close(fd); return KV_ERROR; }

    kv_db *db = (kv_db *)calloc(1, sizeof(*db));
    if (!db) { munmap(map, want); close(fd); return KV_ERROR; }

    db->fd       = fd;
    db->map_size = want;
    db->map      = map;
    db->hdr      = (struct header *)map;
    db->slots    = (struct slot *)((uint8_t *)map + HEADER_SIZE);
    db->clock    = 0;

    *out = db;
    return KV_OK;
}

void kv_close(kv_db *db) {
    if (!db) return;
    msync(db->map, db->map_size, MS_SYNC);
    munmap(db->map, db->map_size);
    close(db->fd);
    free(db);
}

// Probe for a key. On success returns the index. On miss returns capacity if
// `for_insert` is 0, or the index of the first empty/tombstone slot if 1.
static uint32_t probe(kv_db *db,
                      const void *key, uint32_t key_len,
                      int for_insert) {
    uint32_t cap = db->hdr->capacity;
    uint64_t h = fnv1a(key, key_len);
    uint32_t start = (uint32_t)(h % cap);
    uint32_t first_free = cap;

    for (uint32_t i = 0; i < cap; i++) {
        uint32_t idx = (start + i) % cap;
        struct slot *s = &db->slots[idx];
        if (s->state == SLOT_EMPTY) {
            if (for_insert && first_free == cap) first_free = idx;
            return for_insert ? first_free : cap;
        }
        if (s->state == SLOT_TOMB) {
            if (keys_eq(s, key, key_len)) return idx;
            if (for_insert && first_free == cap) first_free = idx;
            continue;
        }
        if (keys_eq(s, key, key_len)) return idx;
    }
    return for_insert ? first_free : cap;
}

int kv_get(kv_db *db,
           const void *key, uint32_t key_len,
           void *out, uint32_t out_cap, uint32_t *out_len,
           uint64_t *out_version,
           uint64_t *out_expires_at) {
    if (!db || !key || key_len == 0 || key_len > KV_KEY_MAX) return KV_ERROR;
    uint32_t idx = probe(db, key, key_len, 0);
    if (idx == db->hdr->capacity) return KV_NOT_FOUND;

    struct slot *s = &db->slots[idx];
    if (slot_expired(db, s)) lazy_tombstone(db, s);

    if (out_version) *out_version = s->version;
    if (out_expires_at) *out_expires_at = s->expires_at;

    if (s->state == SLOT_TOMB) {
        if (out_len) *out_len = 0;
        return KV_DELETED;
    }

    if (out_len) *out_len = s->value_len;
    if (out && out_cap > 0) {
        uint32_t n = s->value_len < out_cap ? s->value_len : out_cap;
        memcpy(out, s->value, n);
    }
    return KV_OK;
}

static struct slot *slot_for_write(kv_db *db, const void *key, uint32_t key_len) {
    uint32_t cap = db->hdr->capacity;
    uint64_t live = (uint64_t)db->hdr->count + db->hdr->tombstones;
    if (live * MAX_LOAD_DEN >= (uint64_t)cap * MAX_LOAD_NUM) {
        if (probe(db, key, key_len, 0) == cap) return NULL;
    }
    uint32_t idx = probe(db, key, key_len, 1);
    if (idx == cap) return NULL;
    return &db->slots[idx];
}

static int grow_once(kv_db *db) {
    uint32_t cap = db->hdr->capacity;
    if (cap > (UINT32_MAX / 2u)) return KV_FULL;
    uint32_t next = cap * 2u;
    if (next < cap) return KV_FULL;
    return kv_rehash(db, next);
}

static int put_once(kv_db *db,
                    const void *key, uint32_t key_len,
                    const void *value, uint32_t value_len,
                    uint64_t version, uint64_t expires_at) {
    struct slot *s = slot_for_write(db, key, key_len);
    if (!s) return KV_FULL;

    int matches = (s->state != SLOT_EMPTY) && keys_eq(s, key, key_len);
    if (matches && s->version > version) {
        return KV_OK;
    }

    int was_live = (s->state == SLOT_OCCUPIED);
    int was_tomb = (s->state == SLOT_TOMB);

    s->key_len    = key_len;
    s->value_len  = value_len;
    s->version    = version;
    s->expires_at = expires_at;
    memcpy(s->key, key, key_len);
    if (value_len) memcpy(s->value, value, value_len);
    s->state = SLOT_OCCUPIED;

    msync(s, sizeof(*s), MS_SYNC);
    if (!was_live) db->hdr->count++;
    if (was_tomb)  db->hdr->tombstones--;
    sync_header(db);
    return KV_OK;
}

int kv_put_ttl(kv_db *db,
               const void *key, uint32_t key_len,
               const void *value, uint32_t value_len,
               uint64_t version,
               uint64_t expires_at) {
    if (!db || !key) return KV_ERROR;
    if (key_len == 0 || key_len > KV_KEY_MAX) return KV_TOO_LARGE;
    if (value_len > KV_VALUE_MAX) return KV_TOO_LARGE;

    int rc = put_once(db, key, key_len, value, value_len, version, expires_at);
    if (rc != KV_FULL) return rc;
    rc = grow_once(db);
    if (rc != KV_OK) return rc;
    return put_once(db, key, key_len, value, value_len, version, expires_at);
}

int kv_put(kv_db *db,
           const void *key, uint32_t key_len,
           const void *value, uint32_t value_len,
           uint64_t version) {
    return kv_put_ttl(db, key, key_len, value, value_len, version, 0);
}

static int plant_tombstone(kv_db *db,
                           const void *key, uint32_t key_len,
                           uint64_t version) {
    struct slot *s = slot_for_write(db, key, key_len);
    if (!s) {
        int rc = grow_once(db);
        if (rc != KV_OK) return rc;
        s = slot_for_write(db, key, key_len);
        if (!s) return KV_FULL;
    }

    int matches = (s->state != SLOT_EMPTY) && keys_eq(s, key, key_len);
    if (matches && s->version > version) return KV_OK;

    int was_live = (s->state == SLOT_OCCUPIED);
    int was_tomb = (s->state == SLOT_TOMB);

    s->key_len    = key_len;
    s->value_len  = 0;
    s->version    = version;
    s->expires_at = 0;
    memcpy(s->key, key, key_len);
    s->state = SLOT_TOMB;
    msync(s, sizeof(*s), MS_SYNC);

    if (was_live) {
        db->hdr->count--;
        db->hdr->tombstones++;
    } else if (!was_tomb) {
        db->hdr->tombstones++;
    }
    sync_header(db);
    return KV_OK;
}

int kv_del(kv_db *db, const void *key, uint32_t key_len, uint64_t version) {
    if (!db || !key || key_len == 0 || key_len > KV_KEY_MAX) return KV_ERROR;
    uint32_t idx = probe(db, key, key_len, 0);
    if (idx == db->hdr->capacity) {
        int rc = plant_tombstone(db, key, key_len, version);
        if (rc != KV_OK) return rc;
        return KV_NOT_FOUND; /* planted, but it was never present */
    }

    struct slot *s = &db->slots[idx];
    if (slot_expired(db, s)) lazy_tombstone(db, s);

    if (s->state == SLOT_TOMB) {
        if (version > s->version) {
            s->version = version;
            msync(s, sizeof(*s), MS_SYNC);
        }
        return KV_OK;
    }
    if (s->state != SLOT_OCCUPIED) return KV_NOT_FOUND;
    if (s->version > version) return KV_OK;

    s->state     = SLOT_TOMB;
    s->version   = version;
    s->value_len = 0;
    msync(s, sizeof(*s), MS_SYNC);
    db->hdr->count--;
    db->hdr->tombstones++;
    sync_header(db);
    return KV_OK;
}

int kv_scan(kv_db *db, kv_scan_cb cb, void *ctx) {
    if (!db || !cb) return KV_ERROR;
    uint32_t cap = db->hdr->capacity;
    for (uint32_t i = 0; i < cap; i++) {
        struct slot *s = &db->slots[i];
        if (s->state == SLOT_OCCUPIED && slot_expired(db, s)) {
            lazy_tombstone(db, s);
        }
        if (s->state != SLOT_OCCUPIED) continue;
        int rc = cb(s->key, s->key_len, s->value, s->value_len,
                    s->version, s->expires_at, ctx);
        if (rc != 0) return rc;
    }
    return KV_OK;
}

int kv_scan_prefix(kv_db *db, const void *prefix, uint32_t plen,
                   kv_scan_cb cb, void *ctx) {
    if (!db || !cb) return KV_ERROR;
    if (plen == 0) return kv_scan(db, cb, ctx);
    if (!prefix) return KV_ERROR;

    uint32_t cap = db->hdr->capacity;
    for (uint32_t i = 0; i < cap; i++) {
        struct slot *s = &db->slots[i];
        if (s->state == SLOT_OCCUPIED && slot_expired(db, s)) {
            lazy_tombstone(db, s);
        }
        if (s->state != SLOT_OCCUPIED) continue;
        if (s->key_len < plen) continue;
        if (memcmp(s->key, prefix, plen) != 0) continue;
        int rc = cb(s->key, s->key_len, s->value, s->value_len,
                    s->version, s->expires_at, ctx);
        if (rc != 0) return rc;
    }
    return KV_OK;
}

int kv_scan_all(kv_db *db, kv_scan_all_cb cb, void *ctx) {
    if (!db || !cb) return KV_ERROR;
    uint32_t cap = db->hdr->capacity;
    for (uint32_t i = 0; i < cap; i++) {
        struct slot *s = &db->slots[i];
        if (s->state == SLOT_EMPTY) continue;
        if (s->state == SLOT_OCCUPIED && slot_expired(db, s)) {
            lazy_tombstone(db, s);
        }
        int tomb = (s->state == SLOT_TOMB);
        int rc = cb(s->key, s->key_len,
                    tomb ? NULL : s->value,
                    tomb ? 0 : s->value_len,
                    s->version, s->expires_at, tomb, ctx);
        if (rc != 0) return rc;
    }
    return KV_OK;
}

static int insert_fresh(kv_db *db, const struct rec *r) {
    uint32_t idx = probe(db, r->key, r->key_len, 1);
    if (idx == db->hdr->capacity) return KV_FULL;
    struct slot *s = &db->slots[idx];
    s->key_len    = r->key_len;
    s->value_len  = r->value_len;
    s->version    = r->version;
    s->expires_at = r->expires_at;
    memcpy(s->key, r->key, r->key_len);
    if (r->value_len) memcpy(s->value, r->value, r->value_len);
    s->state = SLOT_OCCUPIED;
    msync(s, sizeof(*s), MS_SYNC);
    db->hdr->count++;
    return KV_OK;
}

int kv_rehash(kv_db *db, uint32_t new_capacity) {
    if (!db || new_capacity == 0) return KV_ERROR;

    uint32_t cap = db->hdr->capacity;
    uint32_t live = 0;
    for (uint32_t i = 0; i < cap; i++) {
        struct slot *s = &db->slots[i];
        if (s->state == SLOT_OCCUPIED && !slot_expired(db, s)) live++;
    }
    if (live > new_capacity) return KV_FULL;

    struct rec *recs = NULL;
    if (live > 0) {
        recs = (struct rec *)calloc(live, sizeof(*recs));
        if (!recs) return KV_ERROR;
    }

    uint32_t n = 0;
    for (uint32_t i = 0; i < cap; i++) {
        struct slot *s = &db->slots[i];
        if (s->state != SLOT_OCCUPIED || slot_expired(db, s)) continue;
        recs[n].key_len    = s->key_len;
        recs[n].value_len  = s->value_len;
        recs[n].version    = s->version;
        recs[n].expires_at = s->expires_at;
        memcpy(recs[n].key, s->key, s->key_len);
        if (s->value_len) memcpy(recs[n].value, s->value, s->value_len);
        n++;
    }

    size_t old_size = db->map_size;
    msync(db->map, db->map_size, MS_SYNC);
    munmap(db->map, db->map_size);
    db->map = NULL;

    size_t want = HEADER_SIZE + (size_t)new_capacity * sizeof(struct slot);
    if (ftruncate(db->fd, (off_t)want) != 0) {
        (void)remap(db, old_size);
        free(recs);
        return KV_ERROR;
    }
    if (remap(db, want) != 0) {
        (void)ftruncate(db->fd, (off_t)old_size);
        (void)remap(db, old_size);
        free(recs);
        return KV_ERROR;
    }

    memset(db->map, 0, want);
    memcpy(db->hdr->magic, MAGIC, MAGIC_LEN);
    db->hdr->version    = FORMAT_VER;
    db->hdr->capacity   = new_capacity;
    db->hdr->count      = 0;
    db->hdr->tombstones = 0;

    int rc = KV_OK;
    for (uint32_t i = 0; i < n; i++) {
        if (insert_fresh(db, &recs[i]) != KV_OK) {
            rc = KV_ERROR;
            break;
        }
    }
    sync_header(db);
    free(recs);
    return rc;
}

int kv_compact(kv_db *db) {
    if (!db) return KV_ERROR;
    return kv_rehash(db, db->hdr->capacity);
}

uint32_t kv_count(const kv_db *db)      { return db ? db->hdr->count      : 0; }
uint32_t kv_capacity(const kv_db *db)   { return db ? db->hdr->capacity   : 0; }
uint32_t kv_tombstones(const kv_db *db) { return db ? db->hdr->tombstones : 0; }

uint64_t kv_max_version(const kv_db *db) {
    if (!db) return 0;
    uint64_t max = 0;
    uint32_t cap = db->hdr->capacity;
    for (uint32_t i = 0; i < cap; i++) {
        const struct slot *s = &db->slots[i];
        if (s->state != SLOT_EMPTY && s->version > max) max = s->version;
    }
    return max;
}
