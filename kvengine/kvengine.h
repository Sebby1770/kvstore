#ifndef KVENGINE_H
#define KVENGINE_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define KV_OK         0
#define KV_NOT_FOUND -1
#define KV_FULL      -2
#define KV_ERROR     -3
#define KV_TOO_LARGE -4
#define KV_BAD_FILE  -5
#define KV_DELETED   -6   /* key has a tombstone; *out_version is still valid */

#define KV_KEY_MAX   64u
#define KV_VALUE_MAX 512u

typedef struct kv_db kv_db;

int      kv_open(const char *path, uint32_t capacity, kv_db **out);
void     kv_close(kv_db *db);

/* Inject a clock for tests. unix_sec == 0 means use time(NULL). */
void     kv_set_time(kv_db *db, uint64_t unix_sec);

/* Fetch the value, version, and optional expiry (unix seconds, 0 = never).
 *   KV_OK        -> *out_version + value written; *out_expires_at set
 *   KV_DELETED   -> *out_version set (tombstone), *out_len = 0
 *   KV_NOT_FOUND -> key never existed here
 *
 * Occupied slots with expires_at != 0 and now >= expires_at are lazily
 * tombstoned and reported as KV_DELETED.
 */
int      kv_get(kv_db *db,
                const void *key, uint32_t key_len,
                void *out, uint32_t out_cap, uint32_t *out_len,
                uint64_t *out_version,
                uint64_t *out_expires_at);

/* Store (key, value) at the given version with expires_at = 0 (never).
 * Last-writer-wins: applied only if `version` is >= the version already
 * stored, so puts are idempotent and safe to replay. Returns KV_OK even
 * when the write is a no-op because a newer version already won.
 *
 * If the table is at load, capacity is doubled via kv_rehash and the
 * insert is retried once.
 */
int      kv_put(kv_db *db,
                const void *key, uint32_t key_len,
                const void *value, uint32_t value_len,
                uint64_t version);

/* Like kv_put, with a native expiry (unix seconds, 0 = never). */
int      kv_put_ttl(kv_db *db,
                    const void *key, uint32_t key_len,
                    const void *value, uint32_t value_len,
                    uint64_t version,
                    uint64_t expires_at);

/* Tombstone a key at the given version (also last-writer-wins). A delete
 * of a key that was never present still plants a tombstone so replicas
 * can replicate deletes. */
int      kv_del(kv_db *db, const void *key, uint32_t key_len, uint64_t version);

typedef int (*kv_scan_cb)(const void *key, uint32_t key_len,
                          const void *value, uint32_t value_len,
                          uint64_t version, uint64_t expires_at, void *ctx);

/* Visit live (non-tombstoned, non-expired) entries only. Expired occupied
 * slots are lazily tombstoned and skipped. */
int      kv_scan(kv_db *db, kv_scan_cb cb, void *ctx);

typedef int (*kv_scan_all_cb)(const void *key, uint32_t key_len,
                              const void *value, uint32_t value_len,
                              uint64_t version, uint64_t expires_at,
                              int tombstone, void *ctx);

/* Visit every non-empty slot, including tombstones. Expired occupied slots
 * are lazily tombstoned, then reported as tombstones. */
int      kv_scan_all(kv_db *db, kv_scan_all_cb cb, void *ctx);

/* Remap the table to new_capacity. Live non-expired keys are copied;
 * tombstones and expired keys are dropped (GC). */
int      kv_rehash(kv_db *db, uint32_t new_capacity);

/* GC without grow: rehash at the current capacity so tombstones and
 * expired keys are dropped. Returns KV_OK or KV_ERROR. */
int      kv_compact(kv_db *db);

uint32_t kv_count(const kv_db *db);
uint32_t kv_capacity(const kv_db *db);
uint32_t kv_tombstones(const kv_db *db);
uint64_t kv_max_version(const kv_db *db);

#ifdef __cplusplus
}
#endif

#endif
