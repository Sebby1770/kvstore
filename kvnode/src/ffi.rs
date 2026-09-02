// Bindings for the C storage engine. The C side is single-threaded — wrap
// the handle in a Mutex before sharing across threads.

use std::ffi::CString;
use std::os::raw::{c_int, c_void};
use std::path::Path;
use std::ptr;

#[allow(non_camel_case_types)]
pub enum kv_db {}

pub const KV_OK: c_int = 0;
pub const KV_NOT_FOUND: c_int = -1;
pub const KV_FULL: c_int = -2;
pub const KV_TOO_LARGE: c_int = -4;
pub const KV_DELETED: c_int = -6;

#[allow(dead_code)] pub const KV_KEY_MAX: u32 = 64;
pub const KV_VALUE_MAX: u32 = 512;

pub type ScanCb = extern "C" fn(*const u8, u32, *const u8, u32, u64, u64, *mut c_void) -> c_int;
pub type ScanAllCb = extern "C" fn(*const u8, u32, *const u8, u32, u64, u64, c_int, *mut c_void) -> c_int;

extern "C" {
    fn kv_open(path: *const i8, capacity: u32, out: *mut *mut kv_db) -> c_int;
    fn kv_close(db: *mut kv_db);
    fn kv_get(
        db: *mut kv_db,
        key: *const u8, key_len: u32,
        out: *mut u8, out_cap: u32, out_len: *mut u32,
        out_version: *mut u64,
        out_expires_at: *mut u64,
    ) -> c_int;
    fn kv_put_ttl(
        db: *mut kv_db,
        key: *const u8, key_len: u32,
        val: *const u8, val_len: u32,
        version: u64,
        expires_at: u64,
    ) -> c_int;
    fn kv_del(db: *mut kv_db, key: *const u8, key_len: u32, version: u64) -> c_int;
    fn kv_scan(db: *mut kv_db, cb: ScanCb, ctx: *mut c_void) -> c_int;
    fn kv_scan_all(db: *mut kv_db, cb: ScanAllCb, ctx: *mut c_void) -> c_int;
    fn kv_compact(db: *mut kv_db) -> c_int;
    fn kv_count(db: *const kv_db) -> u32;
    fn kv_capacity(db: *const kv_db) -> u32;
    fn kv_tombstones(db: *const kv_db) -> u32;
    fn kv_max_version(db: *const kv_db) -> u64;
}

pub struct Db {
    raw: *mut kv_db,
}

// Safe to send across threads as long as access goes through &mut self.
// All public methods take &mut self, so a Mutex<Db> serializes correctly.
unsafe impl Send for Db {}

#[derive(Debug)]
#[allow(dead_code)] // payloads are printed via Debug
pub enum DbErr {
    Open(c_int),
    NotFound,
    Full,
    TooLarge,
    Other(c_int),
}

/// Result of a versioned read: a value, a tombstone, or absence (Err::NotFound).
pub enum Got {
    Value { value: Vec<u8>, version: u64, expires_at: u64 },
    Deleted { version: u64, expires_at: u64 },
}

pub struct ScanRow {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub version: u64,
    pub expires_at: u64,
    pub tombstone: bool,
}

fn check(rc: c_int) -> Result<(), DbErr> {
    match rc {
        KV_OK => Ok(()),
        KV_NOT_FOUND => Err(DbErr::NotFound),
        KV_FULL => Err(DbErr::Full),
        KV_TOO_LARGE => Err(DbErr::TooLarge),
        other => Err(DbErr::Other(other)),
    }
}

impl Db {
    pub fn open(path: &Path, capacity: u32) -> Result<Self, DbErr> {
        let cstr = CString::new(path.to_string_lossy().as_bytes()).map_err(|_| DbErr::Other(0))?;
        let mut raw: *mut kv_db = ptr::null_mut();
        let rc = unsafe { kv_open(cstr.as_ptr(), capacity, &mut raw) };
        if rc != KV_OK {
            return Err(DbErr::Open(rc));
        }
        Ok(Db { raw })
    }

    pub fn get(&mut self, key: &[u8]) -> Result<Got, DbErr> {
        let mut buf = vec![0u8; KV_VALUE_MAX as usize];
        let mut out_len: u32 = 0;
        let mut version: u64 = 0;
        let mut expires_at: u64 = 0;
        let rc = unsafe {
            kv_get(
                self.raw,
                key.as_ptr(), key.len() as u32,
                buf.as_mut_ptr(), buf.len() as u32, &mut out_len,
                &mut version,
                &mut expires_at,
            )
        };
        match rc {
            KV_OK => {
                buf.truncate(out_len as usize);
                Ok(Got::Value { value: buf, version, expires_at })
            }
            KV_DELETED => Ok(Got::Deleted { version, expires_at }),
            KV_NOT_FOUND => Err(DbErr::NotFound),
            other => Err(DbErr::Other(other)),
        }
    }

    pub fn put(&mut self, key: &[u8], val: &[u8], version: u64) -> Result<(), DbErr> {
        self.put_ttl(key, val, version, 0)
    }

    pub fn put_ttl(&mut self, key: &[u8], val: &[u8], version: u64, expires_at: u64) -> Result<(), DbErr> {
        let rc = unsafe {
            kv_put_ttl(
                self.raw,
                key.as_ptr(), key.len() as u32,
                val.as_ptr(), val.len() as u32,
                version,
                expires_at,
            )
        };
        check(rc)
    }

    pub fn del(&mut self, key: &[u8], version: u64) -> Result<(), DbErr> {
        let rc = unsafe { kv_del(self.raw, key.as_ptr(), key.len() as u32, version) };
        check(rc)
    }

    pub fn scan(&mut self) -> Vec<ScanRow> {
        let mut out: Vec<ScanRow> = Vec::new();
        extern "C" fn cb(
            k: *const u8, klen: u32,
            v: *const u8, vlen: u32,
            version: u64,
            expires_at: u64,
            ctx: *mut c_void,
        ) -> c_int {
            let out = unsafe { &mut *(ctx as *mut Vec<ScanRow>) };
            let key = unsafe { std::slice::from_raw_parts(k, klen as usize) }.to_vec();
            let val = unsafe { std::slice::from_raw_parts(v, vlen as usize) }.to_vec();
            out.push(ScanRow { key, value: val, version, expires_at, tombstone: false });
            0
        }
        let ctx = &mut out as *mut _ as *mut c_void;
        unsafe { kv_scan(self.raw, cb, ctx); }
        out
    }

    pub fn scan_all(&mut self) -> Vec<ScanRow> {
        let mut out: Vec<ScanRow> = Vec::new();
        extern "C" fn cb(
            k: *const u8, klen: u32,
            v: *const u8, vlen: u32,
            version: u64,
            expires_at: u64,
            tombstone: c_int,
            ctx: *mut c_void,
        ) -> c_int {
            let out = unsafe { &mut *(ctx as *mut Vec<ScanRow>) };
            let key = unsafe { std::slice::from_raw_parts(k, klen as usize) }.to_vec();
            let val = if v.is_null() || vlen == 0 {
                Vec::new()
            } else {
                unsafe { std::slice::from_raw_parts(v, vlen as usize) }.to_vec()
            };
            out.push(ScanRow {
                key,
                value: val,
                version,
                expires_at,
                tombstone: tombstone != 0,
            });
            0
        }
        let ctx = &mut out as *mut _ as *mut c_void;
        unsafe { kv_scan_all(self.raw, cb, ctx); }
        out
    }

    pub fn compact(&mut self) -> Result<(), DbErr> {
        check(unsafe { kv_compact(self.raw) })
    }

    pub fn count(&self) -> u32        { unsafe { kv_count(self.raw) } }
    pub fn capacity(&self) -> u32     { unsafe { kv_capacity(self.raw) } }
    pub fn tombstones(&self) -> u32   { unsafe { kv_tombstones(self.raw) } }
    pub fn max_version(&self) -> u64  { unsafe { kv_max_version(self.raw) } }
}

impl Drop for Db {
    fn drop(&mut self) {
        if !self.raw.is_null() {
            unsafe { kv_close(self.raw); }
        }
    }
}
