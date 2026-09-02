// Wire protocol between the Rust node and Go gateway. Length-prefixed,
// big-endian. Designed to be obvious to read with `tcpdump -X`.
//
// Every stored value carries an 8-byte version so the gateway can do
// last-writer-wins conflict resolution across replicas. Native TTL is an
// 8-byte expires_at (unix seconds, 0 = never) on PUTX and GET/SCAN replies.
//
// Request:
//   GET      : 1B op | 4B key_len | key
//   PUT      : 1B op | 4B key_len | key | 8B version | 4B val_len | val
//              (expires_at = 0; kept for compatibility)
//   DEL      : 1B op | 4B key_len | key | 8B version
//   SCAN     : 1B op
//   PING     : 1B op
//   STAT     : 1B op
//   PUTX     : 1B op | 4B key_len | key | 8B version | 8B expires_at | 4B val_len | val
//   SCAN_ALL : 1B op   (includes tombstones)
//   COMPACT  : 1B op   (GC tombstones/expired keys at current capacity)
//
// Response:
//   1B status (0=OK, 1=NOT_FOUND, 2=ERROR, 3=DELETED)
//   GET OK:      + 8B version + 8B expires_at + 4B val_len + val
//   GET DELETED: + 8B version + 8B expires_at
//   SCAN OK:     + repeated (4B klen + key + 8B version + 8B expires_at + 4B vlen + val),
//                  terminated by klen = 0xFFFFFFFF
//   SCAN_ALL OK: + repeated (4B klen + key + 8B version + 8B expires_at + 1B tomb + 4B vlen + val),
//                  terminated by klen = 0xFFFFFFFF
//   STAT OK:     + 4B count + 4B capacity + 8B max_version + 4B tombstones
//   PUT/DEL:     no body

use std::io::{self, Read, Write};

pub const OP_GET:      u8 = 1;
pub const OP_PUT:      u8 = 2;
pub const OP_DEL:      u8 = 3;
pub const OP_SCAN:     u8 = 4;
pub const OP_PING:     u8 = 5;
pub const OP_STAT:     u8 = 6;
pub const OP_PUTX:     u8 = 7;
pub const OP_SCAN_ALL: u8 = 8;
pub const OP_COMPACT:  u8 = 9;

pub const STATUS_OK:        u8 = 0;
pub const STATUS_NOT_FOUND: u8 = 1;
pub const STATUS_ERROR:     u8 = 2;
pub const STATUS_DELETED:   u8 = 3;

pub const SCAN_TERMINATOR: u32 = 0xFFFF_FFFF;
pub const KEY_MAX: usize = 64;
pub const VALUE_MAX: usize = 512;

pub fn read_u8<R: Read>(r: &mut R) -> io::Result<u8> {
    let mut b = [0u8; 1];
    r.read_exact(&mut b)?;
    Ok(b[0])
}

pub fn read_u32<R: Read>(r: &mut R) -> io::Result<u32> {
    let mut b = [0u8; 4];
    r.read_exact(&mut b)?;
    Ok(u32::from_be_bytes(b))
}

pub fn read_u64<R: Read>(r: &mut R) -> io::Result<u64> {
    let mut b = [0u8; 8];
    r.read_exact(&mut b)?;
    Ok(u64::from_be_bytes(b))
}

pub fn read_bytes<R: Read>(r: &mut R, n: usize) -> io::Result<Vec<u8>> {
    let mut buf = vec![0u8; n];
    r.read_exact(&mut buf)?;
    Ok(buf)
}

pub fn read_limited_bytes<R: Read>(
    r: &mut R,
    n: u32,
    max: usize,
    label: &str,
) -> io::Result<Vec<u8>> {
    let n = n as usize;
    if n > max {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{label} length {n} exceeds limit {max}"),
        ));
    }
    read_bytes(r, n)
}

pub fn write_u8<W: Write>(w: &mut W, v: u8) -> io::Result<()> {
    w.write_all(&[v])
}

pub fn write_u32<W: Write>(w: &mut W, v: u32) -> io::Result<()> {
    w.write_all(&v.to_be_bytes())
}

pub fn write_u64<W: Write>(w: &mut W, v: u64) -> io::Result<()> {
    w.write_all(&v.to_be_bytes())
}

pub fn write_bytes<W: Write>(w: &mut W, b: &[u8]) -> io::Result<()> {
    w.write_all(b)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn read_limited_bytes_accepts_values_within_limit() {
        let mut input = Cursor::new(b"hello".to_vec());

        let out = read_limited_bytes(&mut input, 5, 5, "key").unwrap();

        assert_eq!(out, b"hello");
    }

    #[test]
    fn read_limited_bytes_rejects_oversized_lengths_before_allocation() {
        let mut input = Cursor::new(Vec::<u8>::new());

        let err = read_limited_bytes(&mut input, 65, KEY_MAX, "key").unwrap_err();

        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("key length"));
    }

    #[test]
    fn u64_version_roundtrips_big_endian() {
        let mut buf = Vec::new();
        write_u64(&mut buf, 0x0102_0304_0506_0708).unwrap();
        assert_eq!(buf, vec![1, 2, 3, 4, 5, 6, 7, 8]);

        let mut cur = Cursor::new(buf);
        assert_eq!(read_u64(&mut cur).unwrap(), 0x0102_0304_0506_0708);
    }
}
