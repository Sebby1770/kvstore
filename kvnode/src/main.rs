// kvnode — accepts TCP connections from the Go gateway and dispatches into
// the C storage engine. Thread-per-connection with a single mutex guarding
// the (non-thread-safe) C handle. Honest and easy to reason about.
//
// The node is deliberately dumb: it stores whatever version the gateway tells
// it (the engine enforces last-writer-wins and native TTL) and reports versions
// back. All the distributed-systems policy — replication, quorum, read-repair,
// hinted handoff — lives in the gateway, not here.

mod ffi;
mod proto;

use std::io::{self, BufReader, BufWriter, Write};
use std::net::TcpListener;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::thread;

use ffi::{Db, DbErr, Got};
use proto::*;

struct Args {
    listen:   String,
    db_path:  PathBuf,
    capacity: u32,
}

fn parse_args() -> Result<Args, String> {
    let mut listen   = "127.0.0.1:7070".to_string();
    let mut db_path  = PathBuf::from("kvnode.db");
    let mut capacity = 1024u32;

    let mut iter = std::env::args().skip(1);
    while let Some(a) = iter.next() {
        match a.as_str() {
            "--listen"   => listen   = iter.next().ok_or("--listen needs a value")?,
            "--db"       => db_path  = PathBuf::from(iter.next().ok_or("--db needs a value")?),
            "--capacity" => capacity = iter.next().ok_or("--capacity needs a value")?
                .parse().map_err(|_| "--capacity must be u32")?,
            "-h" | "--help" => {
                eprintln!("kvnode --listen 127.0.0.1:7070 --db kvnode.db --capacity 1024");
                std::process::exit(0);
            }
            other => return Err(format!("unknown arg: {other}")),
        }
    }
    Ok(Args { listen, db_path, capacity })
}

fn main() {
    let args = match parse_args() {
        Ok(a) => a,
        Err(e) => { eprintln!("kvnode: {e}"); std::process::exit(2); }
    };

    let db = match Db::open(&args.db_path, args.capacity) {
        Ok(d) => d,
        Err(e) => { eprintln!("kvnode: open {:?}: {:?}", args.db_path, e); std::process::exit(1); }
    };
    eprintln!(
        "kvnode: db {} (count={}, capacity={}, tombstones={}, max_version={})",
        args.db_path.display(), db.count(), db.capacity(), db.tombstones(), db.max_version()
    );

    let db = Arc::new(Mutex::new(db));
    let listener = TcpListener::bind(&args.listen)
        .unwrap_or_else(|e| { eprintln!("kvnode: bind {}: {e}", args.listen); std::process::exit(1); });
    eprintln!("kvnode: listening on {}", args.listen);

    for stream in listener.incoming() {
        let stream = match stream {
            Ok(s) => s,
            Err(e) => { eprintln!("kvnode: accept: {e}"); continue; }
        };
        let db = Arc::clone(&db);
        thread::spawn(move || {
            if let Err(e) = handle(stream, db) {
                if e.kind() != io::ErrorKind::UnexpectedEof {
                    eprintln!("kvnode: conn: {e}");
                }
            }
        });
    }
}

fn write_scan_row<W: Write>(w: &mut W, row: &ffi::ScanRow, with_tomb: bool) -> io::Result<()> {
    write_u32(w, row.key.len() as u32)?;
    write_bytes(w, &row.key)?;
    write_u64(w, row.version)?;
    write_u64(w, row.expires_at)?;
    if with_tomb {
        write_u8(w, if row.tombstone { 1 } else { 0 })?;
    }
    write_u32(w, row.value.len() as u32)?;
    write_bytes(w, &row.value)
}

fn handle(stream: std::net::TcpStream, db: Arc<Mutex<Db>>) -> io::Result<()> {
    stream.set_nodelay(true).ok();
    let mut r = BufReader::new(stream.try_clone()?);
    let mut w = BufWriter::new(stream);

    loop {
        let op = match read_u8(&mut r) {
            Ok(o) => o,
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(()),
            Err(e) => return Err(e),
        };

        match op {
            OP_PING => {
                write_u8(&mut w, STATUS_OK)?;
                w.flush()?;
            }
            OP_STAT => {
                let (count, cap, maxv, tombs) = {
                    let db = db.lock().unwrap();
                    (db.count(), db.capacity(), db.max_version(), db.tombstones())
                };
                write_u8(&mut w, STATUS_OK)?;
                write_u32(&mut w, count)?;
                write_u32(&mut w, cap)?;
                write_u64(&mut w, maxv)?;
                write_u32(&mut w, tombs)?;
                w.flush()?;
            }
            OP_GET => {
                let klen = read_u32(&mut r)?;
                let key  = read_limited_bytes(&mut r, klen, KEY_MAX, "key")?;
                let res  = { db.lock().unwrap().get(&key) };
                match res {
                    Ok(Got::Value { value, version, expires_at }) => {
                        write_u8(&mut w, STATUS_OK)?;
                        write_u64(&mut w, version)?;
                        write_u64(&mut w, expires_at)?;
                        write_u32(&mut w, value.len() as u32)?;
                        write_bytes(&mut w, &value)?;
                    }
                    Ok(Got::Deleted { version, expires_at }) => {
                        write_u8(&mut w, STATUS_DELETED)?;
                        write_u64(&mut w, version)?;
                        write_u64(&mut w, expires_at)?;
                    }
                    Err(DbErr::NotFound) => { write_u8(&mut w, STATUS_NOT_FOUND)?; }
                    Err(_)               => { write_u8(&mut w, STATUS_ERROR)?; }
                }
                w.flush()?;
            }
            OP_PUT => {
                let klen    = read_u32(&mut r)?;
                let key     = read_limited_bytes(&mut r, klen, KEY_MAX, "key")?;
                let version = read_u64(&mut r)?;
                let vlen    = read_u32(&mut r)?;
                let val     = read_limited_bytes(&mut r, vlen, VALUE_MAX, "value")?;
                let res     = { db.lock().unwrap().put(&key, &val, version) };
                write_u8(&mut w, if res.is_ok() { STATUS_OK } else { STATUS_ERROR })?;
                w.flush()?;
            }
            OP_PUTX => {
                let klen       = read_u32(&mut r)?;
                let key        = read_limited_bytes(&mut r, klen, KEY_MAX, "key")?;
                let version    = read_u64(&mut r)?;
                let expires_at = read_u64(&mut r)?;
                let vlen       = read_u32(&mut r)?;
                let val        = read_limited_bytes(&mut r, vlen, VALUE_MAX, "value")?;
                let res        = { db.lock().unwrap().put_ttl(&key, &val, version, expires_at) };
                write_u8(&mut w, if res.is_ok() { STATUS_OK } else { STATUS_ERROR })?;
                w.flush()?;
            }
            OP_DEL => {
                let klen    = read_u32(&mut r)?;
                let key     = read_limited_bytes(&mut r, klen, KEY_MAX, "key")?;
                let version = read_u64(&mut r)?;
                let res     = { db.lock().unwrap().del(&key, version) };
                let s = match res {
                    Ok(())               => STATUS_OK,
                    Err(DbErr::NotFound) => STATUS_NOT_FOUND,
                    Err(_)               => STATUS_ERROR,
                };
                write_u8(&mut w, s)?;
                w.flush()?;
            }
            OP_SCAN => {
                let entries = { db.lock().unwrap().scan() };
                write_u8(&mut w, STATUS_OK)?;
                for row in &entries {
                    write_scan_row(&mut w, row, false)?;
                }
                write_u32(&mut w, SCAN_TERMINATOR)?;
                w.flush()?;
            }
            OP_SCAN_ALL => {
                let entries = { db.lock().unwrap().scan_all() };
                write_u8(&mut w, STATUS_OK)?;
                for row in &entries {
                    write_scan_row(&mut w, row, true)?;
                }
                write_u32(&mut w, SCAN_TERMINATOR)?;
                w.flush()?;
            }
            OP_COMPACT => {
                let res = { db.lock().unwrap().compact() };
                write_u8(&mut w, if res.is_ok() { STATUS_OK } else { STATUS_ERROR })?;
                w.flush()?;
            }
            _ => {
                write_u8(&mut w, STATUS_ERROR)?;
                w.flush()?;
                return Ok(());
            }
        }
    }
}
