// kvnode — the storage tier: serves the Go coordinators' requests from the C
// storage engine. Thread-per-connection with a single mutex guarding the
// (non-thread-safe) C handle. Honest and easy to reason about.
//
// Two transports carry the same request protocol (proto.rs):
//   --ktp ADDR     KTP over UDP: encrypted (AES-256-GCM), authenticated
//                  (Ed25519 identities, keys bound to the cluster secret) and
//                  loss-tolerant; every stream a coordinator opens is served
//                  exactly like a TCP connection. This is the mode to use.
//   --listen ADDR  plain TCP, for localhost setups and old coordinators.
//
// The node is deliberately dumb: it stores whatever version the coordinator
// tells it (the engine enforces last-writer-wins and native TTL) and reports
// versions back. All the distributed-systems policy — replication, quorum,
// read-repair, hinted handoff — lives in the coordinators, not here.

mod ffi;
mod ktp;
mod proto;

use std::fs::{self, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;

use ed25519_dalek::SigningKey;

use ffi::{Db, DbErr, Got};
use proto::*;

/// Concurrent KTP streams served; further streams are refused at once.
const MAX_KTP_HANDLERS: usize = 1024;

const USAGE: &str = "usage: kvnode [--ktp ADDR] [--listen ADDR] [--db FILE] [--capacity N]
              [--identity FILE] [--cluster-key KEY] [--allow-peer HEX]...

  --ktp ADDR         serve KTP (encrypted, authenticated UDP) on ADDR, e.g. 127.0.0.1:7170
  --listen ADDR      serve plain TCP on ADDR (default 127.0.0.1:7070 when --ktp is not given)
  --db FILE          database file (default kvnode.db)
  --capacity N       slots in the engine's table (default 1024)
  --identity FILE    Ed25519 identity seed (hex); created if missing (default: a fresh key per run)
  --cluster-key KEY  cluster secret shared with the coordinators
                     (default $KVNODE_CLUSTER_KEY, then $KVMESH_CLUSTER_KEY)
  --allow-peer HEX   accept only coordinators with this Ed25519 public key (repeatable)";

struct Args {
    listen:      Option<String>,
    ktp:         Option<String>,
    db_path:     PathBuf,
    capacity:    u32,
    identity:    Option<PathBuf>,
    cluster_key: String,
    allow_peers: Vec<[u8; 32]>,
}

fn parse_hex32(s: &str) -> Option<[u8; 32]> {
    let s = s.trim();
    if s.len() != 64 || !s.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    let mut out = [0u8; 32];
    for (i, b) in out.iter_mut().enumerate() {
        *b = u8::from_str_radix(&s[2 * i..2 * i + 2], 16).ok()?;
    }
    Some(out)
}

fn to_hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn parse_args() -> Result<Args, String> {
    let mut listen   = None;
    let mut ktp      = None;
    let mut db_path  = PathBuf::from("kvnode.db");
    let mut capacity = 1024u32;
    let mut identity = None;
    let mut cluster_key = std::env::var("KVNODE_CLUSTER_KEY")
        .or_else(|_| std::env::var("KVMESH_CLUSTER_KEY"))
        .unwrap_or_default();
    let mut allow_peers = Vec::new();

    let mut iter = std::env::args().skip(1);
    while let Some(a) = iter.next() {
        match a.as_str() {
            "--listen"   => listen   = Some(iter.next().ok_or("--listen needs a value")?),
            "--ktp"      => ktp      = Some(iter.next().ok_or("--ktp needs a value")?),
            "--db"       => db_path  = PathBuf::from(iter.next().ok_or("--db needs a value")?),
            "--capacity" => capacity = iter.next().ok_or("--capacity needs a value")?
                .parse().map_err(|_| "--capacity must be u32")?,
            "--identity" => identity = Some(PathBuf::from(iter.next().ok_or("--identity needs a value")?)),
            "--cluster-key" => cluster_key = iter.next().ok_or("--cluster-key needs a value")?,
            "--allow-peer" => {
                let v = iter.next().ok_or("--allow-peer needs a value")?;
                allow_peers.push(parse_hex32(&v).ok_or("--allow-peer takes a 64-hex-digit Ed25519 public key")?);
            }
            "-h" | "--help" => {
                eprintln!("{USAGE}");
                std::process::exit(0);
            }
            other => return Err(format!("unknown arg: {other}\n{USAGE}")),
        }
    }
    if listen.is_none() && ktp.is_none() {
        listen = Some("127.0.0.1:7070".to_string());
    }
    Ok(Args { listen, ktp, db_path, capacity, identity, cluster_key, allow_peers })
}

/// Loads the identity seed at path, creating it (mode 0600) if missing.
fn load_identity(path: &Path) -> io::Result<SigningKey> {
    match fs::read_to_string(path) {
        Ok(text) => parse_hex32(&text)
            .map(|seed| SigningKey::from_bytes(&seed))
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "identity file must hold a 64-hex-digit seed")),
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            let key = ktp::crypto::generate_identity();
            let mut opts = OpenOptions::new();
            opts.write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                opts.mode(0o600);
            }
            let mut f = opts.open(path)?;
            writeln!(f, "{}", to_hex(&key.to_bytes()))?;
            Ok(key)
        }
        Err(e) => Err(e),
    }
}

fn die(msg: String) -> ! {
    eprintln!("kvnode: {msg}");
    std::process::exit(1);
}

fn main() {
    let args = match parse_args() {
        Ok(a) => a,
        Err(e) => { eprintln!("kvnode: {e}"); std::process::exit(2); }
    };

    let db = match Db::open(&args.db_path, args.capacity) {
        Ok(d) => d,
        Err(e) => die(format!("open {:?}: {:?}", args.db_path, e)),
    };
    eprintln!(
        "kvnode: db {} (count={}, capacity={}, tombstones={}, max_version={})",
        args.db_path.display(), db.count(), db.capacity(), db.tombstones(), db.max_version()
    );
    let db = Arc::new(Mutex::new(db));
    let mut servers = Vec::new();

    if let Some(addr) = &args.ktp {
        let identity = match &args.identity {
            Some(p) => load_identity(p).unwrap_or_else(|e| die(format!("identity {}: {e}", p.display()))),
            None => ktp::crypto::generate_identity(),
        };
        let public = identity.verifying_key().to_bytes();
        let mut cfg = ktp::Config::new(identity, args.cluster_key.clone().into_bytes());
        if !args.allow_peers.is_empty() {
            cfg.allowed_peers = Some(args.allow_peers.clone());
        }
        let ep = ktp::Endpoint::bind(addr, cfg, true).unwrap_or_else(|e| die(format!("bind ktp {addr}: {e}")));
        eprintln!(
            "kvnode: ktp on {} (identity {}, fingerprint {})",
            ep.local_addr().map(|a| a.to_string()).unwrap_or_else(|_| addr.clone()),
            to_hex(&public),
            ktp::fingerprint(&public)
        );
        if args.cluster_key.is_empty() {
            eprintln!("kvnode: warning: no --cluster-key; any KTP client can connect (traffic is still encrypted)");
        }
        let db = Arc::clone(&db);
        servers.push(thread::spawn(move || serve_ktp(ep, db)));
    }

    if let Some(addr) = &args.listen {
        let listener = TcpListener::bind(addr).unwrap_or_else(|e| die(format!("bind {addr}: {e}")));
        eprintln!("kvnode: tcp on {addr} (plaintext)");
        let db = Arc::clone(&db);
        servers.push(thread::spawn(move || serve_tcp(listener, db)));
    }

    for s in servers {
        let _ = s.join();
    }
}

fn serve_tcp(listener: TcpListener, db: Arc<Mutex<Db>>) {
    for stream in listener.incoming() {
        let stream = match stream {
            Ok(s) => s,
            Err(e) => { eprintln!("kvnode: accept: {e}"); continue; }
        };
        let db = Arc::clone(&db);
        thread::spawn(move || {
            stream.set_nodelay(true).ok();
            let result = stream.try_clone().and_then(|r| handle(r, stream, &db));
            if let Err(e) = result {
                if e.kind() != io::ErrorKind::UnexpectedEof {
                    eprintln!("kvnode: tcp conn: {e}");
                }
            }
        });
    }
}

fn serve_ktp(ep: ktp::Endpoint, db: Arc<Mutex<Db>>) {
    let active = Arc::new(AtomicUsize::new(0));
    loop {
        let stream = match ep.accept() {
            Ok(s) => s,
            Err(e) => { eprintln!("kvnode: ktp accept: {e}"); return; }
        };
        if active.fetch_add(1, Ordering::SeqCst) >= MAX_KTP_HANDLERS {
            active.fetch_sub(1, Ordering::SeqCst);
            eprintln!("kvnode: ktp: refusing a stream from {}: {MAX_KTP_HANDLERS} already open", stream.peer_addr());
            continue; // dropping the stream finishes it at once
        }
        let db = Arc::clone(&db);
        let active = Arc::clone(&active);
        thread::spawn(move || {
            let peer = stream.peer_addr();
            let result = stream.try_clone().and_then(|r| handle(r, stream, &db));
            active.fetch_sub(1, Ordering::SeqCst);
            match result {
                Err(e) if e.kind() != io::ErrorKind::UnexpectedEof
                    && e.kind() != io::ErrorKind::ConnectionAborted => eprintln!("kvnode: ktp stream from {peer}: {e}"),
                _ => {}
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

/// Serves requests until the peer closes its side. r and w are the two
/// directions of one TCP connection or KTP stream.
fn handle<R: Read, W: Write>(r: R, w: W, db: &Mutex<Db>) -> io::Result<()> {
    let mut r = BufReader::new(r);
    let mut w = BufWriter::new(w);

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
