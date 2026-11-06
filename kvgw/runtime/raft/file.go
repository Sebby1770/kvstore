package raft

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/Sebby1770/kvstore/kvgw/netstack/wire"
)

// FileStorage is durable Storage: a write-ahead log of entries and hard
// states plus a snapshot file, mirrored in a MemoryStorage for reads.
//
// WAL records are [4B length][4B CRC32C][1B type][payload]. Save appends
// records and fsyncs once. On open the snapshot is loaded, then the WAL is
// replayed record by record; the first torn or corrupt record (a crash mid
// write, a flipped bit) ends the replay and the file is truncated there, so
// garbage is never mistaken for log entries -- and since a Ready's records
// are fsynced before its messages are sent, nothing that was acknowledged to
// a peer can be in the lost tail.
type FileStorage struct {
	*MemoryStorage
	dir  string
	mu   sync.Mutex
	wal  *os.File
	info RecoveryInfo
}

// RecoveryInfo reports what opening the storage found.
type RecoveryInfo struct {
	Records        int
	TruncatedBytes int64
	SnapshotIndex  uint64
}

const (
	recEntry     = 1
	recHardState = 2
	recSnapMark  = 3

	walName  = "raft.wal"
	snapName = "raft.snap"
)

// OpenFileStorage opens (or creates) the storage in dir.
func OpenFileStorage(dir string) (*FileStorage, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	fs := &FileStorage{MemoryStorage: NewMemoryStorage(), dir: dir}
	if snap, err := readSnapFile(filepath.Join(dir, snapName)); err != nil {
		return nil, err
	} else if !snap.IsEmpty() {
		fs.MemoryStorage.ApplySnapshot(snap)
		fs.info.SnapshotIndex = snap.Index
	}
	f, err := os.OpenFile(filepath.Join(dir, walName), os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	good, err := fs.replay(f)
	if err != nil {
		f.Close()
		return nil, err
	}
	size, _ := f.Seek(0, io.SeekEnd)
	if size > good {
		fs.info.TruncatedBytes = size - good
		if err := f.Truncate(good); err != nil {
			f.Close()
			return nil, err
		}
		if err := f.Sync(); err != nil {
			f.Close()
			return nil, err
		}
	}
	if _, err := f.Seek(good, io.SeekStart); err != nil {
		f.Close()
		return nil, err
	}
	fs.wal = f
	return fs, nil
}

// Recovery reports what OpenFileStorage found.
func (fs *FileStorage) Recovery() RecoveryInfo { return fs.info }

// replay applies valid records and returns the offset just past the last one.
func (fs *FileStorage) replay(f *os.File) (int64, error) {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}
	data, err := io.ReadAll(f)
	if err != nil {
		return 0, err
	}
	var off int64
	for {
		if len(data[off:]) < 8 {
			return off, nil
		}
		n := binary.BigEndian.Uint32(data[off:])
		sum := binary.BigEndian.Uint32(data[off+4:])
		if n == 0 || int64(n) > int64(len(data))-off-8 {
			return off, nil
		}
		body := data[off+8 : off+8+int64(n)]
		if wire.Checksum(body) != sum {
			return off, nil
		}
		if err := fs.applyRecord(body[0], body[1:]); err != nil {
			return off, nil
		}
		fs.info.Records++
		off += 8 + int64(n)
	}
}

func (fs *FileStorage) applyRecord(typ byte, payload []byte) error {
	d := &rd{b: payload}
	switch typ {
	case recEntry:
		e := readEntry(d)
		if d.err != nil {
			return d.err
		}
		if e.Index <= fs.snap.Index {
			return nil
		}
		return fs.MemoryStorage.Append([]Entry{e})
	case recHardState:
		hs := HardState{Term: d.u(), Vote: d.str(), Commit: d.u()}
		if d.err != nil {
			return d.err
		}
		return fs.MemoryStorage.SetHardState(hs)
	case recSnapMark:
		idx := d.u()
		if d.err != nil {
			return d.err
		}
		// A snapshot was installed here: whatever followed its index in
		// the log at that moment was superseded.
		fs.MemoryStorage.truncateAfter(idx)
		return nil
	}
	return errCodec
}

func record(typ byte, payload []byte) []byte {
	body := append([]byte{typ}, payload...)
	out := binary.BigEndian.AppendUint32(nil, uint32(len(body)))
	out = binary.BigEndian.AppendUint32(out, wire.Checksum(body))
	return append(out, body...)
}

func hardStateRecord(hs HardState) []byte {
	e := &buf{}
	e.u(hs.Term)
	e.str(hs.Vote)
	e.u(hs.Commit)
	return record(recHardState, e.b)
}

// Save persists a Ready's snapshot, entries and hard state (in that order)
// and fsyncs. Call it before sending the Ready's messages.
func (fs *FileStorage) Save(hs HardState, ents []Entry, snap Snapshot) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	var w []byte
	if !snap.IsEmpty() {
		if err := writeSnapFile(filepath.Join(fs.dir, snapName), snap); err != nil {
			return err
		}
		if err := fs.MemoryStorage.ApplySnapshot(snap); err != nil && !errors.Is(err, ErrSnapOutOfDate) {
			return err
		}
		e := &buf{}
		e.u(snap.Index)
		w = append(w, record(recSnapMark, e.b)...)
	}
	for _, ent := range ents {
		e := &buf{}
		appendEntry(e, ent)
		w = append(w, record(recEntry, e.b)...)
	}
	if !hs.IsEmpty() {
		w = append(w, hardStateRecord(hs)...)
	}
	if len(w) > 0 {
		if _, err := fs.wal.Write(w); err != nil {
			return err
		}
		if err := fs.wal.Sync(); err != nil {
			return err
		}
	}
	if err := fs.MemoryStorage.Append(ents); err != nil {
		return err
	}
	if !hs.IsEmpty() {
		fs.MemoryStorage.SetHardState(hs)
	}
	return nil
}

// CreateSnapshot records a snapshot durably.
func (fs *FileStorage) CreateSnapshot(i uint64, conf ConfState, data []byte) (Snapshot, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	snap, err := fs.MemoryStorage.CreateSnapshot(i, conf, data)
	if err != nil {
		return snap, err
	}
	return snap, writeSnapFile(filepath.Join(fs.dir, snapName), snap)
}

// Compact drops entries up to i (which must be covered by a snapshot) and
// rewrites the WAL so it stops growing.
func (fs *FileStorage) Compact(i uint64) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if err := fs.MemoryStorage.Compact(i); err != nil {
		return err
	}
	ms := fs.MemoryStorage
	ms.mu.Lock()
	var w []byte
	for _, ent := range ms.ents[1:] {
		e := &buf{}
		appendEntry(e, ent)
		w = append(w, record(recEntry, e.b)...)
	}
	if !ms.hs.IsEmpty() {
		w = append(w, hardStateRecord(ms.hs)...)
	}
	ms.mu.Unlock()
	tmp := filepath.Join(fs.dir, walName+".tmp")
	if err := writeFileSync(tmp, w); err != nil {
		return err
	}
	if err := os.Rename(tmp, filepath.Join(fs.dir, walName)); err != nil {
		return err
	}
	f, err := os.OpenFile(filepath.Join(fs.dir, walName), os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	fs.wal.Close()
	fs.wal = f
	return syncDir(fs.dir)
}

// Close closes the WAL.
func (fs *FileStorage) Close() error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.wal.Close()
}

// truncateAfter drops entries above i.
func (ms *MemoryStorage) truncateAfter(i uint64) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	if i < ms.offset() {
		return
	}
	if k := i - ms.offset() + 1; k < uint64(len(ms.ents)) {
		ms.ents = ms.ents[:k]
	}
}

func writeSnapFile(path string, s Snapshot) error {
	e := &buf{}
	appendSnapshot(e, s)
	return writeFileSync(path+".tmp", record(recSnapMark, e.b), path)
}

func readSnapFile(path string) (Snapshot, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return Snapshot{}, nil
	}
	if err != nil {
		return Snapshot{}, err
	}
	if len(data) < 9 {
		return Snapshot{}, fmt.Errorf("raft: snapshot file %s is truncated", path)
	}
	n := binary.BigEndian.Uint32(data)
	body := data[8:]
	if int(n) != len(body) || wire.Checksum(body) != binary.BigEndian.Uint32(data[4:]) {
		return Snapshot{}, fmt.Errorf("raft: snapshot file %s is corrupt", path)
	}
	d := &rd{b: body[1:]}
	s := readSnapshot(d)
	if d.err != nil {
		return Snapshot{}, fmt.Errorf("raft: snapshot file %s is corrupt", path)
	}
	return s, nil
}

// writeFileSync writes data to tmp, fsyncs, and (if final is given) renames
// it into place atomically.
func writeFileSync(tmp string, data []byte, final ...string) error {
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if len(final) > 0 {
		if err := os.Rename(tmp, final[0]); err != nil {
			return err
		}
		return syncDir(filepath.Dir(final[0]))
	}
	return nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	d.Sync() // best effort: not every platform supports directory fsync
	return nil
}
