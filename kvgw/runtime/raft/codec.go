package raft

import (
	"errors"

	"github.com/Sebby1770/kvstore/kvgw/netstack/wire"
)

// Binary encodings for messages (the network) and entries (the WAL). Every
// decoder bounds-checks and returns errors; FuzzUnmarshalMessage holds it to
// that.

var errCodec = errors.New("raft: malformed encoding")

// MaxMessageEntries bounds entries per decoded message.
const MaxMessageEntries = 1 << 16

type buf struct{ b []byte }

func (e *buf) u(v uint64)     { e.b = wire.AppendVarint(e.b, v) }
func (e *buf) bytes(v []byte) { e.u(uint64(len(v))); e.b = append(e.b, v...) }
func (e *buf) str(s string)   { e.u(uint64(len(s))); e.b = append(e.b, s...) }

type rd struct {
	b   []byte
	err error
}

func (d *rd) u() uint64 {
	if d.err != nil {
		return 0
	}
	v, n, err := wire.ParseVarint(d.b)
	if err != nil {
		d.err = errCodec
		return 0
	}
	d.b = d.b[n:]
	return v
}

func (d *rd) bytes() []byte {
	n := d.u()
	if d.err != nil || n > uint64(len(d.b)) {
		d.err = errCodec
		return nil
	}
	out := append([]byte(nil), d.b[:n]...)
	d.b = d.b[n:]
	return out
}

func (d *rd) str() string { return string(d.bytes()) }

func appendEntry(e *buf, ent Entry) {
	e.u(ent.Term)
	e.u(ent.Index)
	e.u(uint64(ent.Type))
	e.bytes(ent.Data)
}

func readEntry(d *rd) Entry {
	ent := Entry{Term: d.u(), Index: d.u(), Type: EntryType(d.u())}
	ent.Data = d.bytes()
	if ent.Type > EntryConfChange {
		d.err = errCodec
	}
	return ent
}

func appendStrings(e *buf, ss []string) {
	e.u(uint64(len(ss)))
	for _, s := range ss {
		e.str(s)
	}
}

func readStrings(d *rd) []string {
	n := d.u()
	if n > 1<<16 {
		d.err = errCodec
		return nil
	}
	var out []string
	for i := uint64(0); i < n && d.err == nil; i++ {
		out = append(out, d.str())
	}
	return out
}

func appendSnapshot(e *buf, s Snapshot) {
	e.u(s.Index)
	e.u(s.Term)
	appendStrings(e, s.Conf.Voters)
	appendStrings(e, s.Conf.Learners)
	e.bytes(s.Data)
}

func readSnapshot(d *rd) Snapshot {
	s := Snapshot{Index: d.u(), Term: d.u()}
	s.Conf.Voters = readStrings(d)
	s.Conf.Learners = readStrings(d)
	s.Data = d.bytes()
	return s
}

// Marshal encodes m for the network.
func (m Message) Marshal() []byte {
	e := &buf{b: []byte{1, byte(m.Type)}}
	e.str(m.From)
	e.str(m.To)
	e.u(m.Term)
	e.u(m.LogTerm)
	e.u(m.Index)
	e.u(m.Commit)
	var flags uint64
	if m.Reject {
		flags |= 1
	}
	if m.Snapshot != nil {
		flags |= 2
	}
	e.u(flags)
	e.u(m.RejectHint)
	e.bytes(m.Context)
	e.u(uint64(len(m.Entries)))
	for _, ent := range m.Entries {
		appendEntry(e, ent)
	}
	if m.Snapshot != nil {
		appendSnapshot(e, *m.Snapshot)
	}
	return e.b
}

// UnmarshalMessage decodes a Marshal'd message.
func UnmarshalMessage(b []byte) (Message, error) {
	if len(b) < 2 || b[0] != 1 {
		return Message{}, errCodec
	}
	m := Message{Type: MessageType(b[1])}
	if m.Type >= msgTypeCount {
		return Message{}, errCodec
	}
	d := &rd{b: b[2:]}
	m.From, m.To = d.str(), d.str()
	m.Term, m.LogTerm, m.Index, m.Commit = d.u(), d.u(), d.u(), d.u()
	flags := d.u()
	m.Reject = flags&1 != 0
	m.RejectHint = d.u()
	m.Context = d.bytes()
	if len(m.Context) == 0 {
		m.Context = nil
	}
	n := d.u()
	if n > MaxMessageEntries {
		return Message{}, errCodec
	}
	for i := uint64(0); i < n && d.err == nil; i++ {
		m.Entries = append(m.Entries, readEntry(d))
	}
	if flags&2 != 0 {
		s := readSnapshot(d)
		m.Snapshot = &s
	}
	if d.err != nil || len(d.b) != 0 {
		return Message{}, errCodec
	}
	return m, nil
}
