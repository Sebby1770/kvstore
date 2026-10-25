package swim

import (
	"errors"
	"sort"

	"github.com/Sebby1770/kvstore/kvgw/netstack/wire"
)

// Message types.
const (
	msgPing     = 1
	msgAck      = 2
	msgPingReq  = 3
	msgNack     = 4
	msgPushPull = 5
	msgGossip   = 6
	version     = 1
)

var errDecode = errors.New("swim: malformed message")

// update is one rumour about a member (also the unit of push-pull state).
type update struct {
	name, addr  string
	state       State
	incarnation uint64
	meta        map[string]string
}

// message is the decoded form of every datagram.
type message struct {
	typ        byte
	from, addr string // sender's name and advertised address
	seq        uint64
	target     string // ping: who we think we're pinging; ping-req: whom to probe
	targetAddr string
	join       bool // push-pull: this is a join (reply expected)
	reply      bool // push-pull: this is the reply half
	updates    []update
}

type enc struct{ b []byte }

func (e *enc) varint(v uint64) { e.b = wire.AppendVarint(e.b, v) }
func (e *enc) str(s string) {
	e.varint(uint64(len(s)))
	e.b = append(e.b, s...)
}

func encodeUpdate(e *enc, u update) {
	e.str(u.name)
	e.str(u.addr)
	e.b = append(e.b, byte(u.state))
	e.varint(u.incarnation)
	keys := make([]string, 0, len(u.meta))
	for k := range u.meta {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	e.varint(uint64(len(keys)))
	for _, k := range keys {
		e.str(k)
		e.str(u.meta[k])
	}
}

func updateSize(u update) int {
	var e enc
	encodeUpdate(&e, u)
	return len(e.b)
}

// header encodes everything but the updates (and their count).
func (m *message) header() []byte {
	e := &enc{b: []byte{version, m.typ}}
	e.str(m.from)
	e.str(m.addr)
	e.varint(m.seq)
	e.str(m.target)
	e.str(m.targetAddr)
	var flags byte
	if m.join {
		flags |= 1
	}
	if m.reply {
		flags |= 2
	}
	e.b = append(e.b, flags)
	return e.b
}

func (m *message) encode() []byte {
	e := &enc{b: m.header()}
	e.varint(uint64(len(m.updates)))
	for _, u := range m.updates {
		encodeUpdate(e, u)
	}
	return e.b
}

type dec struct {
	b   []byte
	err error
}

func (d *dec) varint() uint64 {
	if d.err != nil {
		return 0
	}
	v, n, err := wire.ParseVarint(d.b)
	if err != nil {
		d.err = errDecode
		return 0
	}
	d.b = d.b[n:]
	return v
}

func (d *dec) str() string {
	n := d.varint()
	if d.err != nil {
		return ""
	}
	if n > uint64(len(d.b)) || n > 4096 {
		d.err = errDecode
		return ""
	}
	s := string(d.b[:n])
	d.b = d.b[n:]
	return s
}

func (d *dec) byte1() byte {
	if d.err != nil || len(d.b) == 0 {
		d.err = errDecode
		return 0
	}
	c := d.b[0]
	d.b = d.b[1:]
	return c
}

func decode(b []byte) (message, error) {
	var m message
	d := &dec{b: b}
	if d.byte1() != version {
		return m, errDecode
	}
	m.typ = d.byte1()
	m.from, m.addr = d.str(), d.str()
	m.seq = d.varint()
	m.target, m.targetAddr = d.str(), d.str()
	flags := d.byte1()
	m.join, m.reply = flags&1 != 0, flags&2 != 0
	n := d.varint()
	if d.err != nil || n > 1024 || m.typ < msgPing || m.typ > msgGossip || m.from == "" {
		return message{}, errDecode
	}
	for i := uint64(0); i < n; i++ {
		u := update{name: d.str(), addr: d.str(), state: State(d.byte1()), incarnation: d.varint()}
		k := d.varint()
		if d.err != nil || k > 64 || u.state > StateLeft || u.name == "" {
			return message{}, errDecode
		}
		if k > 0 {
			u.meta = make(map[string]string, k)
			for j := uint64(0); j < k; j++ {
				key := d.str()
				u.meta[key] = d.str()
			}
		}
		if d.err != nil {
			return message{}, errDecode
		}
		m.updates = append(m.updates, u)
	}
	if len(d.b) != 0 {
		return message{}, errDecode
	}
	return m, nil
}
