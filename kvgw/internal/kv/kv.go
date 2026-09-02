// Package kv holds the value types and the Node interface shared by the TCP
// client and the quorum coordinator. Keeping them here (instead of in the
// client) lets the coordinator be tested against in-memory fake nodes.
package kv

// State is the outcome of a read on a single node.
type State int

const (
	Missing State = iota // key never existed on this node
	Present              // key has a live value
	Deleted              // key has a tombstone
)

func (s State) String() string {
	switch s {
	case Present:
		return "present"
	case Deleted:
		return "deleted"
	default:
		return "missing"
	}
}

// ReadResult is one node's answer to a Get, including the version so the
// coordinator can pick the newest copy across replicas.
type ReadResult struct {
	State     State
	Value     []byte
	Version   uint64
	ExpiresAt uint64 // unix seconds; 0 = never
}

type Entry struct {
	Key       []byte
	Value     []byte
	Version   uint64
	ExpiresAt uint64
	Tombstone bool
}

type Stat struct {
	Count      uint32
	Capacity   uint32
	MaxVersion uint64
	Tombstones uint32
}

// Node is one storage node as seen by the gateway. The real implementation is
// a TCP client; tests use an in-memory fake.
type Node interface {
	Addr() string
	Ping() error
	Get(key []byte) (ReadResult, error)
	Put(key, val []byte, version uint64) error
	// PutX is PUT with a native expires_at (unix seconds, 0 = never).
	PutX(key, val []byte, version, expiresAt uint64) error
	// Del reports whether the key was present (true) or already absent (false).
	Del(key []byte, version uint64) (bool, error)
	Scan() ([]Entry, error)
	ScanAll() ([]Entry, error)
	Stat() (Stat, error)
}
