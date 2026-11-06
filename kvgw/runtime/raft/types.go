package raft

import (
	"errors"
	"fmt"
)

// EntryType distinguishes application entries from membership changes.
type EntryType uint8

const (
	EntryNormal     EntryType = 0
	EntryConfChange EntryType = 1
)

// Entry is one log entry.
type Entry struct {
	Term  uint64
	Index uint64
	Type  EntryType
	Data  []byte
}

// HardState is what must be durable before messages are sent.
type HardState struct {
	Term   uint64
	Vote   string
	Commit uint64
}

// IsEmpty reports whether hs is the zero HardState.
func (hs HardState) IsEmpty() bool { return hs.Term == 0 && hs.Vote == "" && hs.Commit == 0 }

// ConfState is the cluster membership.
type ConfState struct {
	Voters   []string
	Learners []string
}

// Snapshot is a compacted prefix of the log plus the state machine image.
type Snapshot struct {
	Index, Term uint64
	Conf        ConfState
	Data        []byte
}

// IsEmpty reports whether s carries no snapshot.
func (s Snapshot) IsEmpty() bool { return s.Index == 0 }

// ConfChangeType is a single-server membership change.
type ConfChangeType uint8

const (
	AddVoter   ConfChangeType = 1
	AddLearner ConfChangeType = 2
	RemoveNode ConfChangeType = 3
)

func (t ConfChangeType) String() string {
	switch t {
	case AddVoter:
		return "add-voter"
	case AddLearner:
		return "add-learner"
	case RemoveNode:
		return "remove"
	}
	return "unknown"
}

// ConfChange adds or removes one node.
type ConfChange struct {
	Type ConfChangeType
	Node string
}

func (cc ConfChange) encode() []byte { return append([]byte{byte(cc.Type)}, cc.Node...) }

// DecodeConfChange parses the Data of an EntryConfChange.
func DecodeConfChange(b []byte) (ConfChange, error) {
	if len(b) < 2 || b[0] < 1 || b[0] > 3 {
		return ConfChange{}, errors.New("raft: malformed conf change")
	}
	return ConfChange{Type: ConfChangeType(b[0]), Node: string(b[1:])}, nil
}

// MessageType enumerates protocol messages. Local types never cross the
// network.
type MessageType uint8

const (
	MsgHup            MessageType = iota // local: start an election
	MsgBeat                              // local: leader sends heartbeats
	MsgProp                              // proposal (may be forwarded to the leader)
	MsgApp                               // append entries
	MsgAppResp                           //
	MsgVote                              //
	MsgVoteResp                          //
	MsgPreVote                           //
	MsgPreVoteResp                       //
	MsgHeartbeat                         //
	MsgHeartbeatResp                     //
	MsgSnap                              // install snapshot
	MsgSnapStatus                        // local: snapshot delivery report
	MsgUnreachable                       // local: peer unreachable report
	MsgReadIndex                         // linearizable read request
	MsgReadIndexResp                     //
	MsgTransferLeader                    //
	MsgTimeoutNow                        // "campaign now" from a transferring leader
	MsgCheckQuorum                       // local: leader checks it still has a quorum
	msgTypeCount
)

var msgNames = [...]string{"MsgHup", "MsgBeat", "MsgProp", "MsgApp", "MsgAppResp", "MsgVote", "MsgVoteResp",
	"MsgPreVote", "MsgPreVoteResp", "MsgHeartbeat", "MsgHeartbeatResp", "MsgSnap", "MsgSnapStatus",
	"MsgUnreachable", "MsgReadIndex", "MsgReadIndexResp", "MsgTransferLeader", "MsgTimeoutNow", "MsgCheckQuorum"}

func (t MessageType) String() string {
	if int(t) < len(msgNames) {
		return msgNames[t]
	}
	return fmt.Sprintf("MsgType(%d)", uint8(t))
}

func (t MessageType) local() bool {
	return t == MsgHup || t == MsgBeat || t == MsgSnapStatus || t == MsgUnreachable || t == MsgCheckQuorum
}

// Message is a Raft protocol message.
type Message struct {
	Type     MessageType
	From, To string
	Term     uint64
	// MsgApp: index/term of the entry preceding Entries. MsgVote: the
	// candidate's last index/term. MsgAppResp: the matched (or rejected)
	// index, with LogTerm the term at RejectHint. MsgReadIndexResp: the
	// read index.
	LogTerm, Index uint64
	Entries        []Entry
	Commit         uint64
	Reject         bool
	RejectHint     uint64
	Snapshot       *Snapshot
	Context        []byte
}

// Role is a node's role.
type Role uint8

const (
	Follower Role = iota
	PreCandidate
	Candidate
	Leader
)

func (r Role) String() string {
	return [...]string{"follower", "pre-candidate", "candidate", "leader"}[r]
}

// SoftState is volatile state worth reporting.
type SoftState struct {
	Lead string
	Role Role
}

// ReadState answers a ReadIndex request: once the local state machine has
// applied Index, reads that started before the request are linearizable.
type ReadState struct {
	Index      uint64
	RequestCtx []byte
}

// Ready bundles everything the application must do next, in order: persist
// HardState, Entries and Snapshot; then send Messages; then apply Snapshot
// and CommittedEntries; then call Advance.
type Ready struct {
	*SoftState
	HardState        HardState
	Entries          []Entry
	Snapshot         Snapshot
	CommittedEntries []Entry
	Messages         []Message
	ReadStates       []ReadState
}

// Errors.
var (
	ErrProposalDropped  = errors.New("raft: proposal dropped")
	ErrNotLeader        = errors.New("raft: not the leader")
	ErrCompacted        = errors.New("raft: requested index is compacted")
	ErrUnavailable      = errors.New("raft: requested entry is unavailable")
	ErrSnapOutOfDate    = errors.New("raft: snapshot is older than the existing one")
	ErrStepLocal        = errors.New("raft: local message type received from the network")
	ErrStepPeerNotFound = errors.New("raft: message from unknown peer")
)

// Progress is the leader's view of one follower.
type Progress struct {
	Match, Next  uint64
	State        string
	RecentActive bool
	Learner      bool
}

// Status reports a node's state.
type Status struct {
	ID         string
	Term       uint64
	Vote       string
	Lead       string
	Role       Role
	Commit     uint64
	Applied    uint64
	LastIndex  uint64
	Voters     []string
	Learners   []string
	Progress   map[string]Progress // leader only
	Transferee string
}
