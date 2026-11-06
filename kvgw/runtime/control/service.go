package control

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Sebby1770/kvstore/kvgw/runtime/raft"
)

// Transport carries Raft messages between Services. Send must not block: a
// real transport queues per peer and drops (Raft retransmits) when a peer is
// slow or unreachable, calling ReportUnreachable / ReportSnapshot as it
// learns about delivery.
type Transport interface {
	Send(msgs []raft.Message)
}

// ServiceConfig configures a Service.
type ServiceConfig struct {
	ID string
	// Peers are the founding voters (bootstrap). Leave empty on a node that
	// joins an existing cluster; it waits until the leader adds it.
	Peers []string
	// DataDir holds the Raft WAL and snapshots. Empty means in-memory
	// (tests, and nodes that deliberately keep no durable state).
	DataDir       string
	Transport     Transport
	TickInterval  time.Duration // [100ms]
	ElectionTick  int           // [10]
	HeartbeatTick int           // [1]
	// SnapshotEvery: entries between state-machine snapshots [1024].
	SnapshotEvery uint64
	// Clock stamps proposals ("now" in every command) [time.Now].
	Clock func() time.Time
	Logf  func(format string, args ...any)
}

// Errors.
var (
	ErrStopped    = errors.New("control: service stopped")
	ErrNoLeader   = errors.New("control: no leader (election in progress or quorum lost)")
	ErrNotApplied = errors.New("control: outcome unknown: the proposal was not applied before the deadline")
)

// Service runs one control-plane replica: a Raft node, its storage, and the
// FSM, behind a goroutine that ticks, persists, sends and applies.
type Service struct {
	cfg     ServiceConfig
	fsm     *FSM
	store   raft.Storage
	file    *raft.FileStorage
	mem     *raft.MemoryStorage
	snapIdx uint64

	mu       sync.Mutex
	rn       *raft.RawNode
	applied  uint64
	status   raft.Status
	waiters  map[string]chan Result
	reads    map[string]chan uint64
	appliedC chan struct{} // closed and replaced whenever applied advances
	ringSubs []func(RingConfig)
	confSubs []func(raft.ConfState)

	kick    chan struct{}
	stop    chan struct{}
	done    chan struct{}
	stopped bool
}

// NewService opens storage and restores state; Start runs it.
func NewService(cfg ServiceConfig) (*Service, error) {
	if cfg.ID == "" || cfg.Transport == nil {
		return nil, errors.New("control: ServiceConfig needs ID and Transport")
	}
	if cfg.TickInterval <= 0 {
		cfg.TickInterval = 100 * time.Millisecond
	}
	if cfg.SnapshotEvery == 0 {
		cfg.SnapshotEvery = 1024
	}
	if cfg.Clock == nil {
		cfg.Clock = time.Now
	}
	s := &Service{cfg: cfg, fsm: NewFSM(), waiters: map[string]chan Result{}, reads: map[string]chan uint64{},
		appliedC: make(chan struct{}), kick: make(chan struct{}, 1), stop: make(chan struct{}), done: make(chan struct{})}
	if cfg.DataDir != "" {
		fs, err := raft.OpenFileStorage(cfg.DataDir)
		if err != nil {
			return nil, err
		}
		s.file, s.store = fs, fs
	} else {
		s.mem = raft.NewMemoryStorage()
		s.store = s.mem
	}
	snap, err := s.store.Snapshot()
	if err != nil {
		return nil, err
	}
	if !snap.IsEmpty() {
		if err := s.fsm.Restore(snap.Data); err != nil {
			return nil, fmt.Errorf("control: restore snapshot: %w", err)
		}
		s.applied, s.snapIdx = snap.Index, snap.Index
	}
	rn, err := raft.NewRawNode(&raft.Config{
		ID: cfg.ID, Peers: cfg.Peers, Storage: s.store, Applied: s.applied,
		ElectionTick: cfg.ElectionTick, HeartbeatTick: cfg.HeartbeatTick,
		PreVote: true, CheckQuorum: true, MaxSizePerMsg: 512 << 10, MaxInflightMsgs: 128,
	})
	if err != nil {
		return nil, err
	}
	s.rn = rn
	s.status = rn.Status()
	return s, nil
}

// Start launches the run loop.
func (s *Service) Start() { go s.loop() }

// Stop halts the loop and closes storage. Pending proposals fail.
func (s *Service) Stop() {
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return
	}
	s.stopped = true
	s.mu.Unlock()
	close(s.stop)
	<-s.done
	if s.file != nil {
		s.file.Close()
	}
}

func (s *Service) logf(format string, args ...any) {
	if s.cfg.Logf != nil {
		s.cfg.Logf(format, args...)
	}
}

func (s *Service) poke() {
	select {
	case s.kick <- struct{}{}:
	default:
	}
}

func (s *Service) loop() {
	defer close(s.done)
	t := time.NewTicker(s.cfg.TickInterval)
	defer t.Stop()
	for {
		select {
		case <-s.stop:
			s.mu.Lock()
			for id, w := range s.waiters {
				close(w)
				delete(s.waiters, id)
			}
			s.mu.Unlock()
			return
		case <-t.C:
			s.mu.Lock()
			s.rn.Tick()
			s.mu.Unlock()
		case <-s.kick:
		}
		s.processReady()
	}
}

// processReady runs the Ready contract: persist, send, apply, advance.
func (s *Service) processReady() {
	var ringChanged *RingConfig
	var confChanged *raft.ConfState
	s.mu.Lock()
	for s.rn.HasReady() {
		rd := s.rn.Ready()
		if err := s.persist(rd); err != nil {
			s.logf("control: persist failed, stopping replica: %v", err)
			s.mu.Unlock()
			go s.Stop()
			return
		}
		if len(rd.Messages) > 0 {
			s.cfg.Transport.Send(rd.Messages)
		}
		if !rd.Snapshot.IsEmpty() && rd.Snapshot.Index > s.applied {
			if err := s.fsm.Restore(rd.Snapshot.Data); err != nil {
				s.logf("control: restore snapshot %d: %v", rd.Snapshot.Index, err)
			} else {
				s.applied = rd.Snapshot.Index
				r := s.fsm.Ring()
				ringChanged = &r
			}
		}
		for _, e := range rd.CommittedEntries {
			if e.Index <= s.applied {
				continue
			}
			switch e.Type {
			case raft.EntryNormal:
				id, cmd := splitEntry(e.Data)
				res := s.fsm.Apply(e.Index, cmd)
				if res.Op == OpSetRing && res.OK() {
					ringChanged = res.Ring
				}
				if w, ok := s.waiters[id]; ok {
					w <- res
					delete(s.waiters, id)
				}
			}
			s.applied = e.Index
		}
		for _, rs := range rd.ReadStates {
			if w, ok := s.reads[string(rs.RequestCtx)]; ok {
				w <- rs.Index
				delete(s.reads, string(rs.RequestCtx))
			}
		}
		hadConf := false
		for _, e := range rd.CommittedEntries {
			if e.Type == raft.EntryConfChange {
				hadConf = true
			}
		}
		s.rn.Advance(rd)
		if hadConf {
			cs := s.rn.ConfState()
			confChanged = &cs
		}
		close(s.appliedC)
		s.appliedC = make(chan struct{})
		s.maybeSnapshot()
	}
	s.status = s.rn.Status()
	subs, csubs := s.ringSubs, s.confSubs
	s.mu.Unlock()
	if ringChanged != nil {
		for _, f := range subs {
			f(*ringChanged)
		}
	}
	if confChanged != nil {
		for _, f := range csubs {
			f(*confChanged)
		}
	}
}

func (s *Service) persist(rd raft.Ready) error {
	if s.file != nil {
		return s.file.Save(rd.HardState, rd.Entries, rd.Snapshot)
	}
	if !rd.Snapshot.IsEmpty() {
		if err := s.mem.ApplySnapshot(rd.Snapshot); err != nil && !errors.Is(err, raft.ErrSnapOutOfDate) {
			return err
		}
	}
	if err := s.mem.Append(rd.Entries); err != nil {
		return err
	}
	if !rd.HardState.IsEmpty() {
		s.mem.SetHardState(rd.HardState)
	}
	return nil
}

// maybeSnapshot compacts the log every SnapshotEvery applied entries,
// keeping a tail so briefly lagging followers can catch up from the log.
func (s *Service) maybeSnapshot() {
	if s.applied < s.snapIdx+s.cfg.SnapshotEvery {
		return
	}
	data, err := s.fsm.Snapshot()
	if err != nil {
		return
	}
	cs := s.rn.ConfState()
	if s.file != nil {
		_, err = s.file.CreateSnapshot(s.applied, cs, data)
	} else {
		_, err = s.mem.CreateSnapshot(s.applied, cs, data)
	}
	if err != nil {
		return
	}
	s.snapIdx = s.applied
	const keep = 256
	if s.applied > keep {
		if s.file != nil {
			s.file.Compact(s.applied - keep)
		} else {
			s.mem.Compact(s.applied - keep)
		}
	}
}

// Step feeds a Raft message from a peer.
func (s *Service) Step(m raft.Message) error {
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return ErrStopped
	}
	err := s.rn.Step(m)
	s.mu.Unlock()
	s.poke()
	return err
}

// ReportUnreachable tells Raft a peer could not be reached.
func (s *Service) ReportUnreachable(id string) {
	s.mu.Lock()
	s.rn.ReportUnreachable(id)
	s.mu.Unlock()
}

// ReportSnapshot tells Raft whether a snapshot reached a peer.
func (s *Service) ReportSnapshot(id string, ok bool) {
	s.mu.Lock()
	s.rn.ReportSnapshot(id, ok)
	s.mu.Unlock()
	s.poke()
}

// ---- proposals ----

func newID() string {
	var b [12]byte
	rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// Entries carry a request ID so the proposer can match the applied result.
func joinEntry(id string, cmd []byte) []byte {
	return append(append([]byte{byte(len(id))}, id...), cmd...)
}

func splitEntry(b []byte) (string, []byte) {
	if len(b) == 0 || int(b[0])+1 > len(b) {
		return "", b
	}
	n := int(b[0])
	return string(b[1 : 1+n]), b[1+n:]
}

// Now is the clock that stamps commands.
func (s *Service) Now() time.Time { return s.cfg.Clock() }

// Propose replicates cmd (built with the *Cmd constructors) and returns the
// FSM's Result once this replica has applied it. Proposals made on a
// follower are forwarded to the leader by Raft. If ctx ends first the
// outcome is unknown (the command may still commit): callers must only
// blindly retry commands that are safe to repeat.
func (s *Service) Propose(ctx context.Context, cmd []byte) (Result, error) {
	id := newID()
	data := joinEntry(id, cmd)
	w := make(chan Result, 1)
	backoff := 20 * time.Millisecond
	for {
		s.mu.Lock()
		if s.stopped {
			s.mu.Unlock()
			return Result{}, ErrStopped
		}
		s.waiters[id] = w
		err := s.rn.Propose(data)
		if err != nil {
			delete(s.waiters, id)
		}
		s.mu.Unlock()
		if err == nil {
			break
		}
		if !errors.Is(err, raft.ErrProposalDropped) {
			return Result{}, err
		}
		select {
		case <-time.After(backoff):
			backoff = min(2*backoff, 500*time.Millisecond)
		case <-ctx.Done():
			return Result{}, ErrNoLeader
		}
	}
	s.poke()
	select {
	case res, ok := <-w:
		if !ok {
			return Result{}, ErrStopped
		}
		return res, nil
	case <-ctx.Done():
		s.mu.Lock()
		delete(s.waiters, id)
		s.mu.Unlock()
		return Result{}, ErrNotApplied
	}
}

// Apply is Propose for callers that want an error rather than a Result code.
func (s *Service) Apply(ctx context.Context, cmd []byte) (Result, error) {
	res, err := s.Propose(ctx, cmd)
	if err != nil {
		return res, err
	}
	return res, res.Err()
}

// ---- reads ----

// ReadBarrier blocks until this replica has applied everything committed
// before the call began (Raft ReadIndex). Reads of the FSM after it returns
// are linearizable.
func (s *Service) ReadBarrier(ctx context.Context) error {
	backoff := 20 * time.Millisecond
	for {
		rctx := newID()
		w := make(chan uint64, 1)
		s.mu.Lock()
		if s.stopped {
			s.mu.Unlock()
			return ErrStopped
		}
		s.reads[rctx] = w
		err := s.rn.ReadIndex([]byte(rctx))
		s.mu.Unlock()
		s.poke()
		if err == nil {
			select {
			case idx := <-w:
				return s.waitApplied(ctx, idx)
			case <-time.After(5 * s.cfg.TickInterval * time.Duration(max(s.cfg.ElectionTick, 10))):
				// No answer within an election timeout: leadership may have
				// moved. Try again.
			case <-ctx.Done():
				s.dropRead(rctx)
				return ErrNoLeader
			}
		}
		s.dropRead(rctx)
		select {
		case <-time.After(backoff):
			backoff = min(2*backoff, 500*time.Millisecond)
		case <-ctx.Done():
			return ErrNoLeader
		}
	}
}

func (s *Service) dropRead(rctx string) {
	s.mu.Lock()
	delete(s.reads, rctx)
	s.mu.Unlock()
}

func (s *Service) waitApplied(ctx context.Context, idx uint64) error {
	for {
		s.mu.Lock()
		if s.applied >= idx {
			s.mu.Unlock()
			return nil
		}
		ch := s.appliedC
		s.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		case <-s.stop:
			return ErrStopped
		}
	}
}

// ---- membership and introspection ----

func (s *Service) confChange(ctx context.Context, cc raft.ConfChange) error {
	backoff := 20 * time.Millisecond
	for {
		s.mu.Lock()
		if s.stopped {
			s.mu.Unlock()
			return ErrStopped
		}
		if s.status.Role != raft.Leader {
			s.mu.Unlock()
			return fmt.Errorf("control: membership changes must go to the leader (%q)", s.status.Lead)
		}
		target := s.rn.Status().LastIndex + 1
		err := s.rn.ProposeConfChange(cc)
		s.mu.Unlock()
		s.poke()
		if err == nil {
			return s.waitApplied(ctx, target)
		}
		if !errors.Is(err, raft.ErrProposalDropped) {
			return err
		}
		select { // another change is still pending
		case <-time.After(backoff):
			backoff = min(2*backoff, 500*time.Millisecond)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// AddLearner adds a non-voting replica (call on the leader).
func (s *Service) AddLearner(ctx context.Context, id string) error {
	return s.confChange(ctx, raft.ConfChange{Type: raft.AddLearner, Node: id})
}

// AddVoter adds (or promotes) a voting replica (call on the leader).
func (s *Service) AddVoter(ctx context.Context, id string) error {
	return s.confChange(ctx, raft.ConfChange{Type: raft.AddVoter, Node: id})
}

// RemoveNode removes a replica (call on the leader).
func (s *Service) RemoveNode(ctx context.Context, id string) error {
	return s.confChange(ctx, raft.ConfChange{Type: raft.RemoveNode, Node: id})
}

// TransferLeadership asks the leader to hand over to id.
func (s *Service) TransferLeadership(id string) {
	s.mu.Lock()
	s.rn.TransferLeadership(id)
	s.mu.Unlock()
	s.poke()
}

// Campaign makes this replica start an election now.
func (s *Service) Campaign() {
	s.mu.Lock()
	s.rn.Campaign()
	s.mu.Unlock()
	s.poke()
}

// FSM is the local replica of the state machine. Read it after ReadBarrier
// for linearizable results, or directly for possibly-stale ones.
func (s *Service) FSM() *FSM { return s.fsm }

// Status reports Raft state as of the last Ready.
func (s *Service) Status() raft.Status {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.status
}

// Leader is the current leader's ID ("" if unknown).
func (s *Service) Leader() string { return s.Status().Lead }

// IsLeader reports whether this replica leads.
func (s *Service) IsLeader() bool { return s.Status().Role == raft.Leader }

// Applied is the last applied index.
func (s *Service) Applied() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.applied
}

// OnRing registers a callback run (outside locks) after every applied ring
// change, and once immediately with the current ring if one is set.
func (s *Service) OnRing(f func(RingConfig)) {
	s.mu.Lock()
	s.ringSubs = append(s.ringSubs, f)
	s.mu.Unlock()
	if r := s.fsm.Ring(); r.Epoch > 0 {
		f(r)
	}
}

// OnConf registers a callback run after membership changes apply.
func (s *Service) OnConf(f func(raft.ConfState)) {
	s.mu.Lock()
	s.confSubs = append(s.confSubs, f)
	s.mu.Unlock()
}
