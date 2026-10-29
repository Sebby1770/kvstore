// Package actor is a virtual-actor runtime in the style of Orleans: an actor
// is addressed by ID{Type, Key}, always exists logically, and is activated
// on demand on whichever node currently owns its ID -- callers never create,
// locate or destroy actors.
//
// # Execution
//
// Each activation runs on its own goroutine and processes one message at a
// time (turn-based concurrency), so actor code needs no locks. Mailboxes are
// bounded: Ask waits for room (until its context ends), Tell fails fast with
// ErrMailboxFull. Timers set with Context.SetTimer arrive through the mailbox
// like any other message, never concurrently with one. An activation that
// sits idle for IdleTimeout is deactivated, persisting dirty state first.
//
// # Placement and routing
//
// The Placement decides which node owns an ID (KVStore uses rendezvous
// hashing over SWIM's live members). Ask/Tell on any node forward to the
// owner through the Transport; a node that receives a message for an ID it
// does not own forwards it again, up to MaxHops, so routing converges while
// membership views settle instead of ping-ponging forever.
//
// # State and the single-activation argument
//
// Membership views are eventually consistent, so during churn two nodes can
// briefly both believe they own an ID and both activate it. Rather than a
// global directory (which would put a consensus round on every activation),
// this runtime fences state with optimistic concurrency: an activation loads
// state with a version and persists with "expect that version". The first
// writer wins; the stale activation's Persist fails with a conflict, the
// caller gets ErrStaleActivation, and the activation tears itself down
// without writing. The next message re-activates from the winner's state.
//
// The guarantee is therefore about state, not execution: no acknowledged
// (persisted) update is lost or overwritten by a stale activation, but two
// activations may briefly run concurrently and side effects outside the
// StateStore are not fenced. Actors that need exclusive side effects should
// take a lease (with its fencing token) from the control plane.
//
// # Failure
//
// A panic in Receive is recovered: the caller gets ErrActorPanicked, the
// activation is torn down without persisting, and the next message
// re-activates from the last persisted state after an exponential backoff.
package actor

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// ID names an actor.
type ID struct {
	Type string `json:"type"`
	Key  string `json:"key"`
}

func (id ID) String() string { return id.Type + "/" + id.Key }

// Actor is user code. Receive handles one message and returns a reply.
type Actor interface {
	Receive(ctx *Context, msg []byte) ([]byte, error)
}

// Activator is an optional hook run after state is loaded.
type Activator interface {
	OnActivate(ctx *Context) error
}

// Deactivator is an optional hook run before an activation goes away.
type Deactivator interface {
	OnDeactivate(ctx *Context) error
}

// Factory builds an actor instance for an ID.
type Factory func(ID) Actor

// Placement maps IDs to their owning node.
type Placement interface {
	Owner(id ID) (node string, ok bool)
}

// Envelope is a message in transit between nodes.
type Envelope struct {
	ID   ID     `json:"id"`
	Msg  []byte `json:"msg"`
	Hops int    `json:"hops"`
	Tell bool   `json:"tell,omitempty"`
}

// Transport delivers an envelope to a remote node's Host.HandleRemote.
type Transport interface {
	Deliver(ctx context.Context, node string, env Envelope) ([]byte, error)
}

// StateStore persists actor state with optimistic concurrency. Load returns
// version 0 for an actor that has never been saved. Save succeeds only if
// the stored version equals expect, returning the new version, and fails
// with ErrConflict otherwise.
type StateStore interface {
	Load(ctx context.Context, id ID) (data []byte, version uint64, err error)
	Save(ctx context.Context, id ID, data []byte, expect uint64) (uint64, error)
}

// Errors.
var (
	ErrConflict        = errors.New("actor: state version conflict")
	ErrStaleActivation = errors.New("actor: stale activation (another node owns this actor's state)")
	ErrMailboxFull     = errors.New("actor: mailbox full")
	ErrTooManyHops     = errors.New("actor: too many forwarding hops (placement not converged)")
	ErrUnknownType     = errors.New("actor: unknown actor type")
	ErrNoOwner         = errors.New("actor: no node owns this actor (no live members)")
	ErrClosed          = errors.New("actor: host closed")
	ErrActorPanicked   = errors.New("actor: actor panicked")
	ErrBackoff         = errors.New("actor: actor is restarting after a crash; retry shortly")
)

// RemoteError carries an error message across the transport.
type RemoteError struct{ Msg string }

func (e *RemoteError) Error() string { return e.Msg }

// Context is what an actor sees during one turn.
type Context struct {
	a     *activation
	ctx   context.Context
	timer string
}

// ID is the actor's ID.
func (c *Context) ID() ID { return c.a.id }

// Node is the node running this activation.
func (c *Context) Node() string { return c.a.host.cfg.Node }

// Context is the caller's context (deadline, cancellation).
func (c *Context) Context() context.Context { return c.ctx }

// Timer names the timer that triggered this turn ("" for a message).
func (c *Context) Timer() string { return c.timer }

// State is the actor's current state (loaded on activation).
func (c *Context) State() []byte { return c.a.state }

// SetState replaces the state in memory; Persist (or deactivation) saves it.
func (c *Context) SetState(b []byte) {
	c.a.state = append([]byte(nil), b...)
	c.a.dirty = true
}

// Version is the stored version the state was loaded at (or last saved as).
func (c *Context) Version() uint64 { return c.a.version }

// Persist saves the state, fenced by the version it was loaded at. On a
// conflict the activation is stale: it returns ErrStaleActivation and the
// activation is torn down after this turn without writing.
func (c *Context) Persist() error {
	a := c.a
	v, err := a.host.cfg.Store.Save(c.ctx, a.id, a.state, a.version)
	if errors.Is(err, ErrConflict) {
		a.poisoned = true
		a.host.count(func(s *Stats) { s.Conflicts++ })
		return ErrStaleActivation
	}
	if err != nil {
		return err
	}
	a.version, a.dirty = v, false
	return nil
}

// Ask sends a message to another actor and waits for the reply. Asking
// yourself would deadlock a turn-based actor and is refused.
func (c *Context) Ask(id ID, msg []byte) ([]byte, error) {
	if id == c.a.id {
		return nil, fmt.Errorf("actor: %s cannot Ask itself", id)
	}
	return c.a.host.Ask(c.ctx, id, msg)
}

// Tell sends a message without waiting.
func (c *Context) Tell(id ID, msg []byte) error { return c.a.host.Tell(c.ctx, id, msg) }

// SetTimer delivers payload to this actor every interval (through the
// mailbox) until cancelled or the activation ends.
func (c *Context) SetTimer(name string, every time.Duration, payload []byte) {
	c.a.setTimer(name, every, payload)
}

// CancelTimer stops a timer.
func (c *Context) CancelTimer(name string) { c.a.cancelTimer(name) }

// Deactivate ends the activation after this turn (persisting dirty state).
func (c *Context) Deactivate() { c.a.deactivate = true }
