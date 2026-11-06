package control

import (
	"encoding/json"
	"errors"
	"fmt"
)

// Code classifies the outcome of an applied command. The empty code is
// success. Codes are part of the wire contract (a Service returns Results to
// remote proposers), so they are short stable strings rather than Go errors.
type Code string

const (
	CodeOK         Code = ""
	CodeInvalid    Code = "invalid"     // malformed command or out-of-range field
	CodeConflict   Code = "conflict"    // lease held by someone else, CAS mismatch
	CodeNotHolder  Code = "not_holder"  // renew/release by someone who is not the holder
	CodeExpired    Code = "expired"     // renew of a lease whose TTL already lapsed
	CodeNotFound   Code = "not_found"   // no such lease / key / job
	CodeStaleToken Code = "stale_token" // fencing token older than the current grant/claim
	CodeStaleIndex Code = "stale_index" // Apply called with an index already applied
	CodeFull       Code = "full"        // a count limit (MaxLeases, MaxQueueJobs, …) was hit
)

// Sentinel errors, one per non-OK code, for errors.Is on Result.Err().
var (
	ErrInvalid    = errors.New("control: invalid command")
	ErrConflict   = errors.New("control: conflict")
	ErrNotHolder  = errors.New("control: not the lease holder")
	ErrExpired    = errors.New("control: lease expired")
	ErrNotFound   = errors.New("control: not found")
	ErrStaleToken = errors.New("control: stale fencing token")
	ErrStaleIndex = errors.New("control: index already applied")
	ErrFull       = errors.New("control: limit reached")
)

var codeErrors = map[Code]error{
	CodeInvalid:    ErrInvalid,
	CodeConflict:   ErrConflict,
	CodeNotHolder:  ErrNotHolder,
	CodeExpired:    ErrExpired,
	CodeNotFound:   ErrNotFound,
	CodeStaleToken: ErrStaleToken,
	CodeStaleIndex: ErrStaleIndex,
	CodeFull:       ErrFull,
}

// OpError is the error form of a failed Result. errors.Is(err, ErrConflict)
// (etc.) matches on Code, so it survives a JSON round trip through a Service.
type OpError struct {
	Op   string
	Code Code
	Msg  string
}

func (e *OpError) Error() string {
	if e.Msg == "" {
		return fmt.Sprintf("control: %s: %s", e.Op, e.Code)
	}
	return fmt.Sprintf("control: %s: %s: %s", e.Op, e.Code, e.Msg)
}

// Is reports whether target is the sentinel for e's code.
func (e *OpError) Is(target error) bool {
	s, ok := codeErrors[e.Code]
	return ok && s == target
}

// Result is what Apply returns for one command. Which payload field is set
// depends on the op:
//
//	set_ring                       Ring (the new config; the current one on conflict)
//	lease_*                        Lease (the grant; the current holder on conflict)
//	reg_put / reg_delete           Entry (the written entry; the current one on conflict)
//	job_enqueue                    Job (Existing=true if the id was already known)
//	job_claim                      Job, or nil when nothing is ready
//	job_complete/fail/extend/...   Job
//
// Every payload is a copy; holding a Result never aliases FSM state.
type Result struct {
	Op    string `json:"op"`
	Index uint64 `json:"index"`
	Code  Code   `json:"code,omitempty"`
	Error string `json:"error,omitempty"`

	Ring  *RingConfig    `json:"ring,omitempty"`
	Lease *Lease         `json:"lease,omitempty"`
	Entry *RegistryEntry `json:"entry,omitempty"`
	Job   *Job           `json:"job,omitempty"`

	// Existing: lease_acquire re-entered a lease the holder already held, or
	// job_enqueue found the id already present (nothing was changed).
	Existing bool `json:"existing,omitempty"`
	// Duplicate: job_complete repeated for an already-completed claim.
	Duplicate bool `json:"duplicate,omitempty"`
	// DeadLettered: job_fail exhausted the job's attempts.
	DeadLettered bool `json:"dead_lettered,omitempty"`
}

// OK reports whether the command succeeded.
func (r Result) OK() bool { return r.Code == CodeOK }

// Err returns nil on success, otherwise an *OpError matching the sentinel for
// the code.
func (r Result) Err() error {
	if r.Code == CodeOK {
		return nil
	}
	return &OpError{Op: r.Op, Code: r.Code, Msg: r.Error}
}

// Encode returns the JSON form of r (what a Service sends back to a remote
// proposer).
func (r Result) Encode() []byte {
	b, err := json.Marshal(r)
	if err != nil {
		panic("control: encoding Result: " + err.Error())
	}
	return b
}

// DecodeResult parses an encoded Result. It never panics on hostile input.
func DecodeResult(b []byte) (Result, error) {
	var r Result
	if err := json.Unmarshal(b, &r); err != nil {
		return Result{}, fmt.Errorf("control: decode result: %w", err)
	}
	return r, nil
}

// LeaseGrant extracts the lease from a lease_acquire / lease_renew result, or
// the error if the command failed.
func (r Result) LeaseGrant() (Lease, error) {
	if err := r.Err(); err != nil {
		return Lease{}, err
	}
	if r.Lease == nil {
		return Lease{}, &OpError{Op: r.Op, Code: CodeInvalid, Msg: "result carries no lease"}
	}
	return *r.Lease, nil
}

// ClaimedJob extracts the job from a job_claim result. ok is false (with a
// nil error) when the queue had nothing ready.
func (r Result) ClaimedJob() (job Job, ok bool, err error) {
	if err := r.Err(); err != nil {
		return Job{}, false, err
	}
	if r.Job == nil {
		return Job{}, false, nil
	}
	return *r.Job, true, nil
}

// RegistryEntry extracts the entry from a reg_put result.
func (r Result) RegistryEntry() (RegistryEntry, error) {
	if err := r.Err(); err != nil {
		return RegistryEntry{}, err
	}
	if r.Entry == nil {
		return RegistryEntry{}, &OpError{Op: r.Op, Code: CodeInvalid, Msg: "result carries no entry"}
	}
	return *r.Entry, nil
}

func ok() Result { return Result{} }

func fail(code Code, format string, args ...any) Result {
	return Result{Code: code, Error: fmt.Sprintf(format, args...)}
}
