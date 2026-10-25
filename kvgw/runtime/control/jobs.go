package control

import (
	"fmt"
	"slices"
	"sort"
	"time"
)

// DefaultVisibility applies when job_claim leaves visibility_ms at 0.
const DefaultVisibility = 30 * time.Second

// queue is one named job queue. Jobs are keyed by id; Seq orders them for
// FIFO claims. Completed jobs linger (bounded) so a producer retrying an
// enqueue is answered "already known" instead of running the work twice.
type queue struct {
	name string
	seq  uint64
	jobs map[string]*Job

	enqueued, claims, reclaims, completions, failures, deadLettered uint64
}

func newQueue(name string) *queue { return &queue{name: name, jobs: make(map[string]*Job)} }

func (q *queue) stats() QueueStats {
	s := QueueStats{Name: q.name, Enqueued: q.enqueued, Claims: q.claims, Reclaims: q.reclaims,
		Completions: q.completions, Failures: q.failures, DeadLettered: q.deadLettered}
	for _, j := range q.jobs {
		switch j.State {
		case JobPending:
			s.Pending++
		case JobClaimed:
			s.Claimed++
		case JobDead:
			s.Dead++
		case JobCompleted:
			s.Completed++
		}
	}
	return s
}

// sorted returns copies of the jobs that pass keep, in enqueue order.
func (q *queue) sorted(keep func(*Job) bool) []Job {
	out := make([]Job, 0, len(q.jobs))
	for _, j := range q.jobs {
		if keep(j) {
			out = append(out, j.clone())
		}
	}
	sort.Slice(out, func(a, b int) bool { return out[a].Seq < out[b].Seq })
	return out
}

func (q *queue) active() int {
	n := 0
	for _, j := range q.jobs {
		if j.State == JobPending || j.State == JobClaimed {
			n++
		}
	}
	return n
}

// prune forgets completed jobs past their retention, and the oldest ones
// beyond MaxCompletedPerQueue. Judged by command time, so deterministic.
func (q *queue) prune(now int64) {
	var done []*Job
	for id, j := range q.jobs {
		if j.State != JobCompleted {
			continue
		}
		if addMs(j.FinishedAt, CompletedRetention.Milliseconds()) <= now {
			delete(q.jobs, id)
			continue
		}
		done = append(done, j)
	}
	if extra := len(done) - MaxCompletedPerQueue; extra > 0 {
		sort.Slice(done, func(a, b int) bool { return done[a].FinishedIndex < done[b].FinishedIndex })
		for _, j := range done[:extra] {
			delete(q.jobs, j.ID)
		}
	}
}

func (q *queue) check(applied uint64) error {
	for id, j := range q.jobs {
		if j == nil || j.ID != id || id == "" || j.Queue != q.name {
			return fmt.Errorf("job %q: bad record", id)
		}
		if j.Seq == 0 || j.Seq > q.seq {
			return fmt.Errorf("job %q: seq %d outside (0, %d]", id, j.Seq, q.seq)
		}
		if j.Token > applied || j.FinishedIndex > applied {
			return fmt.Errorf("job %q: index from the future", id)
		}
		if j.MaxAttempts < 1 || j.MaxAttempts > MaxMaxAttempts || j.Attempts < 0 || j.Attempts > j.MaxAttempts {
			return fmt.Errorf("job %q: attempts %d/%d", id, j.Attempts, j.MaxAttempts)
		}
		switch j.State {
		case JobPending, JobDead, JobCompleted:
		case JobClaimed:
			if j.Token == 0 || j.Worker == "" {
				return fmt.Errorf("job %q: claimed without token/worker", id)
			}
		default:
			return fmt.Errorf("job %q: state %q", id, j.State)
		}
	}
	return nil
}

// job looks a job up for the token-checked ops.
func (f *FSM) job(c *Command) (*queue, *Job, *Result) {
	if r := checkName("queue", c.Queue, MaxNameLen); r != nil {
		return nil, nil, r
	}
	if r := checkName("id", c.ID, MaxNameLen); r != nil {
		return nil, nil, r
	}
	q, ok := f.queues[c.Queue]
	if !ok {
		r := fail(CodeNotFound, "no queue %q", truncate(c.Queue, 64))
		return nil, nil, &r
	}
	j, ok := q.jobs[c.ID]
	if !ok {
		r := fail(CodeNotFound, "no job %q in %q", truncate(c.ID, 64), truncate(c.Queue, 64))
		return nil, nil, &r
	}
	return q, j, nil
}

// claimedBy checks that c carries the token of j's current claim.
func claimedBy(j *Job, c *Command) *Result {
	if j.State != JobClaimed || j.Token != c.Token {
		r := fail(CodeStaleToken, "job %q: token %d is not the current claim (state %s, token %d)",
			truncate(j.ID, 64), c.Token, j.State, j.Token)
		cp := j.clone()
		r.Job = &cp
		return &r
	}
	return nil
}

func (f *FSM) jobEnqueue(_ uint64, c *Command) Result {
	if r := checkName("queue", c.Queue, MaxNameLen); r != nil {
		return *r
	}
	if r := checkName("id", c.ID, MaxNameLen); r != nil {
		return *r
	}
	if r := checkNow(c.Now); r != nil {
		return *r
	}
	if len(c.Payload) > MaxPayloadLen {
		return fail(CodeInvalid, "payload is %d bytes, limit %d", len(c.Payload), MaxPayloadLen)
	}
	attempts := c.MaxAttempts
	if attempts == 0 {
		attempts = DefaultMaxAttempts
	}
	if attempts < 1 || attempts > MaxMaxAttempts {
		return fail(CodeInvalid, "max_attempts=%d out of range [1, %d]", c.MaxAttempts, MaxMaxAttempts)
	}
	if r := checkMs("delay_ms", c.DelayMs, 0, MaxDelay); r != nil {
		return *r
	}
	q, ok := f.queues[c.Queue]
	if !ok {
		if len(f.queues) >= MaxQueues {
			return fail(CodeFull, "%d queues, limit %d", len(f.queues), MaxQueues)
		}
		q = newQueue(c.Queue)
		f.queues[c.Queue] = q
	}
	q.prune(c.Now)
	if j, ok := q.jobs[c.ID]; ok {
		cp := j.clone()
		return Result{Job: &cp, Existing: true}
	}
	if q.active() >= MaxQueueJobs {
		return fail(CodeFull, "queue %q has %d active jobs, limit %d", truncate(c.Queue, 64), MaxQueueJobs, MaxQueueJobs)
	}
	q.seq++
	j := &Job{
		Queue: c.Queue, ID: c.ID, Payload: slices.Clone(c.Payload), State: JobPending, Seq: q.seq,
		MaxAttempts: attempts, EnqueuedAt: c.Now, NotBefore: addMs(c.Now, c.DelayMs),
	}
	q.jobs[c.ID] = j
	q.enqueued++
	cp := j.clone()
	return Result{Job: &cp}
}

// jobClaim hands out the oldest ready job. A job whose claim lapsed is ready
// again -- the earlier worker's token dies with it -- unless it has used its
// attempts, in which case a worker that keeps crashing mid-job dead-letters it
// rather than looping forever.
func (f *FSM) jobClaim(idx uint64, c *Command) Result {
	if r := checkName("queue", c.Queue, MaxNameLen); r != nil {
		return *r
	}
	if r := checkName("worker", c.Worker, MaxNameLen); r != nil {
		return *r
	}
	if r := checkNow(c.Now); r != nil {
		return *r
	}
	vis := c.VisibilityMs
	if vis == 0 {
		vis = DefaultVisibility.Milliseconds()
	}
	if r := checkMs("visibility_ms", vis, 1, MaxVisibility); r != nil {
		return *r
	}
	q, ok := f.queues[c.Queue]
	if !ok {
		return Result{}
	}
	var best *Job
	for _, j := range q.jobs {
		if j.State == JobClaimed && j.VisibleUntil <= c.Now && j.Attempts >= j.MaxAttempts {
			j.State = JobDead
			j.LastError = "visibility timeout on final attempt"
			j.FinishedAt, j.FinishedIndex = c.Now, idx
			j.Worker, j.Token = "", 0
			q.deadLettered++
			continue
		}
		if j.Ready(time.Unix(0, c.Now)) && (best == nil || j.Seq < best.Seq) {
			best = j
		}
	}
	if best == nil {
		return Result{}
	}
	if best.State == JobClaimed {
		q.reclaims++
	}
	best.State = JobClaimed
	best.Attempts++
	best.Worker = c.Worker
	best.Token = idx
	best.ClaimedAt = c.Now
	best.VisibleUntil = addMs(c.Now, vis)
	q.claims++
	cp := best.clone()
	return Result{Job: &cp}
}

func (f *FSM) jobComplete(idx uint64, c *Command) Result {
	q, j, r := f.job(c)
	if r != nil {
		return *r
	}
	if j.State == JobCompleted && j.Token == c.Token {
		cp := j.clone()
		return Result{Job: &cp, Duplicate: true}
	}
	if r := claimedBy(j, c); r != nil {
		return *r
	}
	j.State = JobCompleted
	j.FinishedAt, j.FinishedIndex = c.Now, idx
	q.completions++
	q.prune(c.Now)
	cp := j.clone()
	return Result{Job: &cp}
}

func (f *FSM) jobFail(idx uint64, c *Command) Result {
	q, j, r := f.job(c)
	if r != nil {
		return *r
	}
	if r := claimedBy(j, c); r != nil {
		return *r
	}
	if r := checkNow(c.Now); r != nil {
		return *r
	}
	if r := checkMs("backoff_ms", c.BackoffMs, 0, MaxDelay); r != nil {
		return *r
	}
	q.failures++
	j.LastError = truncate(c.Error, MaxErrorLen)
	if j.Attempts >= j.MaxAttempts {
		j.State = JobDead
		j.FinishedAt, j.FinishedIndex = c.Now, idx
		j.Worker, j.Token = "", 0
		q.deadLettered++
		cp := j.clone()
		return Result{Job: &cp, DeadLettered: true}
	}
	backoff := c.BackoffMs
	if backoff == 0 {
		backoff = RetryBackoffBase.Milliseconds()
		for i := 1; i < j.Attempts && backoff < RetryBackoffMax.Milliseconds(); i++ {
			backoff *= 2
		}
		backoff = min(backoff, RetryBackoffMax.Milliseconds())
	}
	j.State = JobPending
	j.NotBefore = addMs(c.Now, backoff)
	j.Worker, j.Token, j.VisibleUntil = "", 0, 0
	cp := j.clone()
	return Result{Job: &cp}
}

func (f *FSM) jobExtend(_ uint64, c *Command) Result {
	_, j, r := f.job(c)
	if r != nil {
		return *r
	}
	if r := claimedBy(j, c); r != nil {
		return *r
	}
	if r := checkNow(c.Now); r != nil {
		return *r
	}
	if r := checkMs("visibility_ms", c.VisibilityMs, 1, MaxVisibility); r != nil {
		return *r
	}
	j.VisibleUntil = addMs(c.Now, c.VisibilityMs)
	cp := j.clone()
	return Result{Job: &cp}
}

func (f *FSM) jobRedrive(_ uint64, c *Command) Result {
	q, j, r := f.job(c)
	if r != nil {
		return *r
	}
	if r := checkNow(c.Now); r != nil {
		return *r
	}
	if j.State != JobDead {
		return fail(CodeConflict, "job %q is %s, not dead", truncate(j.ID, 64), j.State)
	}
	q.seq++
	j.State, j.Seq, j.Attempts = JobPending, q.seq, 0
	j.NotBefore, j.FinishedAt, j.FinishedIndex = c.Now, 0, 0
	cp := j.clone()
	return Result{Job: &cp}
}

func (f *FSM) jobPurge(_ uint64, c *Command) Result {
	q, j, r := f.job(c)
	if r != nil {
		return *r
	}
	if j.State != JobDead && j.State != JobCompleted {
		return fail(CodeConflict, "job %q is %s; only dead or completed jobs can be purged", truncate(j.ID, 64), j.State)
	}
	delete(q.jobs, j.ID)
	cp := j.clone()
	return Result{Job: &cp}
}
