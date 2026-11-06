package hlc

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	// LogicalBits is the width of the logical counter.
	LogicalBits = 16
	// MaxLogical is the largest logical counter value.
	MaxLogical = 1<<LogicalBits - 1
	// MaxWall is the largest representable wall time in Unix milliseconds.
	MaxWall = 1<<(64-LogicalBits) - 1
)

// Timestamp is a hybrid logical clock reading: 48 bits of Unix milliseconds
// followed by a 16-bit logical counter. Integer order is causal order.
type Timestamp uint64

// MaxTimestamp is the largest timestamp; no clock will ever produce it.
const MaxTimestamp = Timestamp(^uint64(0))

// Make builds a timestamp from its parts. wallMs is clamped to [0, MaxWall].
func Make(wallMs int64, logical uint16) Timestamp {
	if wallMs < 0 {
		wallMs = 0
	}
	if wallMs > MaxWall {
		wallMs = MaxWall
	}
	return Timestamp(uint64(wallMs)<<LogicalBits | uint64(logical))
}

// FromTime returns the timestamp for physical time t with a zero logical part.
func FromTime(t time.Time) Timestamp { return Make(t.UnixMilli(), 0) }

// Wall returns the wall-clock part in Unix milliseconds.
func (t Timestamp) Wall() int64 { return int64(uint64(t) >> LogicalBits) }

// Logical returns the logical counter.
func (t Timestamp) Logical() uint16 { return uint16(uint64(t) & MaxLogical) }

// Time returns the wall part as a time.Time (UTC).
func (t Timestamp) Time() time.Time { return time.UnixMilli(t.Wall()).UTC() }

// IsZero reports whether t is the zero timestamp.
func (t Timestamp) IsZero() bool { return t == 0 }

// Compare returns -1, 0 or +1 as t is before, equal to or after u.
func (t Timestamp) Compare(u Timestamp) int {
	switch {
	case t < u:
		return -1
	case t > u:
		return 1
	}
	return 0
}

// Before reports whether t happened before u.
func (t Timestamp) Before(u Timestamp) bool { return t < u }

// After reports whether t happened after u.
func (t Timestamp) After(u Timestamp) bool { return t > u }

// String renders "wallms.logical", e.g. "1729850000123.7". The form is chosen
// so that it round-trips through Parse and stays readable in logs; it does not
// sort lexically (use the integer for that).
func (t Timestamp) String() string {
	return strconv.FormatInt(t.Wall(), 10) + "." + strconv.FormatUint(uint64(t.Logical()), 10)
}

// MarshalText implements encoding.TextMarshaler using String's format.
func (t Timestamp) MarshalText() ([]byte, error) { return []byte(t.String()), nil }

// UnmarshalText implements encoding.TextUnmarshaler.
func (t *Timestamp) UnmarshalText(b []byte) error {
	v, err := Parse(string(b))
	if err != nil {
		return err
	}
	*t = v
	return nil
}

// ErrSyntax is returned by Parse for malformed input.
var ErrSyntax = errors.New("hlc: invalid timestamp syntax")

// Parse parses the String form "wallms.logical". A bare "wallms" means a zero
// logical part.
func Parse(s string) (Timestamp, error) {
	wallStr, logStr, hasDot := strings.Cut(s, ".")
	if wallStr == "" || (hasDot && logStr == "") {
		return 0, fmt.Errorf("%w: %q", ErrSyntax, s)
	}
	wall, err := strconv.ParseUint(wallStr, 10, 64)
	if err != nil || wall > MaxWall {
		return 0, fmt.Errorf("%w: wall part of %q", ErrSyntax, s)
	}
	var logical uint64
	if hasDot {
		logical, err = strconv.ParseUint(logStr, 10, LogicalBits)
		if err != nil {
			return 0, fmt.Errorf("%w: logical part of %q", ErrSyntax, s)
		}
	}
	return Timestamp(wall<<LogicalBits | logical), nil
}

// ErrClockOffset is returned (wrapped in an *OffsetError) when a remote
// timestamp is too far ahead of the local physical clock.
var ErrClockOffset = errors.New("hlc: remote timestamp exceeds max clock offset")

// ErrOverflow is returned when advancing the clock would exceed MaxTimestamp.
// It cannot happen with sane clocks; it exists so that a hostile peer with
// skew checking disabled can't wrap the clock to zero.
var ErrOverflow = errors.New("hlc: timestamp overflow")

// OffsetError describes a rejected remote timestamp.
type OffsetError struct {
	Remote    Timestamp
	Physical  int64 // local physical time, Unix ms
	MaxOffset time.Duration
}

func (e *OffsetError) Error() string {
	return fmt.Sprintf("hlc: remote timestamp %s is %dms ahead of local clock (max offset %s)",
		e.Remote, e.Remote.Wall()-e.Physical, e.MaxOffset)
}

// Unwrap lets errors.Is(err, ErrClockOffset) match.
func (e *OffsetError) Unwrap() error { return ErrClockOffset }

// Clock is a hybrid logical clock. It is safe for concurrent use.
type Clock struct {
	now       func() time.Time
	maxOffset time.Duration

	mu   sync.Mutex
	last Timestamp
}

// New returns a clock reading physical time from now (time.Now if nil).
// maxOffset bounds how far ahead of local physical time a remote timestamp may
// be before Update rejects it; maxOffset <= 0 disables the check.
func New(now func() time.Time, maxOffset time.Duration) *Clock {
	if now == nil {
		now = time.Now
	}
	return &Clock{now: now, maxOffset: maxOffset}
}

// MaxOffset returns the configured skew bound.
func (c *Clock) MaxOffset() time.Duration { return c.maxOffset }

// physical reads the physical clock in milliseconds, clamped to the encodable
// range so a wildly wrong clock can't corrupt the encoding.
func (c *Clock) physical() int64 {
	ms := c.now().UnixMilli()
	if ms < 0 {
		return 0
	}
	if ms > MaxWall {
		return MaxWall
	}
	return ms
}

// Now returns a timestamp for a local or send event. Successive calls return
// strictly increasing timestamps even if the physical clock stalls or steps
// backwards.
func (c *Clock) Now() Timestamp {
	pt := Make(c.physical(), 0)
	c.mu.Lock()
	defer c.mu.Unlock()
	if pt > c.last {
		c.last = pt
	} else if c.last != MaxTimestamp {
		// +1 increments the logical part; at 0xFFFF it carries into the wall
		// part, which is exactly the overflow behaviour we want.
		c.last++
	}
	return c.last
}

// Update folds a timestamp received from another node into the clock and
// returns the timestamp of the receive event, which is strictly greater than
// both remote and every timestamp this clock has issued before. A remote
// timestamp more than MaxOffset ahead of local physical time is rejected with
// an error wrapping ErrClockOffset and leaves the clock unchanged.
func (c *Clock) Update(remote Timestamp) (Timestamp, error) {
	phys := c.physical()
	if c.maxOffset > 0 && remote.Wall()-phys > c.maxOffset.Milliseconds() {
		return 0, &OffsetError{Remote: remote, Physical: phys, MaxOffset: c.maxOffset}
	}
	pt := Make(phys, 0)
	c.mu.Lock()
	defer c.mu.Unlock()
	m := c.last
	if remote > m {
		m = remote
	}
	if pt > m {
		// Physical time is ahead of everything we've seen: reset the
		// logical counter, as in the paper's "l' == pt" case.
		c.last = pt
		return c.last, nil
	}
	if m == MaxTimestamp {
		return 0, ErrOverflow
	}
	// max(l, remote) + 1 covers the paper's three tie cases at once: the
	// logical part of whichever is larger is bumped, and a full counter
	// carries into the wall part.
	c.last = m + 1
	return c.last, nil
}

// Last returns the most recent timestamp issued, without advancing the clock.
func (c *Clock) Last() Timestamp {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.last
}
