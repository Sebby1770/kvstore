// Package rpc is request/response messaging over KTP streams: one call per
// stream, so calls never head-of-line block each other and a slow handler
// holds up nothing but itself.
//
// # Wire format
//
// The caller writes one request frame and keeps its write side open; the
// server answers with zero or more DATA frames and then END, or with ERR, and
// half-closes. The caller then half-closes too, which retires the stream.
//
//	request   1B version (1) | varint len + method | varint deadline (µs remaining, 0 = none)
//	          | varint n | n × (varint len + key, varint len + value) | varint len + payload
//	response  repeated 1B kind | varint len | body
//	          kind 1 DATA (payload), 2 END (empty), 3 ERR (varint code, then message)
//
// Keeping the caller's write side open is what makes cancellation work: if
// the caller gives up it resets the stream, the server's watcher sees the
// reset (rather than a normal EOF) and cancels the handler's context.
//
// Deadlines travel as time remaining, not as an absolute time, so callers
// and servers need not agree on the clock. Status codes mirror gRPC's. A
// handler that panics answers Internal instead of taking the process down.
package rpc

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/Sebby1770/kvstore/kvgw/netstack/wire"
)

// Code is a call status, numbered like gRPC's.
type Code uint32

const (
	OK                 Code = 0
	Canceled           Code = 1
	Unknown            Code = 2
	InvalidArgument    Code = 3
	DeadlineExceeded   Code = 4
	NotFound           Code = 5
	AlreadyExists      Code = 6
	PermissionDenied   Code = 7
	ResourceExhausted  Code = 8
	FailedPrecondition Code = 9
	Aborted            Code = 10
	OutOfRange         Code = 11
	Unimplemented      Code = 12
	Internal           Code = 13
	Unavailable        Code = 14
	DataLoss           Code = 15
	Unauthenticated    Code = 16
)

var codeNames = [...]string{"ok", "canceled", "unknown", "invalid_argument", "deadline_exceeded", "not_found",
	"already_exists", "permission_denied", "resource_exhausted", "failed_precondition", "aborted",
	"out_of_range", "unimplemented", "internal", "unavailable", "data_loss", "unauthenticated"}

func (c Code) String() string {
	if int(c) < len(codeNames) {
		return codeNames[c]
	}
	return fmt.Sprintf("code(%d)", uint32(c))
}

// Error is a call failure with a status code. It survives the wire intact.
type Error struct {
	Code    Code
	Message string
}

func (e *Error) Error() string { return "rpc " + e.Code.String() + ": " + e.Message }

// Errorf builds an *Error.
func Errorf(code Code, format string, args ...any) error {
	return &Error{Code: code, Message: fmt.Sprintf(format, args...)}
}

// CodeOf classifies any error: nil is OK, *Error carries its code, context
// errors map to Canceled / DeadlineExceeded, everything else is Unknown.
func CodeOf(err error) Code {
	var e *Error
	switch {
	case err == nil:
		return OK
	case errors.As(err, &e):
		return e.Code
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, os.ErrDeadlineExceeded):
		return DeadlineExceeded
	case errors.Is(err, context.Canceled):
		return Canceled
	}
	return Unknown
}

// toError converts a handler's error into a wire error.
func toError(err error) *Error {
	var e *Error
	if errors.As(err, &e) {
		return e
	}
	return &Error{Code: CodeOf(err), Message: err.Error()}
}

// DefaultMaxMessage bounds one request or response frame.
const DefaultMaxMessage = 4 << 20

const (
	protoVersion = 1
	kindData     = 1
	kindEnd      = 2
	kindErr      = 3
	maxMethodLen = 256
	maxMetadata  = 64
)

type request struct {
	method   string
	deadline uint64 // µs remaining
	md       map[string]string
	payload  []byte
}

func appendBytes(b, v []byte) []byte {
	return append(wire.AppendVarint(b, uint64(len(v))), v...)
}

func (r request) encode() []byte {
	b := []byte{protoVersion}
	b = appendBytes(b, []byte(r.method))
	b = wire.AppendVarint(b, r.deadline)
	b = wire.AppendVarint(b, uint64(len(r.md)))
	for k, v := range r.md {
		b = appendBytes(b, []byte(k))
		b = appendBytes(b, []byte(v))
	}
	return appendBytes(b, r.payload)
}

func readVarint(r *bufio.Reader) (uint64, error) {
	first, err := r.ReadByte()
	if err != nil {
		return 0, err
	}
	n := 1 << (first >> 6)
	buf := []byte{first}
	for i := 1; i < n; i++ {
		c, err := r.ReadByte()
		if err != nil {
			return 0, noEOF(err)
		}
		buf = append(buf, c)
	}
	v, _, err := wire.ParseVarint(buf)
	return v, err
}

func noEOF(err error) error {
	if err == io.EOF {
		return io.ErrUnexpectedEOF
	}
	return err
}

func readBytes(r *bufio.Reader, max int) ([]byte, error) {
	n, err := readVarint(r)
	if err != nil {
		return nil, noEOF(err)
	}
	if n > uint64(max) {
		return nil, Errorf(ResourceExhausted, "frame of %d bytes exceeds the %d-byte limit", n, max)
	}
	b := make([]byte, n)
	if _, err := io.ReadFull(r, b); err != nil {
		return nil, noEOF(err)
	}
	return b, nil
}

func readRequest(r *bufio.Reader, maxMsg int) (request, error) {
	var req request
	v, err := r.ReadByte()
	if err != nil {
		return req, err
	}
	if v != protoVersion {
		return req, Errorf(Unimplemented, "rpc protocol version %d", v)
	}
	m, err := readBytes(r, maxMethodLen)
	if err != nil {
		return req, err
	}
	req.method = string(m)
	if req.deadline, err = readVarint(r); err != nil {
		return req, noEOF(err)
	}
	n, err := readVarint(r)
	if err != nil {
		return req, noEOF(err)
	}
	if n > maxMetadata {
		return req, Errorf(ResourceExhausted, "%d metadata entries", n)
	}
	if n > 0 {
		req.md = make(map[string]string, n)
		for i := uint64(0); i < n; i++ {
			k, err := readBytes(r, 1024)
			if err != nil {
				return req, err
			}
			val, err := readBytes(r, 64<<10)
			if err != nil {
				return req, err
			}
			req.md[string(k)] = string(val)
		}
	}
	req.payload, err = readBytes(r, maxMsg)
	return req, err
}

func appendFrame(b []byte, kind byte, body []byte) []byte {
	return appendBytes(append(b, kind), body)
}

func errFrame(e *Error) []byte {
	body := wire.AppendVarint(nil, uint64(e.Code))
	body = append(body, e.Message...)
	return appendFrame(nil, kindErr, body)
}

// readFrame reads one response frame.
func readFrame(r *bufio.Reader, maxMsg int) (kind byte, body []byte, err error) {
	kind, err = r.ReadByte()
	if err != nil {
		return 0, nil, err
	}
	body, err = readBytes(r, maxMsg)
	return kind, body, err
}

func parseErr(body []byte) *Error {
	code, n, err := wire.ParseVarint(body)
	if err != nil {
		return &Error{Code: DataLoss, Message: "malformed error frame"}
	}
	return &Error{Code: Code(code), Message: string(body[n:])}
}
