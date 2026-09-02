// Package proto encodes/decodes the binary wire protocol spoken by kvnode.
// See kvnode/src/proto.rs for the canonical reference. Every value carries an
// 8-byte version so the gateway can resolve conflicts with last-writer-wins.
// Native TTL is an 8-byte expires_at (unix seconds, 0 = never).
package proto

import (
	"encoding/binary"
	"fmt"
	"io"
)

const (
	OpGet     byte = 1
	OpPut     byte = 2
	OpDel     byte = 3
	OpScan    byte = 4
	OpPing    byte = 5
	OpStat    byte = 6
	OpPutX    byte = 7
	OpScanAll byte = 8

	StatusOK       byte = 0
	StatusNotFound byte = 1
	StatusError    byte = 2
	StatusDeleted  byte = 3

	ScanTerminator uint32 = 0xFFFFFFFF
)

func WriteU8(w io.Writer, v byte) error {
	_, err := w.Write([]byte{v})
	return err
}

func WriteU32(w io.Writer, v uint32) error {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], v)
	_, err := w.Write(b[:])
	return err
}

func WriteU64(w io.Writer, v uint64) error {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], v)
	_, err := w.Write(b[:])
	return err
}

func WriteBytes(w io.Writer, b []byte) error {
	_, err := w.Write(b)
	return err
}

func ReadU8(r io.Reader) (byte, error) {
	var b [1]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return 0, err
	}
	return b[0], nil
}

func ReadU32(r io.Reader) (uint32, error) {
	var b [4]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(b[:]), nil
}

func ReadU64(r io.Reader) (uint64, error) {
	var b [8]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(b[:]), nil
}

func ReadBytes(r io.Reader, n uint32) ([]byte, error) {
	if n == 0 {
		return []byte{}, nil
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	return buf, nil
}

func StatusString(s byte) string {
	switch s {
	case StatusOK:
		return "ok"
	case StatusNotFound:
		return "not_found"
	case StatusError:
		return "error"
	case StatusDeleted:
		return "deleted"
	default:
		return fmt.Sprintf("unknown(%d)", s)
	}
}
