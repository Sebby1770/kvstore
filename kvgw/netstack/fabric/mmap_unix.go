//go:build unix

package fabric

import (
	"fmt"
	"os"
	"syscall"
)

// MapFile maps size bytes of the file at path (created and extended as
// needed) MAP_SHARED and registers nothing yet: it returns the region so the
// caller can Register it. Every process that maps the same file sees the
// same memory, coherently, through ordinary loads, stores and atomics -- the
// CXL-style shared fabric for processes on one host.
func MapFile(path string, size int) (*Region, error) {
	if size <= 0 {
		return nil, fmt.Errorf("fabric: map size must be positive")
	}
	size = (size + 4095) &^ 4095
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if st.Size() < int64(size) {
		if err := f.Truncate(int64(size)); err != nil {
			return nil, err
		}
	}
	mem, err := syscall.Mmap(int(f.Fd()), 0, size, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return nil, fmt.Errorf("fabric: mmap %s: %w", path, err)
	}
	return &Region{mem: mem, unmap: func() error { return syscall.Munmap(mem) }}, nil
}
