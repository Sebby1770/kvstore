//go:build !unix

package fabric

import "errors"

// MapFile is unavailable on this platform; use heap regions and remote verbs.
func MapFile(path string, size int) (*Region, error) {
	return nil, errors.New("fabric: shared mappings need a unix platform")
}
