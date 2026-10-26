package node

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

const identityPEMType = "KVMESH ED25519 PRIVATE KEY"

// loadOrCreateIdentity returns the node's long-term Ed25519 identity, creating
// it (mode 0600) on first start. The identity is what KTP handshakes sign, so
// it must survive restarts: peers that pin identities would otherwise reject a
// restarted node as an impostor.
func loadOrCreateIdentity(path string) (ed25519.PrivateKey, error) {
	raw, err := os.ReadFile(path)
	if err == nil {
		blk, _ := pem.Decode(raw)
		if blk == nil || blk.Type != identityPEMType || len(blk.Bytes) != ed25519.SeedSize {
			return nil, fmt.Errorf("identity %s: not a %s", path, identityPEMType)
		}
		return ed25519.NewKeyFromSeed(blk.Bytes), nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	data := pem.EncodeToMemory(&pem.Block{Type: identityPEMType, Bytes: priv.Seed()})
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return nil, err
	}
	if err := os.Rename(tmp, path); err != nil {
		return nil, err
	}
	return priv, nil
}

// Fingerprint is a short, human-comparable digest of a public identity.
func Fingerprint(pub ed25519.PublicKey) string {
	sum := sha256.Sum256(pub)
	return hex.EncodeToString(sum[:8])
}
