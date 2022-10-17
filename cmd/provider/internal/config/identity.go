package config

import (
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"os"

	ic "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
)

const (
	PrivateKeyPathEnvVar = "INDEXPROVIDER_PRIV_KEY_PATH"
)

// Identity tracks the configuration of the local node's identity.
type Identity struct {
	PeerID  string
	PrivKey string `json:",omitempty"`
}

func (identity Identity) DecodeOrCreate(out io.Writer) (peer.ID, ic.PrivKey, error) {

	privKey, err := identity.DecodeOrCreatePrivateKey(out, "")
	if err != nil {
		return "", nil, fmt.Errorf("could not decode private key: %s", err)
	}

	peerIDFromPrivKey, err := peer.IDFromPrivateKey(privKey)
	if err != nil {
		return "", nil, fmt.Errorf("could not decode peer id: %s", err)
	}

	// If peer ID is specified in JSON config, then verify that it is:
	//   1. a valid peer ID, and
	//   2. consistent with the peer ID generated from private key.
	if identity.PeerID != "" {
		peerID, err := peer.Decode(identity.PeerID)
		if err != nil {
			return "", nil, fmt.Errorf("could not decode peer id: %w", err)
		}

		if peerID != "" && peerIDFromPrivKey != peerID {
			return "", nil, fmt.Errorf("provided peer ID must either match the peer ID generated from private key or be omitted: expected %s but got %s", peerIDFromPrivKey, peerID)
		}
	}

	return peerIDFromPrivKey, privKey, nil
}

// DecodeOrCreatePrivateKey is a helper to decode the user's PrivateKey. If the key hasn't been provided in json config
// then it's going to be read from PrivateKeyPathEnvVar. If that file doesn't exist then a new key is going to be generated and saved there.
func (identity Identity) DecodeOrCreatePrivateKey(out io.Writer, passphrase string) (ic.PrivKey, error) {
	if identity.PrivKey == "" {
		pkb, err := loadPrivKeyFromFile(out)
		if err != nil {
			return nil, err
		}
		return ic.UnmarshalPrivateKey(pkb)
	}

	pkb, err := base64.StdEncoding.DecodeString(identity.PrivKey)
	if err != nil {
		return nil, err
	}
	return ic.UnmarshalPrivateKey(pkb)
}

func loadPrivKeyFromFile(out io.Writer) ([]byte, error) {
	privKeyPath := os.Getenv(PrivateKeyPathEnvVar)
	if privKeyPath == "" {
		return nil, fmt.Errorf("private key not specified; it must be specified either in config or via %s env var", PrivateKeyPathEnvVar)
	}

	// If the file with key doesn't exist - generate a new key and save it to the file
	if _, err := os.Stat(privKeyPath); errors.Is(err, os.ErrNotExist) {
		pk, err := generateAndSavePrivKey(out, privKeyPath)
		if err != nil {
			return nil, err
		}
		return pk, nil
	}
	pkb, err := os.ReadFile(privKeyPath)
	if err != nil {
		return nil, err
	}
	return pkb, nil
}

func generateAndSavePrivKey(out io.Writer, filePath string) ([]byte, error) {
	identity, err := CreateIdentity(out)
	if err != nil {
		return nil, err
	}

	f, err := os.Create(filePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	pkb, err := base64.StdEncoding.DecodeString(identity.PrivKey)
	if err != nil {
		return nil, err
	}

	_, err = f.Write(pkb)
	return pkb, err
}
