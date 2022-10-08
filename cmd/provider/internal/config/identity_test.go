package config

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	ic "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
)

func TestDecodeOrCreate(t *testing.T) {
	// first, creating an identity and saving the key to the file
	identity := Identity{}
	privKeyPath := filepath.Join(t.TempDir(), "privKey")
	t.Setenv(PrivateKeyPathEnvVar, privKeyPath)

	peerIDBefore, pkBefore, err := identity.DecodeOrCreate(io.Discard)
	require.NoError(t, err)

	stat, err := os.Stat(privKeyPath)
	require.NoError(t, err)
	modTimeBefore := stat.ModTime()

	// verifying that once the file is there, the new identity is going to be read from it
	identityFromPrivateKey := Identity{}
	peerIDAfter, pkAfter, err := identityFromPrivateKey.DecodeOrCreate(io.Discard)
	require.NoError(t, err)

	require.Equal(t, pkBefore, pkAfter)
	require.Equal(t, peerIDBefore, peerIDAfter)

	stat, err = os.Stat(privKeyPath)
	require.NoError(t, err)

	// verifying that the file hasn't changed between runs
	require.Equal(t, modTimeBefore, stat.ModTime())

	pkb, err := os.ReadFile(privKeyPath)
	require.NoError(t, err)
	pkFromFile, err := ic.UnmarshalPrivateKey(pkb)
	require.NoError(t, err)

	// verifying that the key in the file matches the one returned by the function
	require.Equal(t, pkFromFile, pkAfter)

	peerIDFromFile, err := peer.IDFromPrivateKey(pkFromFile)
	require.NoError(t, err)

	// verifying that the identity from the file matches the one returned by the function
	require.Equal(t, peerIDFromFile, peerIDAfter)
}
