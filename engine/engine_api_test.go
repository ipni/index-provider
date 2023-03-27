package engine

import (
	"testing"

	"github.com/ipfs/go-datastore"
	"github.com/ipni/index-provider/engine/chunker"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
)

// Host returns the host on which the engine is started, exposed for testing purposes only.
func (e *Engine) Host() host.Host {
	return e.h
}

// Chunker returns the entries chunker used by the engine, exposed for testing purposes only.
func (e *Engine) Chunker() *chunker.CachedEntriesChunker {
	return e.entriesChunker
}

// Key returns the engine's private key, exposed for testing purposes only.
func (e *Engine) Key() crypto.PrivKey {
	return e.key
}

// ProviderID returns the engine's default provider ID, exposed for testing purposes only.
func (e *Engine) ProviderID() peer.ID {
	return e.provider.ID
}

// ProviderAddrs returns the engine's default provider addresses, exposed for testing purposes only.
func (e *Engine) ProviderAddrs() []string {
	return e.retrievalAddrsAsString()
}

// Datastore returns the engine's datastore, exposed for testing purposes only.
func (e *Engine) Datastore() datastore.Datastore {
	return e.ds
}

func Test_EmptyConfigSetsDefaults(t *testing.T) {
	engine, err := New()
	require.NoError(t, err)
	require.NotNil(t, engine.chunker)
	require.True(t, engine.entCacheCap > 0)
	require.True(t, engine.pubTopicName != "")
}
