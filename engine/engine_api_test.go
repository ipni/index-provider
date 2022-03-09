package engine

import (
	"testing"

	"github.com/filecoin-project/index-provider/engine/chunker"
	"github.com/ipld/go-ipld-prime"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/host"
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

// LinkSystem returns the engine's linksystem, exposed for testing purposes only.
func (e *Engine) LinkSystem() *ipld.LinkSystem {
	return &e.lsys
}

func Test_EmptyConfigSetsDefaults(t *testing.T) {
	engine, err := New()
	require.NoError(t, err)
	require.True(t, engine.entChunkSize > 0)
	require.True(t, engine.entCacheCap > 0)
	require.True(t, engine.pubTopicName != "")
}
