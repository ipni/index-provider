package chunker

import (
	"context"

	provider "github.com/filecoin-project/index-provider"
	"github.com/ipld/go-ipld-prime"
)

// EntriesChunker chunks multihashes supplied by a given provider.MultihashIterator into a chain of
// schema.EntryChunk.
type EntriesChunker interface {
	// Chunk chunks multihashes supplied by a given provider.MultihashIterator into a chain of
	// schema.EntryChunk and returns the link of the chain root.
	Chunk(context.Context, provider.MultihashIterator) (ipld.Link, error)
}
