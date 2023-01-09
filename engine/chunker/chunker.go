package chunker

import (
	"context"

	"github.com/ipld/go-ipld-prime"
	provider "github.com/ipni/index-provider"
)

// EntriesChunker chunks multihashes supplied by a given provider.MultihashIterator into a chain of
// schema.EntryChunk.
type EntriesChunker interface {
	// Chunk chunks multihashes supplied by a given provider.MultihashIterator into a chain of
	// schema.EntryChunk and returns the link of the chain root.
	// If the given iterator has no elements, this function returns a nil link with no error.
	Chunk(context.Context, provider.MultihashIterator) (ipld.Link, error)
}
