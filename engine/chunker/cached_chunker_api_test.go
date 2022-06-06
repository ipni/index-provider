package chunker

import (
	"context"

	"github.com/ipfs/go-datastore"
	"github.com/ipld/go-ipld-prime"
)

// CountOverlap is exposed for testing purposes only.
func (ls *CachedEntriesChunker) CountOverlap(ctx context.Context, link ipld.Link) (uint64, error) {
	return ls.countOverlap(ctx, link)
}

// DSKey is exposed for testing purposes only.
func DSKey(l ipld.Link) datastore.Key {
	return dsKey(l)
}
