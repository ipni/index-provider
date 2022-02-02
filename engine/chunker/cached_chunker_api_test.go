package chunker

import (
	"context"

	"github.com/ipld/go-ipld-prime"
)

// CountOverlap is exposed for testing purposes only.
func (ls *CachedEntriesChunker) CountOverlap(ctx context.Context, link ipld.Link) (uint64, error) {
	return ls.countOverlap(ctx, link)
}
