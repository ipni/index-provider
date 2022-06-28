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

// LinkSystem is exposed for testing purposes only.
func (ls *CachedEntriesChunker) LinkSystem() ipld.LinkSystem {
	return ls.lsys
}

// RootPrefixedDSKey is exposed for testing purposes only.
func RootPrefixedDSKey(l ipld.Link) datastore.Key {
	return rootKeyPrefix.Child(dsKey(l))
}
