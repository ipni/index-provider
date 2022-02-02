// Package chunker provides functionality for chunking entries chain generated from
// provider.MultihashIterator, represented as EntriesChunker interface. The package provides a
// default implementation of this interface, CachedEntriesChunker.
//
// CachedEntriesChunker stores a cache of generated entries chains with configurable capacity and
// maximum chunk size. This cache guarantees that a cached chain of entries is either fully cached
// or not at all. This includes chains that may have overlapping section. In this case, the
// overlapping section is not evicted from the cache until the larger chain it is overlapping with
// is evicted. The CachedEntriesChunker also supports restoring previously cached entries upon
// instantiation.
//
// See: CachedEntriesChunker, NewCachedEntriesChunker
package chunker
