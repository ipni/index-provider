// Package chunker provides functionality for chunking ad entries generated from
// provider.MultihashIterator into an IPLD DAG. The interface given a multihash iterator an
// EntriesChunker drains it, restructures the multihashes in an IPLD DAG and returns the root link
// to that DAG. Two DAG datastructures are currently implemented: ChainChunker, and HamtChunker.
// Additionally, CachedEntriesChunker can use either of the chunkers and provide an LRU caching
// functionality for the generated DAGs.
//
// See: CachedEntriesChunker, ChainChunker, HamtChunker
package chunker
