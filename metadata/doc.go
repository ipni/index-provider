// Package metadata captures the metadata types known by the index-provider.
// The metadata is used to provide information about how to retrieve data blocks associated to
// multihashes from a provider.
// It is represented as an array of bytes in the indexer protocol, starting with a varint ProtocolID
// that defines how to decode the remaining bytes.
//
// Two metadata types are currently represented here: BitswapMetadata and DataTransferMetadata.
package metadata
