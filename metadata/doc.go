// Package metadata captures the metadata types known by the index-provider.
//
// The metadata is used to provide information about how to retrieve data blocks associated to
// multihashes advertised by a provider. It is represented as an array of bytes in the indexer
// protocol, starting with a varint ProtocolID that defines how to decode the remaining bytes.
//
// Two metadata types are currently represented here: Bitswap and
// GraphsyncFilecoinV1.
package metadata
