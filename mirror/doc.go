// Package mirror provides the ability to replicate the advertisement chain from an existing
// provider with options to restructure the advertisement entries into schema.EntryChunk or HAMT.
//
// A Mirror is configurable via a set of options to control the generation of mirrored advertisements.
// It can be configured to simply replicate the original advertisement chain without any changes,
// explicitly re-sign the advertisement with the Mirror's identity, or restructure the advertisement
// entries entirely. Note that any change to the structure of advertisement will require the ad to
// be re-signed as the original signature will no longer be valid.
//
// A Mirror will also act as a CDN for the original advertisement chain by exposing a dagsync.Publisher
// over GraphSync. The endpoint enables an indexer node to fetch the content associated with the
// original chain of advertisement as well as the mirrored advertisement chain which may be
// different.
//
// Upon starting a Mirror, when no prior mirrored advertisements exist, the initial mirroring
// recursion depth is set to unlimited. When the initial limit is set to a value smaller than the
// total number of available advertisements the very first mirrored advertisement will preserve the
// original PreviousID link, even though the content corresponding to that link will not be hosted
// by the mirror.
//
// Note that mirroring advertisements is one-to-one: for each original advertisement there will be
// a mirrored one. This is not affected by optional remapping of entries. Future work will provide
// the ability to also remap advertisements in addition to entries.
package mirror
