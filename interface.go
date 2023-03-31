// Package provider represents a reference implementation of an index provider.
// It integrates with the indexer node protocol, "storetheinex" in order to advertise the
// availability of a list of multihashes as an IPLD DAG.
// For the complete advertisement IPLD schema, see:
//   - https://github.com/ipni/go-libipni/blob/main/ingest/schema/schema.ipldsch
//
// A reference implementation of provider.Interface can be found in engine.Engine.
package provider

import (
	"context"

	"github.com/ipfs/go-cid"
	"github.com/ipni/go-libipni/ingest/schema"
	"github.com/ipni/go-libipni/metadata"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multihash"
)

// Interface represents an index provider that manages the advertisement for
// multihashes to indexer nodes.
type Interface interface {
	// PublishLocal appends adv to the locally stored advertisement chain and
	// returns the corresponding CID to it.  This function does not publish the
	// changes to the advertisement chain onto gossip pubsub channel.  Use
	// Publish instead if indexer nodes must be made aware of the appended
	// advertisement.
	//
	// See: Publish.
	PublishLocal(context.Context, schema.Advertisement) (cid.Cid, error)

	// Publish appends adv to the locally stored advertisement chain, and
	// publishes the new advertisement onto gossip pubsub.  The CID returned
	// represents the ID of the advertisement appended to the chain.
	Publish(context.Context, schema.Advertisement) (cid.Cid, error)

	// RegisterMultihashLister registers the hook that is used by the provider to look up
	// a list of multihashes by context ID. Only a single registration is
	// supported; repeated calls to this function will replace the previous
	// registration.
	RegisterMultihashLister(MultihashLister)

	// NotifyPut signals the provider that the list of multihashes looked up by
	// the given provider and contextIDs is available. The given
	// provider and contextIDs are then used to look up the list of multihashes via MultihashLister.
	// An advertisement is then generated, appended to the chain of advertisements and published onto
	// the gossip pubsub channel. Advertisements for different provider IDs are placed onto the same chain.
	// Use an empty provider string for the default configured provider.
	//
	// A MultihashLister must be registered prior to using this function.
	// ErrNoMultihashLister is returned if no such lister is registered.
	//
	// The metadata is data that provides hints about how to retrieve data and
	// is protocol dependant. The metadata must at least specify a protocol
	// ID, but its data is optional.
	//
	// If provider, contextID and metadata are the same as a previous call to
	// NotifyPut, then ErrAlreadyAdvertised is returned.
	//
	// If provider is nil then the default configured provider will be assumed.
	//
	// This function returns the ID of the advertisement published.
	NotifyPut(ctx context.Context, provider *peer.AddrInfo, contextID []byte, md metadata.Metadata) (cid.Cid, error)

	// NotifyRemove signals to the provider that the multihashes that
	// corresponded to the given provider and contextID are no longer available.  An advertisement
	// is then generated, appended to the chain of advertisements and published
	// onto the gossip pubsub channel.
	// The given provider and contextID tuple must have previously been put via NotifyPut.
	// If not found ErrContextIDNotFound is returned.
	//
	// If providerID is empty then the default configured provider will be assumed.
	//
	// This function returns the ID of the advertisement published.
	NotifyRemove(ctx context.Context, providerID peer.ID, contextID []byte) (cid.Cid, error)

	// GetAdv gets the advertisement that corresponds to the given cid.
	GetAdv(context.Context, cid.Cid) (*schema.Advertisement, error)

	// GetLatestAdv gets the latest advertisement on this provider's
	// advertisement chain and the CID to which it corresponds.
	GetLatestAdv(context.Context) (cid.Cid, *schema.Advertisement, error)

	// Shutdown shuts down this provider, and blocks until all resources
	// occupied by it are discarded.  After calling this function the provider
	// is no longer available for use.
	Shutdown() error
}

// MultihashIterator iterates over a list of multihashes.
//
// See: CarMultihashIterator.
type MultihashIterator interface {
	// Next returns the next multihash in the list of mulitihashes.  The
	// iterator fails fast: errors that occur during iteration are returned
	// immediately.  This function returns a zero multihash and io.EOF when
	// there are no more elements to return.
	Next() (multihash.Multihash, error)
}

// MultihashLister lists the multihashes that correspond to a given provider and contextID.
// The lister must be deterministic: it must produce the same list of multihashes in the same
// order for the same (provider, contextID) tuple.
//
// empty provider means falling back to the default.
// See: Interface.NotifyPut, Interface.NotifyRemove, MultihashIterator.
type MultihashLister func(ctx context.Context, provider peer.ID, contextID []byte) (MultihashIterator, error)
