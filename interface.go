// Package provider represents a reference implementation of an index provider.
package provider

import (
	"context"

	stiapi "github.com/filecoin-project/storetheindex/api/v0"
	"github.com/filecoin-project/storetheindex/api/v0/ingest/schema"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
)

// Interface represents an index provider that manages the advertisement of
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

	// RegisterCallback registers the callback used by the provider to look up
	// a list of multihashes by context ID.  Only a single callback is
	// supported; repeated calls to this function will replace the previous
	// callback.
	RegisterCallback(Callback)

	// NotifyPut sginals to the provider that the list of multihashes looked up
	// by the given contextID are available.  The given contextID is then used to look up
	// the list of multihashes via Callback.  An advertisement is then
	// generated, appended to the chain of advertisements and published onto
	// the gossip pubsub channel.
	// Therefore, a Callback must be registered prior to using this function.
	// ErrNoCallback is returned if no such callback is registered.
	//
	// The metadata is data that provides hints about how to retrieve data and
	// is protocol dependant.  The metadata must at least specify a protocol
	// ID, but its data is optional.
	//
	// This function returns the ID of the advertisement published.
	NotifyPut(ctx context.Context, contextID []byte, metadata stiapi.Metadata) (cid.Cid, error)

	// NotifyRemove sginals to the provider that the multihashes that
	// corresponded to the given contextID are no longer available.  An advertisement
	// is then generated, appended to the chain of advertisements and published
	// onto the gossip pubsub channel.
	// The given contextID must have previously been put via NotifyPut.
	// If not found ErrContextIDNotFound is returned.
	//
	// This function returns the ID of the advertisement published.
	NotifyRemove(ctx context.Context, contextID []byte) (cid.Cid, error)

	// GetAdv gets the advertisement that corresponds to the given cid.
	GetAdv(context.Context, cid.Cid) (schema.Advertisement, error)

	// GetLatestAdv gets the latest advertisement on this provider's
	// advertisement chain and the CID to which it corresponds.
	GetLatestAdv(context.Context) (cid.Cid, schema.Advertisement, error)

	// Shutdown shuts down this provider, and blocks until all resources
	// occupied by it are discared.  After calling this function the provider
	// is no longer available for use.
	Shutdown(context.Context) error
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

// Callback is used by provider to look up a list of multihashes associated to
// a context ID.  The callback must produce the same list of multihashes for the same
// context ID.
//
// See: Interface.NotifyPut, Interface.NotifyRemove, MultihashIterator.
type Callback func(ctx context.Context, contextID []byte) (MultihashIterator, error)
