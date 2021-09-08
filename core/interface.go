package core

import (
	"context"

	schema "github.com/filecoin-project/storetheindex/api/v0/ingest/schema"
	"github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

// Interface for a reference provider
type Interface interface {
	// PublishLocal provides a new advertisement locally.
	// It returns the CID that can be used to uniquely
	// identify the advertisement.
	PublishLocal(ctx context.Context, adv schema.Advertisement) (cid.Cid, error)

	// Publish advertisement to indexer using the indexer pubsub channel
	// Every advertisement published to the pubsub channel
	// is also provided locally.
	Publish(ctx context.Context, adv schema.Advertisement) (cid.Cid, error)

	// PushAdv pushes a new advertisement to a specific indexer
	PushAdv(ctx context.Context, indexer peer.ID, adv schema.Advertisement) error

	// Push an update for a single entry.
	// This can be used to perform updates for a small number of CIDs
	// When a full advertisement is not worth it (web3.storage case).
	// Indexer may only accept pushes from authenticated providers.
	Push(ctx context.Context, indexer peer.ID, cid cid.Cid, metadata []byte) error

	// Registers new Cid callback to go from deal.ID to list of cids for the linksystem.
	// We currently only support one callback, so registering twice overwrites the
	// previous callback. In the future we can think of a system that allows the
	// use of different (or even conditional) callbacks.
	RegisterCidCallback(cb CidCallback)

	// NotifyPut notifies the reference provider to generate a new advertisement
	// including Cids in dealID. It returns the Cid of the generated advertisement.
	NotifyPut(ctx context.Context, dealID cid.Cid, metadata []byte) (cid.Cid, error)

	// NotifyRemove notifies the reference provider to generate a new advertisement
	// including Cids in dealID. It returns the Cid of the generated advertisement.
	NotifyRemove(ctx context.Context, dealID cid.Cid, metadata []byte) (cid.Cid, error)

	// GetAdv gets an advertisement by CID from local storage.
	GetAdv(ctx context.Context, id cid.Cid) (schema.Advertisement, error)

	// GetLatestAdv gets the latest advertisement published by provider from local storage.
	GetLatestAdv(ctx context.Context) (cid.Cid, schema.Advertisement, error)

	// Close
	Close(ctx context.Context) error
}

// CidCallback specifies the logic to go from dealID (indexID)
// to list of CIDs that will be used by the linksystem while
// traversing the DAG
type CidCallback func(dealID cid.Cid) ([]cid.Cid, error)
