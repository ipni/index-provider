package core

import (
	"context"

	"github.com/filecoin-project/go-indexer-core"
	schema "github.com/filecoin-project/storetheindex/api/v0/ingest/schema"
	"github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

// Advertisement type
type Advertisement schema.Advertisement

// Interface for a reference provider
type Interface interface {
	// PublishLocal provides a new advertisement locally.
	PublishLocal(ctx context.Context, ad Advertisement) error

	// Publish advertisement to indexer pubsub channel
	// Every advertisement published to the pubsub channel
	// is also provided locally.
	Publish(ctx context.Context, ad Advertisement) error

	// PushAdv pushes a new advertisement to a specific indexer
	PushAdv(ctx context.Context, indexer peer.ID, ad Advertisement) error

	// Push an update for a single entry.
	// This can be used to perform updates for a small number of CIDs
	// When a full advertisement is not worth it (web3.storage case).
	// Indexer may only accept pushes from authenticated providers.
	Push(ctx context.Context, indexer peer.ID, cid cid.Cid, val indexer.Value)

	// Put new content in provider's local index. Putting new content
	// in the index triggers the generation of a new advertisement.
	Put(cids []cid.Cid, val indexer.Value) (bool, error)

	// GetAdv gets an advertisement by CID from local storage.
	GetAdv(id cid.Cid) (Advertisement, error)

	// GetLatestAdv gets the latest advertisement published by provider from local storage.
	GetLatestAdv() (Advertisement, error)
}
