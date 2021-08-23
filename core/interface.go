package core

import (
	"context"

	schema "github.com/filecoin-project/storetheindex/api/v0/ingest/schema"
	"github.com/ipfs/go-cid"
	peer "github.com/libp2p/go-libp2p-core/peer"
)

// Advertisement type
type Advertisement schema.Advertisement

// AdvLink type
type AdvLink schema.Link_Advertisement

// Index type
type Index schema.Index

// IndexLink type
type IndexLink schema.Link_Index

// Interface for a reference provider
type Interface interface {
	// PublishLocal provides a new advertisement locally.
	// It returns the CID that can be used to uniquely
	// identify the advertisement.
	PublishLocal(ctx context.Context, adv Advertisement) (cid.Cid, error)

	// Publish advertisement to indexer pubsub channel
	// Every advertisement published to the pubsub channel
	// is also provided locally.
	Publish(ctx context.Context, adv Advertisement) error

	// PushAdv pushes a new advertisement to a specific indexer
	PushAdv(ctx context.Context, indexer peer.ID, adv Advertisement) error

	// Push an update for a single entry.
	// This can be used to perform updates for a small number of CIDs
	// When a full advertisement is not worth it (web3.storage case).
	// Indexer may only accept pushes from authenticated providers.
	Push(ctx context.Context, indexer peer.ID, cid cid.Cid, metadata []byte)

	// PutEvent notifies that new data has been added to the provider
	NotifyPut(ctx context.Context, cids []cid.Cid, metadata []byte) error

	// RemoveEvent sends an event to the reference provider to notify
	// that new data has been removed from the provider
	NotifyRemoved(ctx context.Context, cids []cid.Cid, metadata []byte) error

	// GetAdv gets an advertisement by CID from local storage.
	GetAdv(ctx context.Context, id cid.Cid) (Advertisement, error)

	// GetLatestAdv gets the latest advertisement published by provider from local storage.
	GetLatestAdv(ctx context.Context) (Advertisement, error)
}
