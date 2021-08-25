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
	Push(ctx context.Context, indexer peer.ID, cid cid.Cid, metadata []byte)

	// NotifyPutCids notifies when a list of CIDs are added to the provided.
	// It generates the corresponding Index and Advertisement for the update
	// and published it locally and to the pubsub channel. The function returns
	// the CID of the advertisement.
	NotifyPutCids(ctx context.Context, cids []cid.Cid, metadata []byte) (cid.Cid, error)

	// NotifyPutCar notifies when a new CAR is added to the provider.
	// The list of CIDs inside the CAR are processed to generate a new advertisement
	// that points to the updated data.
	NotifyPutCar(ctx context.Context, carID cid.Cid, metadata []byte) (cid.Cid, error)

	// NotifyRemoveCids notifies that a list of CIDs have been removed from the provider
	// and generates and publishes the corresponding advertisement.
	NotifyRemoveCids(ctx context.Context, cids []cid.Cid, metadata []byte) (cid.Cid, error)

	// NotifyRemoveCAr notifies that a CAR has been removed from the provider
	// and generates and publishes the corresponding advertisement.
	NotifyRemoveCar(ctx context.Context, carID cid.Cid, metadata []byte) (cid.Cid, error)

	// GetAdv gets an advertisement by CID from local storage.
	GetAdv(ctx context.Context, id cid.Cid) (schema.Advertisement, error)

	// GetLatestAdv gets the latest advertisement published by provider from local storage.
	GetLatestAdv(ctx context.Context) (schema.Advertisement, error)
}
