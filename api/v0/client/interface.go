package client

import (
	"context"

	"github.com/filecoin-project/indexer-reference-provider/api/v0/models"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/peer"
)

// Provider client interface
type Provider interface {
	// GetAdv gets an advertisement by CID from a provider
	GetAdv(ctx context.Context, p peer.ID, id cid.Cid) (*models.AdResponse, error)

	// GetLatestAdv gets the latest advertisement from a provider
	GetLatestAdv(ctx context.Context, p peer.ID) (*models.AdResponse, error)
}
