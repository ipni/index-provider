package engine

import (
	"context"

	"github.com/filecoin-project/go-indexer-core"
	"github.com/filecoin-project/indexer-reference-provider/config"
	"github.com/filecoin-project/indexer-reference-provider/core"
	schema "github.com/filecoin-project/storetheindex/api/v0/ingest/schema"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	crypto "github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/host"
	peer "github.com/libp2p/go-libp2p-core/peer"
	legs "github.com/willscott/go-legs"
)

var log = logging.Logger("reference-provider")

var _ core.Interface = &Engine{}

const (
	// NOTE: For now this is an IPLD-aware reference provider, wo
	// we need to force GraphSupport. This may change in the future
	// and we may pass this as an option.
	graphSupport = true
)

// Engine is an implementation of the core reference provider interface.
type Engine struct {
	// Provider's privateKey
	privKey crypto.PrivKey
	// Host running the provider process.
	host host.Host
	// Linksystem used for reference provider.
	lsys ipld.LinkSystem
	// Datastore used for persistence of different assets
	// (advertisements, indexed data, etc.).
	ds datastore.Batching
	lp legs.LegPublisher
	// pubsubtopic where the provider will push advertisements
	pubSubTopic string

	// Local indexer with all provided data.
	// NOTE: It may be the case that providers want to keep a local index with all
	// provider data, and they want to store it with go-indexer-core. In this case
	// we may need to wrap go-indexer-core so linkSystems can use it.
	//indexer indexer.Interface
}

// New creates a reference provider engine with the corresponding config.
func New(ctx context.Context,
	cfg config.Config, host host.Host, ds datastore.Batching, index indexer.Interface,
	pubSubTopic string) (*Engine, error) {

	log.Debugw("Starting new reference provider engine")
	lsys := mkLinkSystem(ds)
	lp, err := legs.NewPublisher(ctx, ds, host, pubSubTopic, lsys)
	if err != nil {
		return nil, err
	}
	// TODO(security): We shouldn't keep the privkey decoded here.
	// We should probably unlock it and lock it every time we need it.
	// Once we start encrypting the key locally.
	privKey, err := cfg.Identity.DecodePrivateKey("")
	if err != nil {
		return nil, err
	}
	return &Engine{
		host:        host,
		ds:          ds,
		lp:          lp,
		privKey:     privKey,
		pubSubTopic: pubSubTopic,
	}, nil
}

func (e *Engine) PublishLocal(ctx context.Context, adv core.Advertisement) (cid.Cid, error) {
	return e.publishLocal(ctx, adv)
}

func (e *Engine) publishLocal(ctx context.Context, adv core.Advertisement) (cid.Cid, error) {
	// Advertisement are published in datastore by the linkSystem when links are generated.
	// The linksystem of the reference provider determines how to persist the advertisement.
	adLnk, err := schema.AdvertisementLink(e.lsys, adv)
	if err != nil {
		return cid.Undef, err
	}
	lnk, err := adLnk.AsLink()
	if err != nil {
		return cid.Undef, err
	}
	c := lnk.(cidlink.Link).Cid
	id, err := adv.ID.AsBytes()
	if err != nil {
		return cid.Undef, err
	}
	// Store latest advertisement published from the chain.
	// NOTE: The datastore should be thread-safe, if not
	// we need a lock to protect races on this value.
	err = e.putLatestAdv(id)
	if err != nil {
		return cid.Undef, err
	}
	return c, nil
}

func (e *Engine) Publish(ctx context.Context, adv core.Advertisement) error {
	// First publish the advertisement locally.
	// Advertisements are stored immutably, so it doesn't matter
	// if we try to publish the same advertisement twice, we will just
	// propagate the announcement again through the pubsub channel.
	c, err := e.publishLocal(ctx, adv)
	if err != nil {
		return err
	}

	// Use legPublisher to publish the advertisement.
	return e.lp.UpdateRoot(ctx, c)
}

func (e *Engine) PushAdv(ctx context.Context, indexer peer.ID, adv core.Advertisement) error {
	panic("not implemented")
}

func (e *Engine) Push(ctx context.Context, indexer peer.ID, cid cid.Cid, metadata []byte) {
	panic("not implemented")
}

func (e *Engine) PutEvent(ctx context.Context, cids []cid.Cid, metadata []byte) error {
	latestAdvID, err := e.getLatestAdv()
	if err != nil {
		return err
	}
	latestIndexLink, err := e.getLatestIndexLink()
	if err != nil {
		return err
	}
	// Lsys will store the index conveniently here.
	_, indexLnk, err := schema.NewSingleEntryIndex(e.lsys, cids, nil, metadata, latestIndexLink)
	if err != nil {
		return err
	}
	// Update the latest index
	iLnk, err := indexLnk.AsLink()
	if err != nil {
		return err
	}
	err = e.putLatestIndex(iLnk.(cidlink.Link).Cid)
	if err != nil {
		return err
	}
	adv, err := schema.NewAdvertisement(e.privKey, latestAdvID, indexLnk, e.host.ID().String(), graphSupport)

	return e.Publish(ctx, adv)
}

func (e *Engine) RemoveEvent(ctx context.Context, cids []cid.Cid, metadata []byte) error {
	panic("not implemented")
}

func (e *Engine) GetAdv(id cid.Cid) (core.Advertisement, error) {
	panic("not implemented")
}

func (e *Engine) GetLatestAdv() (core.Advertisement, error) {
	panic("not implemented")
}
