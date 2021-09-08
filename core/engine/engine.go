package engine

import (
	"context"
	"encoding/base64"
	"errors"
	"sync"

	"github.com/filecoin-project/indexer-reference-provider/config"
	"github.com/filecoin-project/indexer-reference-provider/core"
	icl "github.com/filecoin-project/storetheindex/api/v0/ingest/client/libp2p"
	schema "github.com/filecoin-project/storetheindex/api/v0/ingest/schema"
	sticfg "github.com/filecoin-project/storetheindex/config"
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
	// metadataProtocol identifies the protocol used by provider
	// to encode metadata.
	metadataProtocol = 0
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
	lt *legs.LegTransport
	// pubsubtopic where the provider will push advertisements
	pubSubTopic string

	// Callback used in the linkSystem
	cblk sync.Mutex
	cb   core.CidCallback
}

// New creates a new engine
func New(ctx context.Context,
	privKey crypto.PrivKey, host host.Host, ds datastore.Batching,
	pubSubTopic string) (*Engine, error) {

	var err error
	// TODO(security): We shouldn't keep the privkey decoded here.
	// We should probably unlock it and lock it every time we need it.
	// Once we start encrypting the key locally.
	e := &Engine{
		host:        host,
		ds:          ds,
		privKey:     privKey,
		pubSubTopic: pubSubTopic,
	}
	e.lsys = e.mkLinkSystem(ds)
	e.lt, err = legs.MakeLegTransport(context.Background(), host, ds, e.lsys, pubSubTopic)
	if err != nil {
		return nil, err
	}
	e.lp, err = legs.NewPublisher(ctx, e.lt)
	if err != nil {
		return nil, err
	}
	return e, nil
}

// NewFromConfig creates a reference provider engine with the corresponding config.
func NewFromConfig(ctx context.Context, cfg config.Config, ds datastore.Batching, host host.Host) (*Engine, error) {
	log.Debugw("Starting new reference provider engine")
	privKey, err := cfg.Identity.DecodePrivateKey("")
	if err != nil {
		return nil, err
	}
	return New(ctx, privKey, host, ds, cfg.Ingest.PubSubTopic)
}

func (e *Engine) PublishLocal(ctx context.Context, adv schema.Advertisement) (cid.Cid, error) {
	// Advertisement are published in datastore by the linkSystem when links are generated.
	// The linksystem of the reference provider determines how to persist the advertisement.
	adLnk, err := schema.AdvertisementLink(e.lsys, adv)
	if err != nil {
		return cid.Undef, err
	}

	c := adLnk.ToCid()
	// Store latest advertisement published from the chain.
	// NOTE: The datastore should be thread-safe, if not
	// we need a lock to protect races on this value.
	err = e.putLatestAdv(c.Bytes())
	if err != nil {
		return cid.Undef, err
	}
	return c, nil
}

func (e *Engine) Publish(ctx context.Context, adv schema.Advertisement) (cid.Cid, error) {
	// First publish the advertisement locally.
	// Advertisements are stored immutably, so it doesn't matter
	// if we try to publish the same advertisement twice, we will just
	// propagate the announcement again through the pubsub channel.
	c, err := e.PublishLocal(ctx, adv)
	if err != nil {
		return cid.Undef, err
	}

	// Use legPublisher to publish the advertisement.
	return c, e.lp.UpdateRoot(ctx, c)
}

func (e *Engine) PushAdv(ctx context.Context, indexer peer.ID, adv schema.Advertisement) error {
	// TODO: Waiting for libp2p interface for advertisement push.
	panic("not implemented")
}

func (e *Engine) Push(ctx context.Context, indexer peer.ID, cid cid.Cid, metadata []byte) error {
	cl, err := icl.NewIngest(ctx, e.host, indexer)
	if err != nil {
		return err
	}

	// TODO: We should change the interface in sotretheindex so it doesn't use
	// its specific config structure here to call the function.
	skbytes, err := crypto.MarshalPrivateKey(e.privKey)
	if err != nil {
		return err
	}
	cfg := sticfg.Identity{PeerID: e.host.ID().String(), PrivKey: base64.StdEncoding.EncodeToString(skbytes)}
	return cl.IndexContent(ctx, cfg, cid, metadataProtocol, metadata)
}

// Registers new Cid callback to go from deal.ID to list of cids for the linksystem.
func (e *Engine) RegisterCidCallback(cb core.CidCallback) {
	e.cblk.Lock()
	defer e.cblk.Unlock()
	e.cb = cb
}

func (e *Engine) NotifyPut(ctx context.Context, dealID cid.Cid, metadata []byte) (cid.Cid, error) {
	// Publishes an advertisement into the gossipsub channel. The callback
	// must have been registered for the linkSystem to know how to go from
	// dealID to list of CIDs.
	return e.publishAdvForIndex(ctx, dealID, metadata, false)
}

func (e *Engine) NotifyRemove(ctx context.Context, dealID cid.Cid, metadata []byte) (cid.Cid, error) {
	return e.publishAdvForIndex(ctx, dealID, metadata, true)
}

func (e *Engine) Close(ctx context.Context) error {
	e.lp.Close()
	return e.lt.Close(ctx)
}

func (e *Engine) GetAdv(ctx context.Context, c cid.Cid) (schema.Advertisement, error) {
	l, err := schema.LinkAdvFromCid(c).AsLink()
	if err != nil {
		return nil, err
	}

	n, err := e.lsys.Load(ipld.LinkContext{}, l, schema.Type.Advertisement)
	if err != nil {
		return nil, err
	}
	adv, ok := n.(schema.Advertisement)
	if !ok {
		return nil, errors.New("stored IPLD node not of advertisement type")
	}
	return adv, nil
}

func (e *Engine) GetLatestAdv(ctx context.Context) (cid.Cid, schema.Advertisement, error) {
	latestAdv, err := e.getLatest(false)
	if err != nil {
		return cid.Undef, nil, err
	}
	ad, err := e.GetAdv(ctx, latestAdv)
	if err != nil {
		return cid.Undef, nil, err
	}
	return latestAdv, ad, nil
}

func (e *Engine) publishAdvForIndex(ctx context.Context, dealID cid.Cid, metadata []byte, isRm bool) (cid.Cid, error) {
	// TODO: We should probably prevent providers from being able to advertise
	// the same index several times. It may lead to a lot of duplicate retrievals?
	latestAdvID, err := e.getLatest(false)
	if err != nil {
		return cid.Undef, err
	}
	var previousLnk schema.Link_Advertisement
	// NOTE: We need to check if we are getting cid.Undef for
	// the previous link, if this is the case we're bump into a
	// cid too short error in IPLD links serialization.
	if latestAdvID == cid.Undef {
		previousLnk = nil
	} else {
		nb := schema.Type.Link_Advertisement.NewBuilder()
		err = nb.AssignLink(cidlink.Link{Cid: latestAdvID})
		if err != nil {
			return cid.Undef, err
		}
		previousLnk = nb.Build().(schema.Link_Advertisement)
	}

	err = e.putLatestIndex(dealID)
	if err != nil {
		return cid.Undef, err
	}
	adv, err := schema.NewAdvertisement(e.privKey, previousLnk, cidlink.Link{Cid: dealID},
		metadata, isRm, e.host.ID().String())
	if err != nil {
		return cid.Undef, err
	}
	return e.Publish(ctx, adv)
}
