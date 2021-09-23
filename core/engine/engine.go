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
	dssync "github.com/ipfs/go-datastore/sync"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-ipld-prime"

	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	crypto "github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/host"
	peer "github.com/libp2p/go-libp2p-core/peer"
	mh "github.com/multiformats/go-multihash"

	legs "github.com/filecoin-project/go-legs"
)

var log = logging.Logger("provider/engine")

var _ core.Interface = &Engine{}

// NOTE: Consider including this constant as config or options
// in provider so they are conigurable.
const (
	// metadataProtocol identifies the protocol used by provider
	// to encode metadata.
	metadataProtocol = 0
	// MaxCidsInChunk number of entries to include in each chunk
	// of ingestion linked list.
	MaxCidsInChunk = 100
)

var (
	// ErrNoCallback is thrown when no callback has been defined.
	ErrNoCallback = errors.New("no callback was registered in indexer")
)

// Engine is an implementation of the core reference provider interface.
type Engine struct {
	// Provider's privateKey
	privKey crypto.PrivKey
	// Host running the provider process.
	host host.Host
	// Main linksystem used for reference provider.
	lsys ipld.LinkSystem
	// Cache used to track the linked lists through
	// ingestion.
	cachelsys ipld.LinkSystem
	cache     datastore.Batching
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
		cache:       dssync.MutexWrap(datastore.NewMapDatastore()),
	}

	e.cachelsys = e.cacheLinkSystem()
	e.lsys = e.mkLinkSystem()
	e.lt, err = legs.MakeLegTransport(context.Background(), host, ds, e.lsys, pubSubTopic)
	if err != nil {
		log.Errorf("Error initializing leg transport: %s", err)
		return nil, err
	}
	e.lp, err = legs.NewPublisher(ctx, e.lt)
	if err != nil {
		log.Errorf("Error initializing publisher in engine: %s", err)
		return nil, err
	}
	return e, nil
}

// NewFromConfig creates a reference provider engine with the corresponding config.
func NewFromConfig(ctx context.Context, cfg config.Config, ds datastore.Batching, host host.Host) (*Engine, error) {
	log.Debugw("Starting new reference provider engine")
	privKey, err := cfg.Identity.DecodePrivateKey("")
	if err != nil {
		log.Errorf("Error decoding private key from provider: %s", err)
		return nil, err
	}
	return New(ctx, privKey, host, ds, cfg.Ingest.PubSubTopic)
}

func (e *Engine) PublishLocal(ctx context.Context, adv schema.Advertisement) (cid.Cid, error) {
	log.Debugf("Publishing advertisement locally")
	// Advertisement are published in datastore by the linkSystem when links are generated.
	// The linksystem of the reference provider determines how to persist the advertisement.
	adLnk, err := schema.AdvertisementLink(e.lsys, adv)
	if err != nil {
		log.Errorf("Error generating advertisement link: %s", err)
		return cid.Undef, err
	}

	c := adLnk.ToCid()
	// Store latest advertisement published from the chain.
	// NOTE: The datastore should be thread-safe, if not
	// we need a lock to protect races on this value.
	err = e.putLatestAdv(c.Bytes())
	if err != nil {
		log.Errorf("Error storing latest advertisement in blockstore: %s", err)
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
		log.Errorf("Failed to publish advertisement locally: %s", err)
		return cid.Undef, err
	}

	log.Debugf("Publishing advertisement with Cid (%s) in pubsub channel", c)
	// Use legPublisher to publish the advertisement.
	return c, e.lp.UpdateRoot(ctx, c)
}

func (e *Engine) PushAdv(ctx context.Context, indexer peer.ID, adv schema.Advertisement) error {
	// TODO: Waiting for libp2p interface for advertisement push.
	panic("not implemented")
}

func (e *Engine) Push(ctx context.Context, indexer peer.ID, h mh.Multihash, metadata []byte) error {
	log.Debugf("Pushing metadata for multihash (%s) to indexer", h)
	cl, err := icl.NewIngest(ctx, e.host, indexer)
	if err != nil {
		log.Errorf("Ingest client to indexer could not be initialized: %s", err)
		return err
	}

	// TODO: We should change the interface in sotretheindex so it doesn't use
	// its specific config structure here to call the function.
	skbytes, err := crypto.MarshalPrivateKey(e.privKey)
	if err != nil {
		return err
	}
	cfg := sticfg.Identity{PeerID: e.host.ID().String(), PrivKey: base64.StdEncoding.EncodeToString(skbytes)}
	return cl.IndexContent(ctx, cfg, h, metadataProtocol, metadata)
}

// Registers new Cid callback to go from deal.ID to list of cids for the linksystem.
func (e *Engine) RegisterCidCallback(cb core.CidCallback) {
	log.Debugf("Registering callback in engine")
	e.cblk.Lock()
	defer e.cblk.Unlock()
	e.cb = cb
}

func (e *Engine) NotifyPut(ctx context.Context, key core.LookupKey, metadata []byte) (cid.Cid, error) {
	log.Debugf("NotifyPut for lookup key %s", string(key))
	// Publishes an advertisement into the gossipsub channel. The callback
	// must have been registered for the linkSystem to know how to go from
	// lookupKey to list of CIDs.
	return e.publishAdvForIndex(ctx, key, metadata, false)
}

func (e *Engine) NotifyRemove(ctx context.Context, key core.LookupKey, metadata []byte) (cid.Cid, error) {
	log.Debugf("NotifyRemove for lookup key %s", string(key))
	return e.publishAdvForIndex(ctx, key, metadata, true)
}

func (e *Engine) Close(ctx context.Context) error {
	e.lp.Close()
	return e.lt.Close(ctx)
}

func (e *Engine) GetAdv(ctx context.Context, c cid.Cid) (schema.Advertisement, error) {
	log.Debugf("Getting advertisement with cid %s", c)
	l, err := schema.LinkAdvFromCid(c).AsLink()
	if err != nil {
		log.Errorf("Error getting Advertisement link from its CID (%s): %s", c, err)
		return nil, err
	}

	lsys := e.vanillaLinkSystem()
	n, err := lsys.Load(ipld.LinkContext{}, l, schema.Type.Advertisement)
	if err != nil {
		log.Errorf("Error loading advertisement from blockstore with vanilla lsys: %s", err)
		return nil, err
	}
	adv, ok := n.(schema.Advertisement)
	if !ok {
		log.Errorf("stored IPLD node for cid (%s) not of type advertisement", c)
		return nil, errors.New("stored IPLD node not of advertisement type")
	}
	return adv, nil
}

func (e *Engine) GetLatestAdv(ctx context.Context) (cid.Cid, schema.Advertisement, error) {
	log.Debugf("Getting latest advertisement with cid %s")
	latestAdv, err := e.getLatestAdv()
	if err != nil {
		log.Errorf("Failed to fetch latest advertisement from blockstore: %s", err)
		return cid.Undef, nil, err
	}
	ad, err := e.GetAdv(ctx, latestAdv)
	if err != nil {
		log.Errorf("Latest advertisement couldn't be retrieved from blockstore using its CID: %s", err)
		return cid.Undef, nil, err
	}
	return latestAdv, ad, nil
}

func (e *Engine) publishAdvForIndex(ctx context.Context, key core.LookupKey, metadata []byte, isRm bool) (cid.Cid, error) {
	var err error
	var cidsLnk cidlink.Link

	// If no callback registered return error
	if e.cb == nil {
		log.Errorf("No callback defined in engine")
		return cid.Undef, ErrNoCallback
	}

	// If we are not removing, we need to generate the link for the list
	// of CIDs from the lookup key using the callback, and store the relationship
	if !isRm {
		// Call the callback
		chmhs, cherr := e.cb(key)
		// And generate the linked list ipld.Link that will be added
		// to the advertisement and used for ingestion.
		// We don't want to store anything here, thus the noStoreLsys.
		lnk, err := generateChunks(noStoreLinkSystem(), chmhs, cherr, MaxCidsInChunk)
		if err != nil {
			log.Errorf("Error generating link for linked list structure from list of CIDs for key (%s): %s", string(key), err)
			return cid.Undef, err
		}
		cidsLnk = lnk.(cidlink.Link)

		// Store the relationship between lookupKey and CID
		// of the advertised list of Cids.
		err = e.putKeyCidMap(key, cidsLnk.Cid)
		if err != nil {
			log.Errorf("Couldn't set mapping between lookup key and CID of linked list (%s): %s", string(key), err)
			return cid.Undef, err
		}
	} else {
		// If we are removing, we already know the relationship
		// key-cid of the list, so we can add it right away in
		// the advertisement.
		c, err := e.getKeyCidMap(key)
		if err != nil {
			log.Errorf("Couldn't get mapping between lookup key and CID of linked list (%s): %s", string(key), err)
			return cid.Undef, err
		}
		cidsLnk = cidlink.Link{Cid: c}
		// And if we are removing it means we probably don't
		// have the list of CIDs anymore, so we can remove the
		// entry from the datastore.
		err = e.deleteKeyCidMap(key)
		if err != nil {
			log.Errorf("Failed deleting Key-Cid map for lookup key (%s): %s", string(key), err)
			return cid.Undef, err
		}
		err = e.deleteCidKeyMap(c)
		if err != nil {
			log.Errorf("Failed deleting Cid-Key map for lookup cid (%s): %s", c, err)
			return cid.Undef, err
		}
	}

	// Get the latest advertisement that we generated.
	latestAdvID, err := e.getLatestAdv()
	if err != nil {
		log.Errorf("Couldn't get latest advertisement: %s", err)
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
			log.Errorf("Error generating link from latest advertisement: %s", err)
			return cid.Undef, err
		}
		previousLnk = nb.Build().(schema.Link_Advertisement)
	}

	adv, err := schema.NewAdvertisement(e.privKey, previousLnk, cidsLnk,
		metadata, isRm, e.host.ID().String())
	if err != nil {
		log.Errorf("Error generating new advertisement: %s", err)
		return cid.Undef, err
	}
	return e.Publish(ctx, adv)
}
