package engine

import (
	"context"
	"errors"
	"sync"

	dt "github.com/filecoin-project/go-data-transfer"
	"github.com/filecoin-project/go-legs"
	provider "github.com/filecoin-project/indexer-reference-provider"
	"github.com/filecoin-project/indexer-reference-provider/config"
	stiapi "github.com/filecoin-project/storetheindex/api/v0"
	"github.com/filecoin-project/storetheindex/api/v0/ingest/schema"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/host"
)

var log = logging.Logger("provider/engine")

var _ provider.Interface = (*Engine)(nil)

// TODO: Consider including this constant as config or options
// in provider so they are conigurable.

// maxIngestChunk is the maximum number of entries to include in each chunk of ingestion linked list.
const maxIngestChunk = 100

// Engine is an implementation of the core reference provider interface
type Engine struct {
	// privKey is the provider's privateKey
	privKey crypto.PrivKey
	// host is the libp2p host running the provider process
	host host.Host
	// addrs is a list of multiaddr strings which are the addresses advertised
	// for content retrieval
	addrs []string
	// lsys is the main linksystem used for reference provider
	lsys ipld.LinkSystem
	// cachelsys is used to track the linked lists through ingestion
	cachelsys ipld.LinkSystem
	cache     datastore.Batching
	// ds is the datastore used for persistence of different assets (advertisements,
	// indexed data, etc.)
	ds datastore.Batching
	lp legs.LegPublisher
	// pubsubtopic where the provider will push advertisements
	pubSubTopic string

	// cb is the callback used in the linkSystem
	cb   provider.Callback
	cblk sync.Mutex
}

// New creates a new engine
func New(ctx context.Context, privKey crypto.PrivKey, dt dt.Manager, h host.Host, ds datastore.Batching, pubSubTopic string, addrs []string) (*Engine, error) {
	if len(addrs) == 0 {
		addrs = []string{h.Addrs()[0].String()}
		log.Infof("Retrieval address not configured, using %s", addrs[0])
	}

	var err error
	// TODO(security): We should not keep the privkey decoded here.
	// We should probably unlock it and lock it every time we need it.
	// Once we start encrypting the key locally.
	e := &Engine{
		host:        h,
		ds:          ds,
		privKey:     privKey,
		pubSubTopic: pubSubTopic,
		cache:       dssync.MutexWrap(datastore.NewMapDatastore()),
		addrs:       addrs,
	}

	e.cachelsys = e.cacheLinkSystem()
	e.lsys = e.mkLinkSystem()
	e.lp, err = legs.NewPublisherFromExisting(ctx, dt, h, pubSubTopic, e.lsys)
	if err != nil {
		log.Errorf("Error initializing publisher in engine: %s", err)
		return nil, err
	}
	return e, nil
}

// NewFromConfig creates a reference provider engine with the corresponding config.
func NewFromConfig(ctx context.Context, cfg config.Config, dt dt.Manager, host host.Host, ds datastore.Batching) (*Engine, error) {
	log.Info("Starting new reference provider engine")
	privKey, err := cfg.Identity.DecodePrivateKey("")
	if err != nil {
		log.Errorf("Error decoding private key from provider: %s", err)
		return nil, err
	}
	return New(ctx, privKey, dt, host, ds, cfg.Ingest.PubSubTopic, cfg.ProviderServer.RetrievalMultiaddrs)
}

// PublishLocal stores the advertisement in the local datastore.
//
// Advertisements are stored in datastore by the linkSystem when links are
// generated.  The linksystem of the reference provider determines how to
// persist the advertisement.
func (e *Engine) PublishLocal(ctx context.Context, adv schema.Advertisement) (cid.Cid, error) {
	adLnk, err := schema.AdvertisementLink(e.lsys, adv)
	if err != nil {
		log.Errorf("Error generating advertisement link: %s", err)
		return cid.Undef, err
	}

	c := adLnk.ToCid()
	// Store latest advertisement published from the chain
	//
	// NOTE: The datastore should be thread-safe, if not we need a lock to
	// protect races on this value.
	log.Infow("Storing advertisement locally", "cid", c.String())
	err = e.putLatestAdv(c.Bytes())
	if err != nil {
		log.Errorf("Error storing latest advertisement in blockstore: %s", err)
		return cid.Undef, err
	}
	return c, nil
}

func (e *Engine) Publish(ctx context.Context, adv schema.Advertisement) (cid.Cid, error) {
	// Store the advertisement locally.
	c, err := e.PublishLocal(ctx, adv)
	if err != nil {
		log.Errorf("Failed to publish advertisement locally: %s", err)
		return cid.Undef, err
	}

	log.Infow("Publishing advertisement in pubsub channel", "cid", c.String())
	// Use legPublisher to publish the advertisement.
	return c, e.lp.UpdateRoot(ctx, c)
}

func (e *Engine) RegisterCallback(cb provider.Callback) {
	log.Debugf("Registering callback in engine")
	e.cblk.Lock()
	defer e.cblk.Unlock()
	e.cb = cb
}

func (e *Engine) NotifyPut(ctx context.Context, contextID []byte, metadata stiapi.Metadata) (cid.Cid, error) {
	log.Debugf("NotifyPut for context ID %s", string(contextID))
	// The callback must have been registered for the linkSystem to know how to
	// go from contextID to list of CIDs.
	return e.publishAdvForIndex(ctx, contextID, metadata, false)
}

func (e *Engine) NotifyRemove(ctx context.Context, contextID []byte) (cid.Cid, error) {
	log.Debugf("NotifyRemove for contextID %s", string(contextID))
	return e.publishAdvForIndex(ctx, contextID, stiapi.Metadata{}, true)
}

func (e *Engine) Shutdown(ctx context.Context) error {
	return e.lp.Close()
}

func (e *Engine) GetAdv(ctx context.Context, c cid.Cid) (schema.Advertisement, error) {
	log.Infow("Getting advertisement", "cid", c)
	l, err := schema.LinkAdvFromCid(c).AsLink()
	if err != nil {
		log.Errorf("Error getting Advertisement link from its CID %q: %s", c, err)
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
		log.Errorf("Stored IPLD node for cid %q", c)
		return nil, errors.New("stored IPLD node not of advertisement type")
	}
	return adv, nil
}

func (e *Engine) GetLatestAdv(ctx context.Context) (cid.Cid, schema.Advertisement, error) {
	log.Info("Getting latest advertisement")
	latestAdv, err := e.getLatestAdv()
	if err != nil {
		log.Errorf("Failed to fetch latest advertisement from blockstore: %s", err)
		return cid.Undef, nil, err
	}
	ad, err := e.GetAdv(ctx, latestAdv)
	if err != nil {
		log.Errorf("Latest advertisement could not be retrieved from blockstore using its CID: %s", err)
		return cid.Undef, nil, err
	}
	return latestAdv, ad, nil
}

func (e *Engine) publishAdvForIndex(ctx context.Context, contextID []byte, metadata stiapi.Metadata, isRm bool) (cid.Cid, error) {
	var err error
	var cidsLnk cidlink.Link

	// If no callback registered return error
	if e.cb == nil {
		log.Error("No callback defined in engine")
		return cid.Undef, provider.ErrNoCallback
	}

	// If we are not removing, we need to generate the link for the list
	// of CIDs from the contextID using the callback, and store the relationship
	if !isRm {
		log.Info("Generating linked list of CIDs for advertisement")
		// Call the callback
		mhIter, err := e.cb(ctx, contextID)
		if err != nil {
			return cid.Undef, err
		}
		// Generate the linked list ipld.Link that is added to the
		// advertisement and used for ingestion.  We do not want to store
		// anything here, thus the noStoreLsys.
		lnk, err := generateChunks(noStoreLinkSystem(), mhIter, maxIngestChunk)
		if err != nil {
			log.Errorf("Error generating link for linked list structure from list of CIDs for contextID (%s): %s", string(contextID), err)
			return cid.Undef, err
		}
		cidsLnk = lnk.(cidlink.Link)

		// Store the relationship between contextID and CID of the advertised
		// list of Cids
		err = e.putKeyCidMap(contextID, cidsLnk.Cid)
		if err != nil {
			log.Errorf("Could not set mapping between contextID and CID of linked list (%s): %s", string(contextID), err)
			return cid.Undef, err
		}
	} else {
		log.Info("Generating removal list for advertisement")
		// If we are removing, we already know the relationship key-cid of the
		// list, so we can add it right away in the advertisement.
		c, err := e.getKeyCidMap(contextID)
		if err != nil {
			log.Errorf("Could not get mapping between contextID and CID of linked list (%s): %s", string(contextID), err)
			if err == datastore.ErrNotFound {
				return cid.Undef, provider.ErrContextIDNotFound
			}
			return cid.Undef, err
		}
		cidsLnk = cidlink.Link{Cid: c}
		// And if we are removing it means we probably do not have the list of
		// CIDs anymore, so we can remove the entry from the datastore.
		err = e.deleteKeyCidMap(contextID)
		if err != nil {
			log.Errorf("Failed deleting Key-Cid map for contextID (%s): %s", string(contextID), err)
			return cid.Undef, err
		}
		err = e.deleteCidKeyMap(c)
		if err != nil {
			log.Errorf("Failed deleting Cid-Key map for lookup cid (%s): %s", c, err)
			return cid.Undef, err
		}
	}

	// Get the latest advertisement that was generated
	latestAdvID, err := e.getLatestAdv()
	if err != nil {
		log.Errorf("Could not get latest advertisement: %s", err)
		return cid.Undef, err
	}
	var previousLnk schema.Link_Advertisement
	// Check for cid.Undef for the previous link. If this is the case, then
	// this means there is a "cid too short" error in IPLD links serialization.
	if latestAdvID == cid.Undef {
		log.Warn("Latest advertisement CID was undefined")
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
		contextID, metadata, isRm, e.host.ID().String(), e.addrs)
	if err != nil {
		log.Errorf("Error generating new advertisement: %s", err)
		return cid.Undef, err
	}
	return e.Publish(ctx, adv)
}
