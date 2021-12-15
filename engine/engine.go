package engine

import (
	"context"
	"errors"
	"fmt"
	"sync"

	dt "github.com/filecoin-project/go-data-transfer"
	"github.com/filecoin-project/go-legs"
	provider "github.com/filecoin-project/index-provider"
	"github.com/filecoin-project/index-provider/config"
	stiapi "github.com/filecoin-project/storetheindex/api/v0"
	"github.com/filecoin-project/storetheindex/api/v0/ingest/schema"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/host"
)

const (
	keyToCidMapPrefix      = "map/keyCid/"
	cidToKeyMapPrefix      = "map/cidKey/"
	keyToMetadataMapPrefix = "map/keyMD/"
	latestAdvKey           = "sync/adv/"
)

var dsLatestAdvKey = datastore.NewKey(latestAdvKey)
var log = logging.Logger("provider/engine")

// Engine is an implementation of the core reference provider interface
type Engine struct {
	// privKey is the provider's privateKey
	privKey crypto.PrivKey
	// host is the libp2p host running the provider process
	host host.Host
	// dataTransfer is the data transfer module used by legs
	dataTransfer dt.Manager
	// addrs is a list of multiaddr strings which are the addresses advertised
	// for content retrieval
	addrs []string
	// lsys is the main linksystem used for reference provider
	lsys ipld.LinkSystem
	// cachelsys is used to track the linked lists through ingestion
	cachelsys ipld.LinkSystem
	cache     datastore.Datastore
	// ds is the datastore used for persistence of different assets (advertisements,
	// indexed data, etc.)
	ds        datastore.Batching
	lp        legs.LegPublisher
	pubCancel context.CancelFunc

	// pubsubtopic where the provider will push advertisements
	pubSubTopic     string
	linkedChunkSize int

	// purgeLinkCache indicates whether to purge the link cache on startup
	purgeLinkCache bool
	// linkCacheSize is the capacity of the link cache LRU
	linkCacheSize int

	// cb is the callback used in the linkSystem
	cb   provider.Callback
	cblk sync.Mutex
}

var _ provider.Interface = (*Engine)(nil)

// New creates a new engine.  The context is only used for canceling the call
// to New.
func New(ingestCfg config.Ingest, privKey crypto.PrivKey, dt dt.Manager, h host.Host, ds datastore.Batching, retAddrs []string) (*Engine, error) {
	if len(retAddrs) == 0 {
		retAddrs = []string{h.Addrs()[0].String()}
		log.Infof("Retrieval address not configured, using %s", retAddrs[0])
	}

	// TODO(security): We should not keep the privkey decoded here.
	// We should probably unlock it and lock it every time we need it.
	// Once we start encrypting the key locally.
	e := &Engine{
		host:            h,
		dataTransfer:    dt,
		ds:              ds,
		privKey:         privKey,
		pubSubTopic:     ingestCfg.PubSubTopic,
		linkedChunkSize: ingestCfg.LinkedChunkSize,
		purgeLinkCache:  ingestCfg.PurgeLinkCache,
		linkCacheSize:   ingestCfg.LinkCacheSize,

		addrs: retAddrs,
	}

	e.cachelsys = e.cacheLinkSystem()
	e.lsys = e.mkLinkSystem()

	return e, nil
}

// NewFromConfig creates a reference provider engine with the corresponding config.
func NewFromConfig(cfg config.Config, dt dt.Manager, host host.Host, ds datastore.Batching) (*Engine, error) {
	log.Info("Starting new reference provider engine")
	privKey, err := cfg.Identity.DecodePrivateKey("")
	if err != nil {
		log.Errorf("Error decoding private key from provider: %s", err)
		return nil, err
	}
	return New(cfg.Ingest, privKey, dt, host, ds, cfg.ProviderServer.RetrievalMultiaddrs)
}

func (e *Engine) Start(ctx context.Context) error {
	// Create datastore cache
	dsCache, err := newDsCache(ctx, e.ds, e.linkCacheSize)
	if err != nil {
		return err
	}

	if e.purgeLinkCache {
		dsCache.Clear()
	}

	if dsCache.Cap() > e.linkCacheSize {
		log.Infow("Link cache expanded to hold previously cached links", "new_size", dsCache.Cap())
	}

	e.cache = dsCache

	// Create a context that is used to cancel the publisher on shutdown if
	// closing it takes too long.
	var pubCtx context.Context
	pubCtx, e.pubCancel = context.WithCancel(context.Background())
	e.lp, err = legs.NewPublisherFromExisting(pubCtx, e.dataTransfer, e.host, e.pubSubTopic, e.lsys)
	if err != nil {
		log.Errorf("Error initializing publisher in engine: %s", err)
		return err
	}
	return nil
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
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			e.pubCancel()
		case <-done:
		}
	}()
	err := e.lp.Close()
	if err != nil {
		err = fmt.Errorf("error closing leg publisher: %w", err)
	}
	close(done)
	if cerr := e.cache.Close(); cerr != nil {
		log.Errorf("Error closing link cache: %s", cerr)
	}
	return err
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
		log.Errorf("Stored IPLD node for cid %q is not advertisement", c)
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

	c, err := e.getKeyCidMap(contextID)
	if err != nil {
		if err != datastore.ErrNotFound {
			log.Errorf("Could not get mapping between contextID and CID of linked list (%s): %s", string(contextID), err)
			return cid.Undef, err
		}
	}

	// If we are not removing, we need to generate the link for the list
	// of CIDs from the contextID using the callback, and store the relationship
	if !isRm {
		// If no previously-published ad for this context ID.
		if c == cid.Undef {
			log.Info("Generating linked list of CIDs for advertisement")
			// Call the callback
			mhIter, err := e.cb(ctx, contextID)
			if err != nil {
				return cid.Undef, err
			}
			// Generate the linked list ipld.Link that is added to the
			// advertisement and used for ingestion.
			lnk, err := e.generateChunks(mhIter)
			if err != nil {
				log.Errorf("Error generating link from list of CIDs for contextID (%s): %s", string(contextID), err)
				return cid.Undef, err
			}
			cidsLnk = lnk.(cidlink.Link)

			// Store the relationship between contextID and CID of the advertised
			// list of Cids.
			err = e.putKeyCidMap(contextID, cidsLnk.Cid)
			if err != nil {
				log.Errorf("Could not set mapping between contextID and CID of linked list (%s): %s", string(contextID), err)
				return cid.Undef, err
			}
		} else {
			// Lookup metadata for this contextID.
			prevMetadata, err := e.getKeyMetadataMap(contextID)
			if err != nil {
				if err != datastore.ErrNotFound {
					log.Errorf("Could not get metadata for existing chain: %s", err)
					return cid.Undef, err
				}
				log.Warn("No metadata for existing chain, generating new advertisement")
			}

			if metadata.Equal(prevMetadata) {
				// Metadata is the same; no change, no need for new advertisement.
				return cid.Undef, provider.ErrAlreadyAdvertised
			}

			// Linked list is the same, but metadata is different, so generate
			// new advertisement with same linked list, but new metadata.
			cidsLnk = cidlink.Link{Cid: c}
		}

		if err = e.putKeyMetadataMap(contextID, metadata); err != nil {
			log.Errorf("Could not set mapping between contextID (%s) and metadata: %s", string(contextID), err)
			return cid.Undef, err
		}
	} else {
		if c == cid.Undef {
			return cid.Undef, provider.ErrContextIDNotFound
		}
		log.Info("Generating removal list for advertisement")

		// If we are removing, we already know the relationship key-cid of the
		// list, so we can add it right away in the advertisement.
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
		err = e.deleteKeyMetadataMap(contextID)
		if err != nil {
			log.Errorf("Failed deleting Key-Metadata map for contextID (%s): %s", string(contextID), err)
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

func (e *Engine) putKeyCidMap(contextID []byte, c cid.Cid) error {
	// We need to store the map Key-Cid to know what CidLink to put
	// in advertisement when we notify a removal.
	err := e.ds.Put(datastore.NewKey(keyToCidMapPrefix+string(contextID)), c.Bytes())
	if err != nil {
		return err
	}
	// And the other way around when graphsync ios making a request,
	// so the callback in the linksystem knows to what contextID we are referring.
	return e.ds.Put(datastore.NewKey(cidToKeyMapPrefix+c.String()), contextID)
}

func (e *Engine) getKeyCidMap(contextID []byte) (cid.Cid, error) {
	b, err := e.ds.Get(datastore.NewKey(keyToCidMapPrefix + string(contextID)))
	if err != nil {
		return cid.Undef, err
	}
	_, d, err := cid.CidFromBytes(b)
	return d, err
}

func (e *Engine) deleteKeyCidMap(contextID []byte) error {
	return e.ds.Delete(datastore.NewKey(keyToCidMapPrefix + string(contextID)))
}

func (e *Engine) deleteCidKeyMap(c cid.Cid) error {
	return e.ds.Delete(datastore.NewKey(cidToKeyMapPrefix + c.String()))
}

func (e *Engine) getCidKeyMap(c cid.Cid) ([]byte, error) {
	return e.ds.Get(datastore.NewKey(cidToKeyMapPrefix + c.String()))
}

func (e *Engine) putKeyMetadataMap(contextID []byte, metadata stiapi.Metadata) error {
	data, err := metadata.MarshalBinary()
	if err != nil {
		return err
	}
	return e.ds.Put(datastore.NewKey(keyToMetadataMapPrefix+string(contextID)), data)
}

func (e *Engine) getKeyMetadataMap(contextID []byte) (stiapi.Metadata, error) {
	data, err := e.ds.Get(datastore.NewKey(keyToMetadataMapPrefix + string(contextID)))
	if err != nil {
		return stiapi.Metadata{}, err
	}
	var metadata stiapi.Metadata
	if err = metadata.UnmarshalBinary(data); err != nil {
		return stiapi.Metadata{}, err
	}
	return metadata, nil
}

func (e *Engine) deleteKeyMetadataMap(contextID []byte) error {
	return e.ds.Delete(datastore.NewKey(keyToMetadataMapPrefix + string(contextID)))
}

func (e *Engine) putLatestAdv(advID []byte) error {
	return e.ds.Put(dsLatestAdvKey, advID)
}

func (e *Engine) getLatestAdv() (cid.Cid, error) {
	b, err := e.ds.Get(dsLatestAdvKey)
	if err != nil {
		if err == datastore.ErrNotFound {
			return cid.Undef, nil
		}
		return cid.Undef, err
	}
	_, c, err := cid.CidFromBytes(b)
	return c, err
}
