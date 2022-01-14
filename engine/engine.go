package engine

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"sync"

	dt "github.com/filecoin-project/go-data-transfer"
	"github.com/filecoin-project/go-legs"
	"github.com/filecoin-project/go-legs/dtsync"
	"github.com/filecoin-project/go-legs/httpsync"
	provider "github.com/filecoin-project/index-provider"
	"github.com/filecoin-project/index-provider/cardatatransfer"
	"github.com/filecoin-project/index-provider/config"
	"github.com/filecoin-project/index-provider/engine/lrustore"
	stiapi "github.com/filecoin-project/storetheindex/api/v0"
	"github.com/filecoin-project/storetheindex/api/v0/ingest/schema"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dsn "github.com/ipfs/go-datastore/namespace"
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
	linksCachePath         = "/cache/links"
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
	publisher legs.Publisher

	httpPublisherCfg *config.HttpPublisher

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

// New creates a new index provider Engine as the default implementation of provider.Interface.
// It provides the ability to advertise the availability of a list of multihashes associated to
// a context ID as a chain of linked advertisements as defined by the indexer node protocol implemented by "storetheindex".
// Engine internally uses "go-legs", a protocol for propagating and synchronizing changes an IPLD DAG, to publish advertisements.
// See:
//  - https://github.com/filecoin-project/storetheindex
//  - https://github.com/filecoin-project/go-legs
//
// Published advertisements are signed using the given private key.
// The retAddrs corresponds to the endpoints at which the data block associated to the advertised
// multihashes can be retrieved.
// Note that if no retAddrs is specified the listen addresses of the given libp2p host are used.
//
// The engine also provides the ability to generate advertisements via Engine.NotifyPut and
// Engine.NotifyRemove as long as a provider.Callback is registered.
// See: provider.Callback, Engine.RegisterCallback.
//
// The engine must be started via Engine.Start before use and discarded via Engine.Shutdown when no longer needed.
// See: Engine.Start, Engine.Shutdown.
func New(ingestCfg config.Ingest, privKey crypto.PrivKey, dt dt.Manager, h host.Host, ds datastore.Batching, retAddrs []string) (*Engine, error) {
	if len(retAddrs) == 0 {
		retAddrs = []string{h.Addrs()[0].String()}
		log.Infof("Retrieval address not configured, using %s", retAddrs[0])
	}

	ingestCfg.PopulateDefaults()

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

		addrs:            retAddrs,
		httpPublisherCfg: &ingestCfg.HttpPublisher,
	}

	e.cachelsys = e.cacheLinkSystem()
	e.lsys = e.mkLinkSystem()

	return e, nil
}

// NewFromConfig instantiates a new engine by using the given config.Config.
// The instantiation gets the private key via config.Identity, and uses RetrievalMultiaddrs in
// config.ProviderServer as retrieval addresses in advertisements.
//
// See: engine.New .
func NewFromConfig(cfg config.Config, dt dt.Manager, host host.Host, ds datastore.Batching) (*Engine, error) {
	log.Info("Instantiating a new index provider engine")
	privKey, err := cfg.Identity.DecodePrivateKey("")
	if err != nil {
		log.Errorw("Error decoding private key from provider", "err", err)
		return nil, err
	}
	return New(cfg.Ingest, privKey, dt, host, ds, cfg.ProviderServer.RetrievalMultiaddrs)
}

// Start starts the engine by instantiating the internal storage and joins the configured gossipsub
// topic used for publishing advertisements.
//
// The context is used to instantiate the internal LRU cache storage.
//
// See: Engine.Shutdown, lrustore.New, dtsync.NewPublisherFromExisting.
func (e *Engine) Start(ctx context.Context) error {
	// Create datastore cache
	dsCache, err := lrustore.New(ctx, dsn.Wrap(e.ds, datastore.NewKey(linksCachePath)), e.linkCacheSize)
	if err != nil {
		return err
	}

	if e.purgeLinkCache {
		dsCache.Clear(ctx)
	}

	if dsCache.Cap() > e.linkCacheSize {
		log.Infow("Link cache expanded to hold previously cached links", "new_size", dsCache.Cap())
	}

	e.cache = dsCache

	if e.httpPublisherCfg.Enabled {
		var addr string
		addr, err = e.httpPublisherCfg.ListenNetAddr()
		if err != nil {
			log.Errorw("Error forming http addr in engine for httpPublisher:", "err", err)
			return err
		}
		e.publisher, err = httpsync.NewPublisher(addr, e.lsys, e.host.ID(), e.privKey)
	} else {
		e.publisher, err = dtsync.NewPublisherFromExisting(e.dataTransfer, e.host, e.pubSubTopic, e.lsys)
	}

	if err != nil {
		log.Errorw("Error initializing publisher in engine:", "err", err)
		return err
	}

	return nil
}

// PublishLocal stores the advertisement in the local link system and marks it locally as the latest
// advertisement.
//
// The context is used for storing internal mapping information onto the datastore.
//
// See: Engine.Publish.
func (e *Engine) PublishLocal(ctx context.Context, adv schema.Advertisement) (cid.Cid, error) {
	adLnk, err := schema.AdvertisementLink(e.lsys, adv)
	if err != nil {
		log.Errorw("Error generating advertisement link", "err", err)
		return cid.Undef, err
	}

	c := adLnk.ToCid()
	// Store latest advertisement published from the chain
	//
	// NOTE: The datastore should be thread-safe, if not we need a lock to
	// protect races on this value.
	log.Infow("Storing advertisement locally", "cid", c.String())
	err = e.putLatestAdv(ctx, c.Bytes())
	if err != nil {
		log.Errorw("Error storing latest advertisement in blockstore", "err", err)
		return cid.Undef, err
	}
	return c, nil
}

// Publish stores the given advertisement locally via Engine.PublishLocal first, then publishes
// a message onto the gossipsub to signal the change in the latest advertisement by the provider to
// indexer nodes.
//
// The publication mechanism uses legs.Publisher internally.
// See: https://github.com/filecoin-project/go-legs
func (e *Engine) Publish(ctx context.Context, adv schema.Advertisement) (cid.Cid, error) {
	// Store the advertisement locally.
	c, err := e.PublishLocal(ctx, adv)
	if err != nil {
		log.Errorw("Failed to publish advertisement locally", "err", err)
		return cid.Undef, err
	}

	log.Infow("Publishing advertisement in pubsub channel", "cid", c.String())
	// Use legPublisher to publish the advertisement.
	err = e.publisher.UpdateRoot(ctx, c)
	if err != nil {
		return cid.Undef, err
	}
	return c, nil
}

// RegisterCallback registers a new provider.Callback that is used to look up the list of multihashes
// associated to a context ID.
// At least one such callback must be registered before calls to Engine.NotifyPut and Engine.NotifyRemove.
//
// Note that successive calls to this function will replace the previous callback.
// Only a single callback is supported.
//
// See: provider.Interface
func (e *Engine) RegisterCallback(cb provider.Callback) {
	log.Debugf("Registering callback in engine")
	e.cblk.Lock()
	defer e.cblk.Unlock()
	e.cb = cb
}

// NotifyPut publishes an advertisement that signals the list of multihashes associated to the given
// contextID is available by this provider with the given metadata.
//
// Note that prior to calling this function a provider.Callback must be registered.
//
// See: Engine.RegisterCallback, Engine.Publish.
func (e *Engine) NotifyPut(ctx context.Context, contextID []byte, metadata stiapi.Metadata) (cid.Cid, error) {
	// The callback must have been registered for the linkSystem to know how to
	// go from contextID to list of CIDs.
	return e.publishAdvForIndex(ctx, contextID, metadata, false)
}

// NotifyRemove publishes an advertisement that signals the list of multihashes associated to the given
// contextID is no longer available by this provider.
//
// Note that prior to calling this function a provider.Callback must be registered.
//
// See: Engine.RegisterCallback, Engine.Publish.
func (e *Engine) NotifyRemove(ctx context.Context, contextID []byte) (cid.Cid, error) {
	return e.publishAdvForIndex(ctx, contextID, stiapi.Metadata{}, true)
}

// Shutdown shuts down the engine and discards all resources opened by the engine.
// The engine is no longer usable after the call to this function.
func (e *Engine) Shutdown() error {
	err := e.publisher.Close()
	if err != nil {
		err = fmt.Errorf("error closing leg publisher: %w", err)
	}
	if cerr := e.cache.Close(); cerr != nil {
		log.Errorw("Error closing link cache", "err", cerr)
	}

	return err
}

// GetAdv gets the advertisement associated to the given cid c.
// The context is not used.
func (e *Engine) GetAdv(_ context.Context, c cid.Cid) (schema.Advertisement, error) {
	log.Infow("Getting advertisement", "cid", c)
	l, err := schema.LinkAdvFromCid(c).AsLink()
	if err != nil {
		log.Errorw("Error getting Advertisement link from its CID", "cid", c, "err", err)
		return nil, err
	}

	lsys := e.vanillaLinkSystem()
	n, err := lsys.Load(ipld.LinkContext{}, l, schema.Type.Advertisement)
	if err != nil {
		log.Errorw("Error loading advertisement from blockstore with vanilla lsys", "err", err)
		return nil, err
	}
	adv, ok := n.(schema.Advertisement)
	if !ok {
		log.Errorw("Stored IPLD node for cid is not advertisement", "cid", c)
		return nil, errors.New("stored IPLD node not of advertisement type")
	}
	return adv, nil
}

// GetLatestAdv gets the latest advertisement by the provider.
// Note that the latest advertisement may or may not have been published onto the gossipsub topic.
//
// The context is used to retrieve date from the local datastore.
func (e *Engine) GetLatestAdv(ctx context.Context) (cid.Cid, schema.Advertisement, error) {
	log.Info("Getting latest advertisement")
	latestAdv, err := e.getLatestAdv(ctx)
	if err != nil {
		log.Errorw("Failed to fetch latest advertisement from blockstore", "err", err)
		return cid.Undef, nil, err
	}
	ad, err := e.GetAdv(ctx, latestAdv)
	if err != nil {
		log.Errorw("Latest advertisement could not be retrieved from blockstore by CID", "err", err)
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

	log := log.With("contextID", base64.StdEncoding.EncodeToString(contextID))

	c, err := e.getKeyCidMap(ctx, contextID)
	if err != nil {
		if err != datastore.ErrNotFound {
			log.Errorw("Could not get mapping between contextID and CID of linked list", "err", err)
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
				log.Errorw("Error generating link from list of CIDs", "err", err)
				return cid.Undef, err
			}
			cidsLnk = lnk.(cidlink.Link)

			// Store the relationship between contextID and CID of the advertised
			// list of Cids.
			err = e.putKeyCidMap(ctx, contextID, cidsLnk.Cid)
			if err != nil {
				log.Errorw("Could not set mapping between contextID and CID of linked list", "err", err)
				return cid.Undef, err
			}
		} else {
			// Lookup metadata for this contextID.
			prevMetadata, err := e.getKeyMetadataMap(ctx, contextID)
			if err != nil {
				if err != datastore.ErrNotFound {
					log.Errorw("Could not get metadata for existing chain", "err", err)
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

		if err = e.putKeyMetadataMap(ctx, contextID, metadata); err != nil {
			log.Errorw("Could not set mapping between contextID and metadata", "err", err)
			return cid.Undef, err
		}
	} else {
		if c == cid.Undef {
			return cid.Undef, provider.ErrContextIDNotFound
		}
		log.Info("Generating removal list for advertisement")

		// And if we are removing it means we probably do not have the list of
		// CIDs anymore, so we can remove the entry from the datastore.
		err = e.deleteKeyCidMap(ctx, contextID)
		if err != nil {
			log.Errorw("Failed deleting Key-Cid map for contextID", "err", err)
			return cid.Undef, err
		}
		err = e.deleteCidKeyMap(ctx, c)
		if err != nil {
			log.Errorw("Failed deleting Cid-Key map for lookup cid", "cid", c, "err", err)
			return cid.Undef, err
		}
		err = e.deleteKeyMetadataMap(ctx, contextID)
		if err != nil {
			log.Errorw("Failed deleting Key-Metadata map for contextID", "err", err)
			return cid.Undef, err
		}

		// Provide the CID link to the content entries to give the indexer the
		// option to remove all individual entries.  If a contextID is given
		// then the indexer will delete all content for that contextID and not
		// need to retrieve the entries.
		cidsLnk = cidlink.Link{Cid: c}

		// The advertisement still requires a valid metadata even though it is
		// not used for removal.  Create a valid empty metadata.
		metadata = stiapi.Metadata{
			ProtocolID: cardatatransfer.ContextIDCodec, //providerProtocolID,
		}
	}

	// Get the latest advertisement that was generated
	latestAdvID, err := e.getLatestAdv(ctx)
	if err != nil {
		log.Errorw("Could not get latest advertisement", "err", err)
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
			log.Errorw("Error generating link from latest advertisement", "err", err)
			return cid.Undef, err
		}
		previousLnk = nb.Build().(schema.Link_Advertisement)
	}

	adv, err := schema.NewAdvertisement(e.privKey, previousLnk, cidsLnk,
		contextID, metadata, isRm, e.host.ID().String(), e.addrs)
	if err != nil {
		log.Errorw("Error generating new advertisement", "err", err)
		return cid.Undef, err
	}
	return e.Publish(ctx, adv)
}

func (e *Engine) putKeyCidMap(ctx context.Context, contextID []byte, c cid.Cid) error {
	// We need to store the map Key-Cid to know what CidLink to put
	// in advertisement when we notify a removal.
	err := e.ds.Put(ctx, datastore.NewKey(keyToCidMapPrefix+string(contextID)), c.Bytes())
	if err != nil {
		return err
	}
	// And the other way around when graphsync ios making a request,
	// so the callback in the linksystem knows to what contextID we are referring.
	return e.ds.Put(ctx, datastore.NewKey(cidToKeyMapPrefix+c.String()), contextID)
}

func (e *Engine) getKeyCidMap(ctx context.Context, contextID []byte) (cid.Cid, error) {
	b, err := e.ds.Get(ctx, datastore.NewKey(keyToCidMapPrefix+string(contextID)))
	if err != nil {
		return cid.Undef, err
	}
	_, d, err := cid.CidFromBytes(b)
	return d, err
}

func (e *Engine) deleteKeyCidMap(ctx context.Context, contextID []byte) error {
	return e.ds.Delete(ctx, datastore.NewKey(keyToCidMapPrefix+string(contextID)))
}

func (e *Engine) deleteCidKeyMap(ctx context.Context, c cid.Cid) error {
	return e.ds.Delete(ctx, datastore.NewKey(cidToKeyMapPrefix+c.String()))
}

func (e *Engine) getCidKeyMap(ctx context.Context, c cid.Cid) ([]byte, error) {
	return e.ds.Get(ctx, datastore.NewKey(cidToKeyMapPrefix+c.String()))
}

func (e *Engine) putKeyMetadataMap(ctx context.Context, contextID []byte, metadata stiapi.Metadata) error {
	data, err := metadata.MarshalBinary()
	if err != nil {
		return err
	}
	return e.ds.Put(ctx, datastore.NewKey(keyToMetadataMapPrefix+string(contextID)), data)
}

func (e *Engine) getKeyMetadataMap(ctx context.Context, contextID []byte) (stiapi.Metadata, error) {
	data, err := e.ds.Get(ctx, datastore.NewKey(keyToMetadataMapPrefix+string(contextID)))
	if err != nil {
		return stiapi.Metadata{}, err
	}
	var metadata stiapi.Metadata
	if err = metadata.UnmarshalBinary(data); err != nil {
		return stiapi.Metadata{}, err
	}
	return metadata, nil
}

func (e *Engine) deleteKeyMetadataMap(ctx context.Context, contextID []byte) error {
	return e.ds.Delete(ctx, datastore.NewKey(keyToMetadataMapPrefix+string(contextID)))
}

func (e *Engine) putLatestAdv(ctx context.Context, advID []byte) error {
	return e.ds.Put(ctx, dsLatestAdvKey, advID)
}

func (e *Engine) getLatestAdv(ctx context.Context) (cid.Cid, error) {
	b, err := e.ds.Get(ctx, dsLatestAdvKey)
	if err != nil {
		if err == datastore.ErrNotFound {
			return cid.Undef, nil
		}
		return cid.Undef, err
	}
	_, c, err := cid.CidFromBytes(b)
	return c, err
}
