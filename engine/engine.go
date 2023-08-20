package engine

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sync"

	"github.com/hashicorp/go-multierror"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dsn "github.com/ipfs/go-datastore/namespace"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipni/go-libipni/announce"
	"github.com/ipni/go-libipni/announce/httpsender"
	"github.com/ipni/go-libipni/announce/message"
	"github.com/ipni/go-libipni/announce/p2psender"
	"github.com/ipni/go-libipni/dagsync"
	"github.com/ipni/go-libipni/dagsync/dtsync"
	"github.com/ipni/go-libipni/dagsync/ipnisync"
	"github.com/ipni/go-libipni/ingest/schema"
	"github.com/ipni/go-libipni/metadata"
	provider "github.com/ipni/index-provider"
	"github.com/ipni/index-provider/engine/chunker"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
)

const (
	keyToCidMapPrefix            = "map/keyCid/"
	cidToKeyMapPrefix            = "map/cidKey/"
	cidToProviderAndKeyMapPrefix = "map/cidProvAndKey/"
	keyToMetadataMapPrefix       = "map/keyMD/"
	latestAdvKey                 = "sync/adv/"
	linksCachePath               = "/cache/links"
)

var (
	log = logging.Logger("provider/engine")

	dsLatestAdvKey = datastore.NewKey(latestAdvKey)
)

// Engine is an implementation of the core reference provider interface.
type Engine struct {
	*options
	lsys ipld.LinkSystem

	entriesChunker *chunker.CachedEntriesChunker

	publisher dagsync.Publisher
	senders   []announce.Sender

	mhLister provider.MultihashLister
	cblk     sync.Mutex
}

var _ provider.Interface = (*Engine)(nil)

// New creates a new index provider Engine as the default implementation of
// provider.Interface. It provides the ability to advertise the availability of
// a list of multihashes associated to a context ID as a chain of linked
// advertisements as defined by the indexer node protocol implemented by
// "go-libipni".
//
// Engine internally uses "go-libipni/dagsync", a protocol for propagating and
// synchronizing changes an IPLD DAG, to publish advertisements. See:
//
//   - https://github.com/ipni/go-libipni/tree/main/dagsync
//
// Published advertisements are signed using the given private key. The
// retAddrs corresponds to the endpoints at which the data block associated to
// the advertised multihashes can be retrieved. If no retAddrs are specified,
// then use the listen addresses of the given libp2p host.
//
// The engine also provides the ability to generate advertisements via
// Engine.NotifyPut and Engine.NotifyRemove as long as a
// provider.MultihashLister is registered. See: provider.MultihashLister,
// Engine.RegisterMultihashLister.
//
// The engine must be started via Engine.Start before use and discarded via
// Engine.Shutdown when no longer needed.
func New(o ...Option) (*Engine, error) {
	opts, err := newOptions(o...)
	if err != nil {
		return nil, err
	}

	e := &Engine{
		options: opts,
	}

	e.lsys = e.mkLinkSystem()

	return e, nil
}

// Start starts the engine by instantiating the internal storage and joining
// the configured gossipsub topic used for publishing advertisements.
//
// The context is used to instantiate the internal LRU cache storage. See:
// Engine.Shutdown, chunker.NewCachedEntriesChunker,
// dtsync.NewPublisherFromExisting
func (e *Engine) Start(ctx context.Context) error {
	var err error
	// Create datastore entriesChunker.
	entriesCacheDs := dsn.Wrap(e.ds, datastore.NewKey(linksCachePath))
	e.entriesChunker, err = chunker.NewCachedEntriesChunker(ctx, entriesCacheDs, e.entCacheCap, e.chunker, e.purgeCache)
	if err != nil {
		return err
	}

	e.publisher, err = e.newPublisher()
	if err != nil {
		log.Errorw("Failed to instantiate publisher", "err", err, "kind", e.pubKind)
		return err
	}

	if e.publisher != nil {
		// Initialize publisher with latest advertisement CID.
		adCid, err := e.getLatestAdCid(ctx)
		if err != nil {
			return fmt.Errorf("could not get latest advertisement cid: %w", err)
		}
		if adCid != cid.Undef {
			e.publisher.SetRoot(adCid)
		}

		// If publisher created, then create announcement senders.
		e.senders, err = e.createSenders()
		if err != nil {
			return err
		}
	}

	return nil
}

func (e *Engine) newPublisher() (dagsync.Publisher, error) {
	switch e.pubKind {
	case NoPublisher:
		log.Info("Remote announcements disabled; all advertisements will only be stored locally.")
		return nil, nil
	case HttpPublisher:
		streamHost := e.h
		if e.pubHttpNoLibp2p {
			streamHost = nil
		}
		httpPub, err := ipnisync.NewPublisher(e.lsys, e.key,
			ipnisync.WithStreamHost(streamHost),
			ipnisync.WithHTTPListenAddrs(e.pubHttpListenAddr),
			ipnisync.WithHeadTopic(e.pubTopicName),
			ipnisync.WithHandlerPath(e.pubHttpHandlerPath),
			ipnisync.WithStartServer(!e.pubHttpWithoutServer))
		if err != nil {
			return nil, fmt.Errorf("cannot create http publisher: %w", err)
		}
		if len(e.pubHttpAnnounceAddrs) == 0 {
			e.pubHttpAnnounceAddrs = append(e.pubHttpAnnounceAddrs, httpPub.Addrs()...)
			log.Warn("Plain HTTP publisher in use without HTTP address for announcements. Using publisher listen address, but external address may be needed.", "addrs", e.pubHttpAnnounceAddrs)
		}
		return httpPub, nil
	case DataTransferPublisher:
		log.Warn("Support ending for publishing IPNI data over data-transfer/graphsync, Disable this feature in configuration and test that indexing is working over libp2p.")
		if e.pubDT != nil {
			dtPub, err := dtsync.NewPublisherFromExisting(e.pubDT, e.h, e.pubTopicName, e.lsys, dtsync.WithAllowPeer(e.syncPolicy.Allowed))
			if err != nil {
				return nil, fmt.Errorf("cannot create data-transfer publisher with existing dt manager: %w", err)
			}
			return dtPub, nil
		}
		ds := dsn.Wrap(e.ds, datastore.NewKey("/dagsync/dtsync/pub"))
		dtPub, err := dtsync.NewPublisher(e.h, ds, e.lsys, e.pubTopicName, dtsync.WithAllowPeer(e.syncPolicy.Allowed))
		if err != nil {
			return nil, fmt.Errorf("cannot create data-transfer publisher: %w", err)
		}
		return dtPub, nil
	}
	return nil, fmt.Errorf("unknown publisher kind: %s", e.pubKind)
}

func (e *Engine) createSenders() ([]announce.Sender, error) {
	var senders []announce.Sender

	// If there are announce URLs, then creage an announce sender to send
	// direct HTTP announce messages to these URLs.
	if len(e.announceURLs) != 0 {
		id, err := peer.IDFromPrivateKey(e.key)
		if err != nil {
			return nil, fmt.Errorf("cannot get peer ID from private key: %w", err)
		}
		httpSender, err := httpsender.New(e.announceURLs, id)
		if err != nil {
			return nil, fmt.Errorf("cannot create http announce sender: %w", err)
		}
		senders = append(senders, httpSender)
	}

	// If there is a libp2p host, then create a gossip pubsub announce sender.
	if e.h != nil {
		// Create an announce sender to send over gossip pubsub.
		p2pSender, err := p2psender.New(e.h, e.pubTopicName,
			p2psender.WithTopic(e.pubTopic),
			p2psender.WithExtraData(e.pubExtraGossipData))
		if err != nil {
			return nil, fmt.Errorf("cannot create p2p pubsub announce sender: %w", err)
		}
		senders = append(senders, p2pSender)
	}

	return senders, nil
}

// announce uses the engines senders to send advertisement announcement messages.
func (e *Engine) announce(ctx context.Context, c cid.Cid) {
	var err error
	switch e.pubKind {
	case HttpPublisher:
		err = announce.Send(ctx, c, e.pubHttpAnnounceAddrs, e.senders...)
	case DataTransferPublisher:
		// TODO: It may be necessary to specify a set of external addresses to
		// put into the announce message, instead of using the libp2p host's
		// addresses.
		err = announce.Send(ctx, c, e.h.Addrs(), e.senders...)
	}
	if err != nil {
		log.Errorw("Failed to announce advertisement", "err", err)
	}
}

// PublishLocal stores the advertisement in the local link system and marks it
// locally as the latest advertisement.
//
// The context is used for storing internal mapping information onto the
// datastore.
//
// See: Engine.Publish.
func (e *Engine) PublishLocal(ctx context.Context, adv schema.Advertisement) (cid.Cid, error) {
	if err := adv.Validate(); err != nil {
		return cid.Undef, err
	}

	adNode, err := adv.ToNode()
	if err != nil {
		return cid.Undef, err
	}

	lnk, err := e.lsys.Store(ipld.LinkContext{Ctx: ctx}, schema.Linkproto, adNode)
	if err != nil {
		return cid.Undef, fmt.Errorf("cannot generate advertisement link: %s", err)
	}
	c := lnk.(cidlink.Link).Cid
	log := log.With("adCid", c)
	log.Info("Stored ad in local link system")

	if err = e.putLatestAdv(ctx, c.Bytes()); err != nil {
		log.Errorw("Failed to update reference to the latest advertisement", "err", err)
		return cid.Undef, fmt.Errorf("failed to update reference to latest advertisement: %w", err)
	}
	log.Info("Updated reference to the latest advertisement successfully")
	return c, nil
}

// Publish stores the given advertisement locally via Engine.PublishLocal
// first, then publishes a message onto the gossipsub to signal the change in
// the latest advertisement by the provider to indexer nodes.
//
// The publication mechanism uses dagsync.Publisher internally.
// See: https://github.com/ipni/go-libipni/tree/main/dagsync
func (e *Engine) Publish(ctx context.Context, adv schema.Advertisement) (cid.Cid, error) {
	c, err := e.PublishLocal(ctx, adv)
	if err != nil {
		log.Errorw("Failed to store advertisement locally", "err", err)
		return cid.Undef, fmt.Errorf("failed to publish advertisement locally: %w", err)
	}

	// Only announce the advertisement CID if publisher is configured.
	if e.publisher != nil {
		log := log.With("adCid", c)
		if len(e.announceURLs) == 0 && e.h != nil {
			log.Info("Announcing advertisement in pubsub channel")
		} else if len(e.announceURLs) != 0 && e.h == nil {
			log.Info("Announcing advertisement via http")
		} else if len(e.announceURLs) != 0 && e.h != nil {
			log.Info("Announcing advertisement in pubsub channel and via http")
		} else {
			return cid.Undef, fmt.Errorf("unexpected publisher state, no announceURLs or libp2p host")
		}

		e.publisher.SetRoot(c)
		e.announce(ctx, c)
	}

	return c, nil
}

func (e *Engine) latestAdToPublish(ctx context.Context) (cid.Cid, error) {
	// Skip announcing the latest advertisement CID if there is no publisher.
	if e.publisher == nil {
		log.Infow("Skipped announcing the latest: remote announcements are disabled.")
		return cid.Undef, nil
	}

	adCid, err := e.getLatestAdCid(ctx)
	if err != nil {
		return cid.Undef, fmt.Errorf("failed to get latest advertisement cid: %w", err)
	}

	if adCid == cid.Undef {
		log.Info("Skipped announcing the latest: no previously published advertisements.")
		return cid.Undef, nil
	}

	return adCid, nil
}

// PublishLatest re-publishes the latest existing advertisement and send
// announcements using the engine's configured senders.
func (e *Engine) PublishLatest(ctx context.Context) (cid.Cid, error) {
	adCid, err := e.latestAdToPublish(ctx)
	if err != nil {
		return cid.Undef, err
	}
	log.Infow("Publishing latest advertisement", "cid", adCid)

	e.publisher.SetRoot(adCid)
	e.announce(ctx, adCid)

	return adCid, nil
}

// PublishLatestHTTP publishes the latest existing advertisement and sends
// direct HTTP announcements to the specified URLs.
func (e *Engine) PublishLatestHTTP(ctx context.Context, announceURLs ...*url.URL) (cid.Cid, error) {
	adCid, err := e.latestAdToPublish(ctx)
	if err != nil {
		return cid.Undef, err
	}

	e.publisher.SetRoot(adCid)
	err = e.httpAnnounce(ctx, adCid, announceURLs)
	if err != nil {
		return adCid, err
	}

	return adCid, nil
}

// httpAnnounce creates an HTTP sender to send an announcement to the specified
// URLs. An HTTP sender is created because the engine may not have an HTTP
// sender that sends to the URLs specified by announceURLs.
func (e *Engine) httpAnnounce(ctx context.Context, adCid cid.Cid, announceURLs []*url.URL) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if len(announceURLs) == 0 {
		return nil
	}

	// Create announce message.
	msg := message.Message{
		Cid: adCid,
	}

	// The publisher kind determines what addresses to put into the announce
	// message.
	switch e.pubKind {
	case NoPublisher:
		log.Info("Remote announcements disabled")
		return nil
	case DataTransferPublisher:
		// TODO: It may be necessary to specify a set of external addresses to
		// put into the announce message, instead of using the libp2p host's
		// addresses.
		msg.SetAddrs(e.h.Addrs())
	case HttpPublisher:
		if len(e.pubHttpAnnounceAddrs) != 0 {
			msg.SetAddrs(e.pubHttpAnnounceAddrs)
		} else {
			msg.SetAddrs(e.publisher.Addrs())
		}
	}

	// Create the http announce sender.
	httpSender, err := httpsender.New(announceURLs, e.h.ID())
	if err != nil {
		return fmt.Errorf("cannot create http announce sender: %w", err)
	}

	log.Infow("Announcing advertisements over HTTP", "urls", announceURLs)
	return httpSender.Send(ctx, msg)
}

// RegisterMultihashLister registers a provider.MultihashLister that is used to
// look up the list of multihashes associated to a context ID. At least one
// such registration must be registered before calls to Engine.NotifyPut and
// Engine.NotifyRemove.
//
// Note that successive calls to this function will replace the previous
// registration. Only a single registration is supported.
//
// See: provider.Interface
func (e *Engine) RegisterMultihashLister(mhl provider.MultihashLister) {
	log.Debugf("Registering multihash lister in engine")
	e.cblk.Lock()
	defer e.cblk.Unlock()
	e.mhLister = mhl
}

// NotifyPut publishes an advertisement that signals the list of multihashes
// associated to the given contextID is available by this provider with the
// given metadata. A provider.MultihashLister is required, and is used to look
// up the list of multihashes associated to a context ID.
//
// Note that prior to calling this function a provider.MultihashLister must be
// registered.
//
// See: Engine.RegisterMultihashLister, Engine.Publish.
func (e *Engine) NotifyPut(ctx context.Context, provider *peer.AddrInfo, contextID []byte, md metadata.Metadata) (cid.Cid, error) {
	// The multihash lister must have been registered for the linkSystem to
	// know how to go from contextID to list of CIDs.
	pID := e.options.provider.ID
	addrs := e.options.provider.Addrs
	if provider != nil {
		pID = provider.ID
		addrs = provider.Addrs
	}
	return e.publishAdvForIndex(ctx, pID, addrs, contextID, md, false)
}

// NotifyRemove publishes an advertisement that signals the list of multihashes
// associated to the given contextID is no longer available by this provider.
//
// Note that prior to calling this function a provider.MultihashLister must be
// registered.
//
// See: Engine.RegisterMultihashLister, Engine.Publish.
func (e *Engine) NotifyRemove(ctx context.Context, provider peer.ID, contextID []byte) (cid.Cid, error) {
	// TODO: add support for "delete all" for provider
	if provider == "" {
		provider = e.options.provider.ID
	}
	return e.publishAdvForIndex(ctx, provider, nil, contextID, metadata.Metadata{}, true)
}

// LinkSystem gets the link system used by the engine to store and retrieve
// advertisement data.
func (e *Engine) LinkSystem() *ipld.LinkSystem {
	return &e.lsys
}

// Shutdown shuts down the engine and discards all resources opened by the
// engine. The engine is no longer usable after the call to this function.
func (e *Engine) Shutdown() error {
	var err, errs error
	if e.publisher != nil {
		for i := range e.senders {
			if err = e.senders[i].Close(); err != nil {
				errs = multierror.Append(errs, fmt.Errorf("error closing sender: %s", err))
			}
		}
		if err = e.publisher.Close(); err != nil {
			errs = multierror.Append(errs, fmt.Errorf("error closing leg publisher: %s", err))
		}
	}
	if err = e.entriesChunker.Close(); err != nil {
		errs = multierror.Append(errs, fmt.Errorf("error closing link entriesChunker: %s", err))
	}
	return errs
}

// GetPublisherHttpFunc gets the http.HandlerFunc that can be used to serve
// advertisements over HTTP. The returned handler is only valid if the
// PublisherKind is HttpPublisher and the HttpPublisherWithoutServer option is
// set.
func (e *Engine) GetPublisherHttpFunc() (http.HandlerFunc, error) {
	if e.publisher == nil {
		return nil, errors.New("no publisher configured")
	}
	if !e.pubHttpWithoutServer {
		return nil, errors.New("HttpPublisherWithoutServer option not set")
	}
	hp, ok := e.publisher.(*ipnisync.Publisher)
	if !ok {
		return nil, errors.New("publisher is not an http publisher")
	}
	return hp.ServeHTTP, nil
}

// GetAdv gets the advertisement associated to the given cid c. The context is
// not used.
func (e *Engine) GetAdv(_ context.Context, adCid cid.Cid) (*schema.Advertisement, error) {
	log := log.With("cid", adCid)
	log.Infow("Getting advertisement by CID")

	lsys := e.vanillaLinkSystem()
	n, err := lsys.Load(ipld.LinkContext{}, cidlink.Link{Cid: adCid}, schema.AdvertisementPrototype)
	if err != nil {
		return nil, fmt.Errorf("cannot load advertisement from blockstore with vanilla linksystem: %s", err)
	}
	return schema.UnwrapAdvertisement(n)
}

// GetLatestAdv gets the latest advertisement by the provider. If there are no
// previously published advertisements, then cid.Undef is returned as the
// advertisement CID.
func (e *Engine) GetLatestAdv(ctx context.Context) (cid.Cid, *schema.Advertisement, error) {
	log.Info("Getting latest advertisement")
	latestAdCid, err := e.getLatestAdCid(ctx)
	if err != nil {
		return cid.Undef, nil, fmt.Errorf("could not get latest advertisement cid from blockstore: %s", err)
	}
	if latestAdCid == cid.Undef {
		return cid.Undef, nil, nil
	}

	ad, err := e.GetAdv(ctx, latestAdCid)
	if err != nil {
		return cid.Undef, nil, fmt.Errorf("count not get latest advertisement from blockstore by cid: %s", err)
	}
	return latestAdCid, ad, nil
}

func (e *Engine) publishAdvForIndex(ctx context.Context, p peer.ID, addrs []multiaddr.Multiaddr, contextID []byte, md metadata.Metadata, isRm bool) (cid.Cid, error) {
	var err error
	var cidsLnk cidlink.Link

	log := log.With("providerID", p).With("contextID", base64.StdEncoding.EncodeToString(contextID))

	c, err := e.getKeyCidMap(ctx, p, contextID)
	if err != nil {
		if err != datastore.ErrNotFound {
			return cid.Undef, fmt.Errorf("cound not not get entries cid by provider + context id: %s", err)
		}
	}

	// If not removing, then generate the link for the list of CIDs from the
	// contextID using the multihash lister, and store the relationship.
	if !isRm {
		log.Info("Creating advertisement")

		// If no previously-published ad for this context ID.
		if c == cid.Undef {
			log.Info("Generating entries linked list for advertisement")
			// If no lister registered return error.
			if e.mhLister == nil {
				return cid.Undef, provider.ErrNoMultihashLister
			}

			// Call the lister.
			mhIter, err := e.mhLister(ctx, p, contextID)
			if err != nil {
				return cid.Undef, err
			}
			// Generate the linked list ipld.Link that is added to the
			// advertisement and used for ingestion.
			lnk, err := e.entriesChunker.Chunk(ctx, mhIter)
			if err != nil {
				return cid.Undef, fmt.Errorf("could not generate entries list: %s", err)
			} else if lnk == nil {
				log.Warnw("chunking for context ID resulted in no link", "contextID", contextID)
				lnk = schema.NoEntries
			}
			cidsLnk = lnk.(cidlink.Link)

			// Store the relationship between providerID, contextID and CID of the
			// advertised list of Cids.
			err = e.putKeyCidMap(ctx, p, contextID, cidsLnk.Cid)
			if err != nil {
				return cid.Undef, fmt.Errorf("failed to write provider + context id to entries cid mapping: %s", err)
			}
		} else {
			// Lookup metadata for this providerID and contextID.
			prevMetadata, err := e.getKeyMetadataMap(ctx, p, contextID)
			if err != nil {
				if err != datastore.ErrNotFound {
					return cid.Undef, fmt.Errorf("could not get metadata for provider + context id: %s", err)
				}
				log.Warn("No metadata for existing provider + context ID, generating new advertisement")
			}

			if md.Equal(prevMetadata) {
				// Metadata is the same; no change, no need for new
				// advertisement.
				return cid.Undef, provider.ErrAlreadyAdvertised
			}

			// Linked list is the same, but metadata is different, so generate
			// new advertisement with same linked list, but new metadata.
			cidsLnk = cidlink.Link{Cid: c}
		}

		if err = e.putKeyMetadataMap(ctx, p, contextID, &md); err != nil {
			return cid.Undef, fmt.Errorf("failed to write provider + context id to metadata mapping: %s", err)
		}
	} else {
		log.Info("Creating removal advertisement")

		if c == cid.Undef {
			return cid.Undef, provider.ErrContextIDNotFound
		}

		// If removing by context ID, it means the list of CIDs is not needed
		// anymore, so we can remove the entry from the datastore.
		err = e.deleteKeyCidMap(ctx, p, contextID)
		if err != nil {
			return cid.Undef, fmt.Errorf("failed to delete provider + context id to entries cid mapping: %s", err)
		}
		err = e.deleteCidKeyMap(ctx, c)
		if err != nil {
			return cid.Undef, fmt.Errorf("failed to delete entries cid to provider + context id mapping: %s", err)
		}
		err = e.deleteKeyMetadataMap(ctx, p, contextID)
		if err != nil {
			return cid.Undef, fmt.Errorf("failed to delete provider + context id to metadata mapping: %s", err)
		}

		// Create an advertisement to delete content by contextID by specifying
		// that advertisement has no entries.
		cidsLnk = schema.NoEntries

		// The advertisement still requires a valid metadata even though
		// metadata is not used for removal. Create a valid empty metadata.
		md = metadata.Default.New()
	}

	mdBytes, err := md.MarshalBinary()
	if err != nil {
		return cid.Undef, err
	}

	var stringAddrs []string
	for _, addr := range addrs {
		stringAddrs = append(stringAddrs, addr.String())
	}

	adv := schema.Advertisement{
		Provider:  p.String(),
		Addresses: stringAddrs,
		Entries:   cidsLnk,
		ContextID: contextID,
		Metadata:  mdBytes,
		IsRm:      isRm,
	}

	// Get the previous advertisement that was generated.
	prevAdvID, err := e.getLatestAdCid(ctx)
	if err != nil {
		return cid.Undef, fmt.Errorf("could not get latest advertisement: %s", err)
	}

	// Check for cid.Undef for the previous link. If this is the case, then
	// this means there are no previous advertisements.
	if prevAdvID == cid.Undef {
		log.Info("Latest advertisement CID was undefined - no previous advertisement")
	} else {
		adv.PreviousID = ipld.Link(cidlink.Link{Cid: prevAdvID})
	}

	// Sign the advertisement.
	if err = adv.Sign(e.key); err != nil {
		return cid.Undef, err
	}
	return e.Publish(ctx, adv)
}

func (e *Engine) keyToCidKey(provider peer.ID, contextID []byte) datastore.Key {
	switch provider {
	case e.provider.ID:
		return datastore.NewKey(keyToCidMapPrefix + string(contextID))
	default:
		return datastore.NewKey(keyToCidMapPrefix + provider.String() + "/" + string(contextID))
	}
}

func (e *Engine) cidToKeyKey(c cid.Cid) datastore.Key {
	return datastore.NewKey(cidToKeyMapPrefix + c.String())
}

func (e *Engine) cidToProviderAndKeyKey(c cid.Cid) datastore.Key {
	return datastore.NewKey(cidToProviderAndKeyMapPrefix + c.String())
}

func (e *Engine) keyToMetadataKey(provider peer.ID, contextID []byte) datastore.Key {
	switch provider {
	case e.provider.ID:
		return datastore.NewKey(keyToMetadataMapPrefix + string(contextID))
	default:
		return datastore.NewKey(keyToMetadataMapPrefix + provider.String() + "/" + string(contextID))
	}
}

func (e *Engine) putKeyCidMap(ctx context.Context, provider peer.ID, contextID []byte, c cid.Cid) error {
	// Store the map Key-Cid to know what CidLink to put in advertisement when
	// notifying about a removal.
	err := e.ds.Put(ctx, e.keyToCidKey(provider, contextID), c.Bytes())
	if err != nil {
		return err
	}
	// And the other way around when graphsync is making a request, so the
	// lister in the linksystem knows to what contextID the CID referrs to.
	// it's enough for us to store just a single mapping of cid to provider and
	// context to generate chunks

	pB, err := provider.Marshal()
	if err != nil {
		return err
	}
	m, err := json.Marshal(&providerAndContext{Provider: pB, ContextID: contextID})
	if err != nil {
		return err
	}
	return e.ds.Put(ctx, e.cidToProviderAndKeyKey(c), m)
}

func (e *Engine) getKeyCidMap(ctx context.Context, provider peer.ID, contextID []byte) (cid.Cid, error) {
	b, err := e.ds.Get(ctx, e.keyToCidKey(provider, contextID))
	if err != nil {
		return cid.Undef, err
	}
	_, d, err := cid.CidFromBytes(b)
	return d, err
}

func (e *Engine) deleteKeyCidMap(ctx context.Context, provider peer.ID, contextID []byte) error {
	return e.ds.Delete(ctx, e.keyToCidKey(provider, contextID))
}

func (e *Engine) deleteCidKeyMap(ctx context.Context, c cid.Cid) error {
	err := e.ds.Delete(ctx, e.cidToProviderAndKeyKey(c))
	if err != nil {
		return err
	}
	return e.ds.Delete(ctx, e.cidToKeyKey(c))
}

type providerAndContext struct {
	Provider  []byte `json:"p"`
	ContextID []byte `json:"c"`
}

// getCidKeyMap returns the provider and contextID for a given cid. Provider
// and Context ID are guaranteed to be not nil. In the case if legacy index
// exists, the default provider identity is assumed.
func (e *Engine) getCidKeyMap(ctx context.Context, c cid.Cid) (*providerAndContext, error) {
	// first see whether the mapping exists in the legacy index
	val, err := e.ds.Get(ctx, e.cidToKeyKey(c))
	if err == nil {
		// if the mapping has been found in the legacy index - return the
		// default provider identity.
		return &providerAndContext{Provider: []byte(e.provider.ID), ContextID: val}, nil
	}
	if !errors.Is(err, datastore.ErrNotFound) {
		return nil, err
	}
	// trying to fetch this mapping from the new index
	val, err = e.ds.Get(ctx, e.cidToProviderAndKeyKey(c))
	if err != nil {
		return nil, err
	}

	var pAndC providerAndContext
	err = json.Unmarshal(val, &pAndC)
	if err != nil {
		return nil, err
	}
	// if provider is empty (which should never happen), assume the default one
	if len(pAndC.Provider) == 0 {
		pAndC.Provider = []byte(e.provider.ID)
	}
	return &pAndC, nil
}

func (e *Engine) putKeyMetadataMap(ctx context.Context, provider peer.ID, contextID []byte, metadata *metadata.Metadata) error {
	data, err := metadata.MarshalBinary()
	if err != nil {
		return err
	}
	return e.ds.Put(ctx, e.keyToMetadataKey(provider, contextID), data)
}

func (e *Engine) getKeyMetadataMap(ctx context.Context, provider peer.ID, contextID []byte) (metadata.Metadata, error) {
	md := metadata.Default.New()
	data, err := e.ds.Get(ctx, e.keyToMetadataKey(provider, contextID))
	if err != nil {
		return md, err
	}
	if err := md.UnmarshalBinary(data); err != nil {
		return md, err
	}
	return md, nil
}

func (e *Engine) deleteKeyMetadataMap(ctx context.Context, provider peer.ID, contextID []byte) error {
	return e.ds.Delete(ctx, e.keyToMetadataKey(provider, contextID))
}

func (e *Engine) putLatestAdv(ctx context.Context, advID []byte) error {
	return e.ds.Put(ctx, dsLatestAdvKey, advID)
}

func (e *Engine) getLatestAdCid(ctx context.Context) (cid.Cid, error) {
	b, err := e.ds.Get(ctx, dsLatestAdvKey)
	if err != nil {
		if errors.Is(err, datastore.ErrNotFound) {
			return cid.Undef, nil
		}
		return cid.Undef, err
	}
	_, c, err := cid.CidFromBytes(b)
	return c, err
}
