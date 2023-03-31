package mirror

import (
	"bytes"
	"context"
	"errors"
	"io"
	"time"

	dt "github.com/filecoin-project/go-data-transfer/v2"
	datatransfer "github.com/filecoin-project/go-data-transfer/v2/impl"
	dtnetwork "github.com/filecoin-project/go-data-transfer/v2/network"
	gstransport "github.com/filecoin-project/go-data-transfer/v2/transport/graphsync"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/namespace"
	"github.com/ipfs/go-graphsync"
	gsimpl "github.com/ipfs/go-graphsync/impl"
	gsnet "github.com/ipfs/go-graphsync/network"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipni/go-libipni/dagsync"
	"github.com/ipni/go-libipni/dagsync/dtsync"
	"github.com/ipni/go-libipni/ingest/schema"
	"github.com/ipni/index-provider/engine/chunker"
	"github.com/ipni/index-provider/metrics"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
)

var log = logging.Logger("provider/mirror")

// Mirror provides the ability to mirror the advertisement chain of an existing provider, with
// options to restructure entries as EntryChunk chain or HAMT.
//
// Additionally, a mirror can also serve as a CDN for the original advertisement chain and its
// entries. It exposes a GraphSync publisher endpoint from which ad chain can be synced.
type Mirror struct {
	*options
	source  peer.AddrInfo
	sub     *dagsync.Subscriber
	pub     dagsync.Publisher
	ls      ipld.LinkSystem
	chunker *chunker.CachedEntriesChunker
	cancel  context.CancelFunc
}

// New instantiates a new Mirror that mirrors ad chain from the given source provider.
//
// See: Mirror.Start, Mirror.Shutdown.
func New(ctx context.Context, source peer.AddrInfo, o ...Option) (*Mirror, error) {
	opts, err := newOptions(o...)
	if err != nil {
		return nil, err
	}
	m := &Mirror{
		options: opts,
		source:  source,
		ls:      cidlink.DefaultLinkSystem(),
	}
	m.ls.StorageReadOpener = m.storageReadOpener
	m.ls.StorageWriteOpener = m.storageWriteOpener

	// Do not bother instantiating chunker if there is no entries remapping to be done.
	if m.remapEntriesEnabled() {
		if m.chunker, err = chunker.NewCachedEntriesChunker(
			ctx, opts.ds,
			opts.chunkCacheCap,
			opts.chunkerFunc,
			opts.chunkCachePurge); err != nil {
			return nil, err
		}
	}

	dtds := namespace.Wrap(opts.ds, datastore.NewKey("datatransfer"))
	dm, gx, err := newDataTransfer(ctx, m.h, dtds, m.ls)
	if err != nil {
		return nil, err
	}

	// TODO: make the publisher kind configurable just like we do for Engine.
	m.pub, err = dtsync.NewPublisherFromExisting(dm, m.h, m.topic, m.ls)
	if err != nil {
		return nil, err
	}
	m.sub, err = dagsync.NewSubscriber(m.h, nil, m.ls, m.topic, nil, dagsync.DtManager(dm, gx))
	if err != nil {
		return nil, err
	}
	return m, nil
}

// TODO: add option to override this
func newDataTransfer(ctx context.Context, host host.Host, ds datastore.Batching, ls ipld.LinkSystem) (dt.Manager, graphsync.GraphExchange, error) {
	gn := gsnet.NewFromLibp2pHost(host)
	gx := gsimpl.New(ctx, gn, ls)
	dn := dtnetwork.NewFromLibp2pHost(host)
	tp := gstransport.NewTransport(host.ID(), gx)
	dm, err := datatransfer.NewDataTransfer(ds, dn, tp)
	if err != nil {
		return nil, nil, err
	}
	dtReady := make(chan error)
	dm.OnReady(func(e error) { dtReady <- e })
	if err := dm.Start(ctx); err != nil {
		return nil, nil, err
	}
	if err := <-dtReady; err != nil {
		return nil, nil, err
	}
	return dm, gx, nil
}

func (m *Mirror) Start() error {
	ctx, cancel := context.WithCancel(context.Background())
	m.cancel = cancel
	go func() {
		for {
			var t time.Time
			select {
			case t = <-m.ticker.C:
			case <-ctx.Done():
				return
			}
			log := log.With("time", t)
			log.Info("checking for new advertisements")
			mc, err := m.getLatestOriginalAdCid(ctx)

			if err != nil {
				log.Errorw("failed to get the latest mirrored cid", "err", err)
				continue
			}
			log = log.With("latestMirroredCid", mc)

			var sel ipld.Node
			if cid.Undef.Equals(mc) {
				sel = selectors.adsWithRecursionLimit(m.initAdRecurLimit)
			} else {
				sel = selectors.adsWithStopAt(selector.RecursionLimitNone(), cidlink.Link{Cid: mc})
			}

			syncedAdCids, err := m.syncAds(ctx, sel)
			if err != nil {
				log.Errorw("Failed to sync source", "err", err)
				continue
			}

			for _, adCid := range syncedAdCids {
				start := time.Now()
				err := m.mirror(ctx, adCid)
				elapsed := time.Since(start)
				attr := metrics.Attributes.StatusSuccess
				if err != nil {
					attr = metrics.Attributes.StatusFailure
					log.Errorw("Failed to mirror ad", "cid", adCid, "err", err)
					// TODO add an option on what to do if the mirroring of an ad failed?
					// TODO codify the errors and use the error code as an additional attribute in metrics.
				}
				metrics.Mirror.ProcessDuration.Record(ctx, elapsed.Milliseconds(), attr)
			}

			syncedCount := len(syncedAdCids)
			if syncedCount > 0 {
				latestOriginal := syncedAdCids[syncedCount-1]
				err = m.setLatestOriginalAdCid(ctx, latestOriginal)
				if err != nil {
					log.Errorw("Failed to store latest original ad cid", "cid", latestOriginal, "err", err)
				}
			}
		}
	}()

	return nil
}

func (m *Mirror) Shutdown() error {
	m.ticker.Stop()
	if m.cancel != nil {
		m.cancel()
	}
	return nil
}

func (m *Mirror) mirror(ctx context.Context, adCid cid.Cid) error {
	log := log.With("originalAd", adCid)
	ad, err := m.loadAd(ctx, adCid)
	if err != nil {
		return err
	}
	if err := ad.Validate(); err != nil {
		log.Errorw("Original ad is invalid", "err", err)
		return err
	}

	origSigner, err := ad.VerifySignature()
	if err != nil {
		log.Errorw("Original ad signature verification failed", "err", err)
		return err
	}
	log = log.With("originalSigner", origSigner)

	var adChanged bool
	// Mirror link to previous ad.
	wasPreviousID := ad.PreviousID
	prevMirroredAdCid, err := m.getLatestMirroredAdCid(ctx)
	if err != nil {
		log.Errorw("Failed to get latest mirrored ad", "err", err)
		return err
	} else if !cid.Undef.Equals(prevMirroredAdCid) {
		// Only override the original previousID link if there is a previously mirrored ad.
		// This means that if mirroring starts from a partial original ad chain, the original link
		// to previous ad will be preserved even though the ad that corresponds to it is not hosted
		// by the mirror.
		ad.PreviousID = cidlink.Link{Cid: prevMirroredAdCid}
	}
	adChanged = wasPreviousID != ad.PreviousID

	// Mirror link to entries.
	wasEntries := ad.Entries
	entriesCid := ad.Entries.(cidlink.Link).Cid
	if !ad.IsRm {
		switch entriesCid {
		case cid.Undef:
			// advertisement is invalid? entries CID should never be cid.Undef for non-removal ads.
			return errors.New("entries link is cid.Undef")
		case schema.NoEntries.Cid:
			// Nothing to do.
		default:
			if len(m.source.Addrs) == 0 {
				return errors.New("no address for source")
			}
			_, err = m.sub.Sync(ctx, m.source, entriesCid, selectors.entriesWithLimit(m.entriesRecurLimit))
			if err != nil {
				log.Errorw("Failed to sync entries", "cid", entriesCid, "err", err)
				return err
			}
			ad.Entries, err = m.remapEntries(ctx, ad.Entries)
			if err != nil {
				return err
			}
		}
	}
	adChanged = adChanged || wasEntries != ad.Entries

	// Only re-sign ad if the option is set or some content in the ad has changed.
	if m.alwaysReSignAds || adChanged {
		if err := ad.Sign(m.h.Peerstore().PrivKey(m.h.ID())); err != nil {
			return err
		}
	}

	// Sanity check that mirrored ad is still valid.
	// At this moment in time ad validation just checks the max size of metadata and context ID
	// neither of which should have been modified by mirroring.
	// Nevertheless, validate the mirrored ad since ad validation logic may (and perhaps should)
	// become more selective to check the fields that may be modified by mirroring like the
	// entries link.
	if err := ad.Validate(); err != nil {
		return err
	}

	node, err := ad.ToNode()
	if err != nil {
		return err
	}
	mirroredAdLink, err := m.ls.Store(ipld.LinkContext{Ctx: ctx}, schema.Linkproto, node)
	if err != nil {
		return err
	}

	mirroredAdCid := mirroredAdLink.(cidlink.Link).Cid
	if err = m.setLatestMirroredAdCid(ctx, mirroredAdCid); err != nil {
		return err
	}

	if err := m.pub.UpdateRoot(ctx, mirroredAdCid); err != nil {
		return err
	}
	log.Infow("Mirrored successfully", "originalAdCid", adCid, "mirroredAdCid", mirroredAdCid)
	return nil
}

func (m *Mirror) storageReadOpener(lctx linking.LinkContext, lnk datamodel.Link) (io.Reader, error) {
	if lnk == schema.NoEntries {
		return nil, errors.New("no-entries CID is not retrievable")
	}
	ctx := lctx.Ctx
	c := lnk.(cidlink.Link).Cid

	val, err := m.ds.Get(ctx, datastore.NewKey(lnk.Binary()))
	if err != nil && err != datastore.ErrNotFound {
		return nil, err
	}
	if len(val) != 0 {
		// Do not discriminate by what the link point to; both old ads old entries and new entries.
		// This makes the mirror act as CDN for the original ad chain too.
		return bytes.NewBuffer(val), err
	}

	// If remapping entries is not enabled then we do not have the blocks asked for.
	if !m.remapEntriesEnabled() {
		return nil, datastore.ErrNotFound
	}

	b, err := m.chunker.GetRawCachedChunk(ctx, lnk)
	if err != nil {
		return nil, err
	}

	if b == nil {
		orig, err := m.getOriginalEntriesLinkFromMirror(ctx, lnk)
		if err != nil {
			log.Errorw("Failed to get original entries link from mirror link", "link", lnk, "err", err)
			return nil, err
		}
		mhi, err := m.loadEntries(ctx, orig)
		if err != nil {
			return nil, err
		}
		chunkedLink, err := m.chunker.Chunk(ctx, mhi)
		if err != nil {
			return nil, err
		}
		if chunkedLink != lnk {
			// TODO the chunker must have changed. Nothing to do; error out.
			return nil, errors.New("chunked link does not match the mapping to original entry")
		}
	} else {
		log.Debugw("Found cache entry for CID", "cid", c)
	}

	// FIXME: under high concurrency or small capacity it is likely enough for the cached entry to
	//        get evicted before we get the chance to read it back. This is true in the current
	//        engine implementation too.
	val, err = m.chunker.GetRawCachedChunk(ctx, lnk)
	if err != nil {
		log.Errorf("Error fetching cached list for CID (%s): %s", c, err)
		return nil, err
	}
	if len(val) == 0 {
		return nil, datastore.ErrNotFound
	}
	return bytes.NewBuffer(val), nil
}

func (m *Mirror) storageWriteOpener(lctx linking.LinkContext) (io.Writer, linking.BlockWriteCommitter, error) {
	buf := bytes.NewBuffer(nil)
	return buf, func(lnk ipld.Link) error {
		return m.ds.Put(lctx.Ctx, datastore.NewKey(lnk.Binary()), buf.Bytes())
	}, nil
}

func (m *Mirror) remapEntries(ctx context.Context, original ipld.Link) (ipld.Link, error) {
	if !m.remapEntriesEnabled() {
		return original, nil
	}
	// Check if remapping should be skipped when the original entry kind matches the target kind.
	if m.skipRemapOnEntriesTypeMatch {
		entriesType, err := m.getEntriesPrototype(ctx, original)
		if err != nil {
			return nil, err
		}
		if entriesType == m.entriesRemapPrototype {
			return original, nil
		}
	}

	// Load the entries as multihash iterator.
	mhi, err := m.loadEntries(ctx, original)
	if err != nil {
		return nil, err
	}
	// Use the chunker mechanism to re-generate entries as it supports both entry chunk chan and
	// HAMT.
	mirroredEntriesLink, err := m.chunker.Chunk(ctx, mhi)
	if err != nil {
		return nil, err
	}
	// Store a mapping between the remapped entries link and the original link.
	// The remapping is used to load the original content in case it needs to be regenerated
	// as a result of entry chunk cache eviction.
	if err := m.setMirroredEntriesLink(ctx, mirroredEntriesLink, original); err != nil {
		return nil, err
	}
	return mirroredEntriesLink, nil
}

func (m *Mirror) syncAds(ctx context.Context, sel ipld.Node) ([]cid.Cid, error) {
	if len(m.source.Addrs) == 0 {
		return nil, errors.New("no address for source")
	}
	startSync := time.Now()
	var syncedAdCids []cid.Cid
	_, err := m.sub.Sync(ctx, m.source, cid.Undef, sel,
		dagsync.ScopedBlockHook(func(id peer.ID, c cid.Cid, actions dagsync.SegmentSyncActions) {
			// TODO: set actions next segment link to ad previous id if it is present. For
			//      now segmentation is disabled.
			//       Here we could be encountering HAMT or Entry Chunk so picking the next
			//       CID is not trivial; we probably should not use segmentation for HAMT
			//       at all.

			// Prepend to the list since the mirroring should start from the oldest ad first.
			syncedAdCids = append([]cid.Cid{c}, syncedAdCids...)
		}),
		// Disable segmentation until the actions in hook are handled appropriately
		dagsync.ScopedSegmentDepthLimit(-1),
	)
	elapsedSync := time.Since(startSync)
	attr := metrics.Attributes.StatusSuccess
	if err != nil {
		attr = metrics.Attributes.StatusFailure
	}
	metrics.Mirror.SyncDuration.Record(ctx, elapsedSync.Milliseconds(), attr)
	return syncedAdCids, err
}
