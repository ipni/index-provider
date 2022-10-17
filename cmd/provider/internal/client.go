package internal

import (
	"context"
	"time"

	"github.com/filecoin-project/go-legs"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	selectorbuilder "github.com/ipld/go-ipld-prime/traversal/selector/builder"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/peer"
)

var log = logging.Logger("client")

type (
	ProviderClient interface {
		GetAdvertisement(ctx context.Context, id cid.Cid) (*Advertisement, error)
		Close() error
	}
	providerClient struct {
		*options
		sub *legs.Subscriber

		store     *ProviderClientStore
		publisher peer.AddrInfo

		adSel ipld.Node
	}
)

func NewProviderClient(provAddr peer.AddrInfo, o ...Option) (ProviderClient, error) {
	opts, err := newOptions(o...)
	if err != nil {
		return nil, err
	}

	h, err := libp2p.New()
	if err != nil {
		return nil, err
	}
	h.Peerstore().AddAddrs(provAddr.ID, provAddr.Addrs, time.Hour)

	store := newProviderClientStore()
	sub, err := legs.NewSubscriber(h, store.Batching, store.LinkSystem, opts.topic, nil)
	if err != nil {
		return nil, err
	}

	ssb := selectorbuilder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	adSel := ssb.ExploreRecursive(selector.RecursionLimitDepth(1), ssb.ExploreFields(
		func(efsb selectorbuilder.ExploreFieldsSpecBuilder) {
			efsb.Insert("PreviousID", ssb.ExploreRecursiveEdge())
		})).Node()

	return &providerClient{
		options:   opts,
		sub:       sub,
		publisher: provAddr,
		store:     store,
		adSel:     adSel,
	}, nil
}

func selectEntriesWithLimit(limit selector.RecursionLimit) datamodel.Node {
	ssb := selectorbuilder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	return ssb.ExploreRecursive(limit, ssb.ExploreFields(
		func(efsb selectorbuilder.ExploreFieldsSpecBuilder) {
			efsb.Insert("Next", ssb.ExploreRecursiveEdge())
		})).Node()
}

func (p *providerClient) GetAdvertisement(ctx context.Context, id cid.Cid) (*Advertisement, error) {
	// Sync the advertisement without entries first.
	id, err := p.syncAdWithRetry(ctx, id)
	if err != nil {
		return nil, err
	}

	// Load the synced advertisement from local store.
	ad, err := p.store.getAdvertisement(ctx, id)
	if err != nil {
		return nil, err
	}

	// Only sync its entries recursively if it is not a removal advertisement and has entries.
	if !ad.IsRemove && ad.HasEntries() {
		_, err = p.syncEntriesWithRetry(ctx, ad.Entries.root)
	}

	// Return the partially synced advertisement useful for output to client.
	return ad, err
}

func (p *providerClient) syncAdWithRetry(ctx context.Context, id cid.Cid) (cid.Cid, error) {
	var attempt uint64
	for {
		id, err := p.sub.Sync(ctx, p.publisher.ID, id, p.adSel, p.publisher.Addrs[0])
		if err == nil {
			return id, nil
		}
		if attempt > p.maxSyncRetry {
			log.Errorw("Reached maximum retry attempt while syncing ad", "cid", id, "attempt", attempt, "err", err)
			return cid.Undef, err
		}
		attempt++
		log.Infow("retrying ad sync", "attempt", attempt, "err", err)
		time.Sleep(p.syncRetryBackoff)
	}
}

func (p *providerClient) syncEntriesWithRetry(ctx context.Context, id cid.Cid) (cid.Cid, error) {
	var attempt uint64
	recurLimit := p.entriesRecurLimit
	for {
		sel := selectEntriesWithLimit(recurLimit)
		_, err := p.sub.Sync(ctx, p.publisher.ID, id, sel, p.publisher.Addrs[0])
		if err == nil {
			return id, nil
		}
		if attempt > p.maxSyncRetry {
			log.Errorw("Reached maximum retry attempt while syncing entries", "cid", id, "attempt", attempt, "err", err)
			return cid.Undef, err
		}
		nextMissing, visitedDepth, present := p.findNextMissingChunkLink(ctx, id)
		if !present {
			return id, nil
		}
		id = nextMissing
		attempt++
		remainingLimit := recurLimit.Depth() - visitedDepth
		recurLimit = selector.RecursionLimitDepth(remainingLimit)
		log.Infow("Retrying entries sync", "recurLimit", remainingLimit, "attempt", attempt, "err", err)
		time.Sleep(p.syncRetryBackoff)
	}
}

func (p *providerClient) findNextMissingChunkLink(ctx context.Context, next cid.Cid) (cid.Cid, int64, bool) {
	var depth int64
	for {
		if !isPresent(next) {
			return cid.Undef, depth, false
		}
		c, err := p.store.getNextChunkLink(ctx, next)
		if err == datastore.ErrNotFound {
			return next, depth, true
		}
		next = c
		depth++
	}
}

func (p *providerClient) Close() error {
	return p.sub.Close()
}
