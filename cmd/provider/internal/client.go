package internal

import (
	"context"
	"errors"
	"time"

	"github.com/filecoin-project/go-legs"
	"github.com/filecoin-project/go-legs/dtsync"
	"github.com/filecoin-project/go-legs/httpsync"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	selectorbuilder "github.com/ipld/go-ipld-prime/traversal/selector/builder"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/peer"
)

type (
	ProviderClient interface {
		GetAdvertisement(ctx context.Context, id cid.Cid) (*Advertisement, error)
		Close() error
	}
	providerGraphSyncClient struct {
		close  func() error
		syncer legs.Syncer
		store  *ProviderClientStore

		adSel  ipld.Node
		entSel ipld.Node
	}
)

func NewHttpProviderClient(provAddr peer.AddrInfo) (ProviderClient, error) {
	store := newProviderClientStore()
	hsync := httpsync.NewSync(store.LinkSystem, nil, nil)
	syncer, err := hsync.NewSyncer(provAddr.ID, provAddr.Addrs[0])
	if err != nil {
		return nil, err
	}
	return &providerGraphSyncClient{
		close: func() error {
			hsync.Close()
			return nil
		},
		syncer: syncer,
		store:  store,
	}, nil
}

func NewGraphSyncProviderClient(provAddr peer.AddrInfo, o ...Option) (ProviderClient, error) {

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
	dtSync, err := dtsync.NewSync(h, store.Batching, store.LinkSystem, nil)
	if err != nil {
		return nil, err
	}
	syncer := dtSync.NewSyncer(provAddr.ID, opts.topic)

	ssb := selectorbuilder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	adSel := ssb.ExploreRecursive(selector.RecursionLimitDepth(1), ssb.ExploreFields(
		func(efsb selectorbuilder.ExploreFieldsSpecBuilder) {
			efsb.Insert("PreviousID", ssb.ExploreRecursiveEdge())
		})).Node()

	entSel := ssb.ExploreRecursive(opts.entriesRecurLimit, ssb.ExploreFields(
		func(efsb selectorbuilder.ExploreFieldsSpecBuilder) {
			efsb.Insert("Next", ssb.ExploreRecursiveEdge())
		})).Node()

	return &providerGraphSyncClient{
		close:  dtSync.Close,
		syncer: syncer,
		store:  store,
		adSel:  adSel,
		entSel: entSel,
	}, nil
}

func (p *providerGraphSyncClient) GetAdvertisement(ctx context.Context, id cid.Cid) (*Advertisement, error) {
	if id == cid.Undef {
		head, err := p.syncer.GetHead(ctx)
		if err != nil {
			return nil, err
		}

		if head == cid.Undef {
			return nil, errors.New("no head advertisement exists")
		}
		id = head
	}

	// Sync the advertisement without entries first.
	if err := p.syncer.Sync(ctx, id, p.adSel); err != nil {
		return nil, err
	}

	// Load the synced advertisement from local store.
	ad, err := p.store.getAdvertisement(ctx, id)
	if err != nil {
		return nil, err
	}

	// Only sync its entries recursively if it is not a removal advertisement and has entries.
	if !ad.IsRemove && ad.HasEntries() {
		err = p.syncer.Sync(ctx, ad.Entries.root, p.entSel)
	}

	// Return the partially synced advertisement useful for output to client.
	return ad, err
}

func (p *providerGraphSyncClient) Close() error {
	return p.close()
}
