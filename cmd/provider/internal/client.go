package internal

import (
	"context"
	"time"

	"github.com/filecoin-project/go-legs"
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
	providerClient struct {
		sub *legs.Subscriber

		store     *ProviderClientStore
		publisher peer.AddrInfo

		adSel  ipld.Node
		entSel ipld.Node
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

	entSel := ssb.ExploreRecursive(opts.entriesRecurLimit, ssb.ExploreFields(
		func(efsb selectorbuilder.ExploreFieldsSpecBuilder) {
			efsb.Insert("Next", ssb.ExploreRecursiveEdge())
		})).Node()

	return &providerClient{
		sub:       sub,
		publisher: provAddr,
		store:     store,
		adSel:     adSel,
		entSel:    entSel,
	}, nil
}

func (p *providerClient) GetAdvertisement(ctx context.Context, id cid.Cid) (*Advertisement, error) {
	// Sync the advertisement without entries first.
	id, err := p.sub.Sync(ctx, p.publisher.ID, id, p.adSel, p.publisher.Addrs[0])
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
		_, err = p.sub.Sync(ctx, p.publisher.ID, ad.Entries.root, p.entSel, p.publisher.Addrs[0])
	}

	// Return the partially synced advertisement useful for output to client.
	return ad, err
}

func (p *providerClient) Close() error {
	return p.sub.Close()
}
