package internal

import (
	"context"
	"errors"
	"time"

	"github.com/filecoin-project/go-legs"
	"github.com/filecoin-project/go-legs/dtsync"
	"github.com/filecoin-project/go-legs/httpsync"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/datamodel"
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
		sel    datamodel.Node
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

func NewGraphSyncProviderClient(provAddr peer.AddrInfo, topic string, entryRecursionLimit int64) (ProviderClient, error) {
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
	syncer := dtSync.NewSyncer(provAddr.ID, topic)

	var erl selector.RecursionLimit
	if entryRecursionLimit <= 0 {
		erl = selector.RecursionLimitNone()
	} else {
		erl = selector.RecursionLimitDepth(entryRecursionLimit)
	}

	ssb := selectorbuilder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	oneAdWithEntries := ssb.ExploreUnion(
		ssb.ExploreRecursive(selector.RecursionLimitDepth(1), ssb.ExploreFields(
			func(efsb selectorbuilder.ExploreFieldsSpecBuilder) {
				efsb.Insert("PreviousID", ssb.ExploreRecursiveEdge())
			})),
		ssb.ExploreRecursive(erl, ssb.ExploreFields(
			func(efsb selectorbuilder.ExploreFieldsSpecBuilder) {
				efsb.Insert("Next", ssb.ExploreRecursiveEdge())
				efsb.Insert("Entries", ssb.ExploreRecursiveEdge())
			})),
	).Node()

	return &providerGraphSyncClient{
		close:  dtSync.Close,
		syncer: syncer,
		store:  store,
		sel:    oneAdWithEntries,
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

	if err := p.syncer.Sync(ctx, id, p.sel); err != nil {
		return nil, err
	}
	return p.store.getAdvertisement(ctx, id)
}

func (p *providerGraphSyncClient) Close() error {
	return p.close()
}
