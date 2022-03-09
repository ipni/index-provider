package engine_test

import (
	"bytes"
	"context"
	"errors"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/filecoin-project/go-legs/dtsync"
	"github.com/filecoin-project/go-legs/p2p/protocol/head"
	provider "github.com/filecoin-project/index-provider"
	"github.com/filecoin-project/index-provider/engine"
	"github.com/filecoin-project/index-provider/metadata"
	"github.com/filecoin-project/index-provider/testutil"
	"github.com/filecoin-project/storetheindex/api/v0/ingest/schema"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/storage/memstore"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	selectorbuilder "github.com/ipld/go-ipld-prime/traversal/selector/builder"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/peer"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/require"
)

const testTopicName = "test-topic"

func TestEngine_NotifyRemoveWithUnknownContextIDIsError(t *testing.T) {
	subject, err := engine.New()
	require.NoError(t, err)
	c, err := subject.NotifyRemove(context.Background(), []byte("unknown context ID"))
	require.Equal(t, cid.Undef, c)
	require.Equal(t, provider.ErrContextIDNotFound, err)
}

func TestEngine_PublishLocal(t *testing.T) {
	ctx := contextWithTimeout(t)
	rng := rand.New(rand.NewSource(1413))

	mhs, err := testutil.RandomMultihashes(rng, 42)
	require.NoError(t, err)

	subject, err := engine.New()
	require.NoError(t, err)
	err = subject.Start(ctx)
	require.NoError(t, err)
	defer subject.Shutdown()

	chunkLnk, err := subject.Chunker().Chunk(ctx, &sliceMhIterator{
		mhs: mhs,
	})
	require.NoError(t, err)

	wantAd, err := schema.NewAdvertisement(
		subject.Key(),
		nil,
		chunkLnk,
		[]byte("fish"),
		metadata.BitswapMetadata,
		false,
		subject.Host().ID().String(),
		multiAddsToString(subject.Host().Addrs()))
	require.NoError(t, err)

	gotPublishedAdCid, err := subject.PublishLocal(ctx, wantAd)
	require.NoError(t, err)
	require.NotEqual(t, cid.Undef, gotPublishedAdCid)

	gotLatestAdCid, gotLatestAd, err := subject.GetLatestAdv(ctx)
	require.NoError(t, err)
	require.True(t, ipld.DeepEqual(wantAd, gotLatestAd))
	require.Equal(t, gotLatestAdCid, gotPublishedAdCid)
}

func TestEngine_PublishWithDataTransferPublisher(t *testing.T) {
	ctx := contextWithTimeout(t)
	rng := rand.New(rand.NewSource(1413))

	mhs, err := testutil.RandomMultihashes(rng, 42)
	require.NoError(t, err)

	wantExtraGossipData := []byte("🐠")
	subject, err := engine.New(
		engine.WithPublisherKind(engine.DataTransferPublisher),
		engine.WithTopicName(testTopicName),
		engine.WithExtraGossipData(wantExtraGossipData),
	)
	require.NoError(t, err)
	err = subject.Start(ctx)
	require.NoError(t, err)
	defer subject.Shutdown()

	wantContextID := []byte("fish")
	subject.RegisterCallback(func(ctx context.Context, contextID []byte) (provider.MultihashIterator, error) {
		if string(contextID) == string(wantContextID) {
			return &sliceMhIterator{
				mhs: mhs,
			}, nil
		}
		return nil, errors.New("not found")
	})

	subHost, err := libp2p.New()
	require.NoError(t, err)
	subHost.Peerstore().AddAddrs(subject.Host().ID(), subject.Host().Addrs(), time.Hour)
	err = subHost.Connect(ctx, subject.Host().Peerstore().PeerInfo(subject.Host().ID()))
	require.NoError(t, err)

	g, err := pubsub.NewGossipSub(ctx, subHost,
		pubsub.WithPeerExchange(true),
		pubsub.WithFloodPublish(true),
		pubsub.WithDirectConnectTicks(1),
		pubsub.WithDirectPeers([]peer.AddrInfo{subject.Host().Peerstore().PeerInfo(subject.Host().ID())}),
	)
	require.NoError(t, err)

	pp, err := g.Join(testTopicName)
	require.NoError(t, err)
	subscribe, err := pp.Subscribe()
	require.NoError(t, err)

	chunkLnk, err := subject.Chunker().Chunk(ctx, &sliceMhIterator{
		mhs: mhs,
	})
	require.NoError(t, err)

	wantAd, err := schema.NewAdvertisement(
		subject.Key(),
		nil,
		chunkLnk,
		wantContextID,
		metadata.BitswapMetadata,
		false,
		subject.Host().ID().String(),
		multiAddsToString(subject.Host().Addrs()))
	require.NoError(t, err)

	gotPublishedAdCid, err := subject.Publish(ctx, wantAd)
	require.NoError(t, err)
	require.NotEqual(t, cid.Undef, gotPublishedAdCid)

	gotLatestAdCid, gotLatestAd, err := subject.GetLatestAdv(ctx)
	require.NoError(t, err)
	require.True(t, ipld.DeepEqual(wantAd, gotLatestAd))
	require.Equal(t, gotLatestAdCid, gotPublishedAdCid)

	next, err := subscribe.Next(ctx)
	require.NoError(t, err)
	require.Equal(t, next.GetFrom(), subject.Host().ID())
	require.Equal(t, next.GetTopic(), testTopicName)

	wantMessage := dtsync.Message{
		Cid:       gotPublishedAdCid,
		ExtraData: wantExtraGossipData,
	}
	wantMessage.SetAddrs(subject.Host().Addrs())

	gotMessage := dtsync.Message{}
	err = gotMessage.UnmarshalCBOR(bytes.NewBuffer(next.Data))
	require.NoError(t, err)
	requireEqualLegsMessage(t, wantMessage, gotMessage)

	gotRootCid, err := head.QueryRootCid(ctx, subHost, testTopicName, subject.Host().ID())
	require.NoError(t, err)
	require.Equal(t, gotPublishedAdCid, gotRootCid)

	ds := dssync.MutexWrap(datastore.NewMapDatastore())
	ls := cidlink.DefaultLinkSystem()
	store := &memstore.Store{}
	ls.SetReadStorage(store)
	ls.SetWriteStorage(store)

	sync, err := dtsync.NewSync(subHost, ds, ls, nil)
	require.NoError(t, err)
	syncer := sync.NewSyncer(subject.Host().ID(), testTopicName)
	gotHead, err := syncer.GetHead(ctx)
	require.NoError(t, err)
	require.Equal(t, gotLatestAdCid, gotHead)

	ssb := selectorbuilder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	adSel := ssb.ExploreRecursive(selector.RecursionLimitNone(), ssb.ExploreFields(
		func(efsb selectorbuilder.ExploreFieldsSpecBuilder) {
			efsb.Insert("PreviousID", ssb.ExploreRecursiveEdge())
			efsb.Insert("Next", ssb.ExploreRecursiveEdge())
			efsb.Insert("Entries", ssb.ExploreRecursiveEdge())
		})).Node()
	err = syncer.Sync(ctx, gotPublishedAdCid, adSel)

	require.NoError(t, err)
	_, err = store.Get(ctx, gotPublishedAdCid.KeyString())
	require.NoError(t, err)
}

func TestEngine_NotifyPutWithoutCallbackIsError(t *testing.T) {
	ctx := contextWithTimeout(t)
	subject, err := engine.New()
	require.NoError(t, err)
	err = subject.Start(ctx)
	require.NoError(t, err)
	defer subject.Shutdown()

	gotCid, err := subject.NotifyPut(ctx, []byte("fish"), metadata.BitswapMetadata)
	require.Error(t, err, provider.ErrNoCallback)
	require.Equal(t, cid.Undef, gotCid)
}

func TestEngine_NotifyPutThenNotifyRemove(t *testing.T) {
	ctx := contextWithTimeout(t)
	rng := rand.New(rand.NewSource(1413))

	mhs, err := testutil.RandomMultihashes(rng, 42)
	require.NoError(t, err)

	subject, err := engine.New()
	require.NoError(t, err)
	err = subject.Start(ctx)
	require.NoError(t, err)
	defer subject.Shutdown()

	wantContextID := []byte("fish")
	subject.RegisterCallback(func(ctx context.Context, contextID []byte) (provider.MultihashIterator, error) {
		if string(contextID) == string(wantContextID) {
			return &sliceMhIterator{
				mhs: mhs,
			}, nil
		}
		return nil, errors.New("not found")
	})

	gotPutAdCid, err := subject.NotifyPut(ctx, wantContextID, metadata.BitswapMetadata)
	require.NoError(t, err)
	require.NotEqual(t, cid.Undef, gotPutAdCid)

	gotLatestAdCid, _, err := subject.GetLatestAdv(ctx)
	require.NoError(t, err)
	require.Equal(t, gotLatestAdCid, gotPutAdCid)

	gotRemoveAdCid, err := subject.NotifyRemove(ctx, wantContextID)
	require.NoError(t, err)
	require.NotEqual(t, gotPutAdCid, gotRemoveAdCid)

	gotLatestAfterRmAdCid, _, err := subject.GetLatestAdv(ctx)
	require.NoError(t, err)
	require.Equal(t, gotLatestAfterRmAdCid, gotRemoveAdCid)
	require.NotEqual(t, gotLatestAfterRmAdCid, gotLatestAdCid)
}

func contextWithTimeout(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func requireEqualLegsMessage(t *testing.T, got, want dtsync.Message) {
	require.Equal(t, want.Cid, got.Cid)
	require.Equal(t, want.ExtraData, got.ExtraData)
	wantAddrs, err := want.GetAddrs()
	require.NoError(t, err)
	gotAddrs, err := got.GetAddrs()
	require.NoError(t, err)
	wantAddrsStr := multiAddsToString(wantAddrs)
	sort.Strings(wantAddrsStr)
	gotAddrsStr := multiAddsToString(gotAddrs)
	sort.Strings(gotAddrsStr)
	require.Equal(t, wantAddrsStr, gotAddrsStr)
}

func multiAddsToString(addrs []multiaddr.Multiaddr) []string {
	var rAddrs []string
	for _, addr := range addrs {
		rAddrs = append(rAddrs, addr.String())
	}
	return rAddrs
}
