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
	// Use test name as gossip topic name for uniqueness per test.
	topic := t.Name()

	subHost, err := libp2p.New()
	require.NoError(t, err)

	pubHost, err := libp2p.New()
	require.NoError(t, err)
	pubG, err := pubsub.NewGossipSub(ctx, pubHost,
		pubsub.WithDirectConnectTicks(1),
		pubsub.WithDirectPeers([]peer.AddrInfo{subHost.Peerstore().PeerInfo(subHost.ID())}),
	)
	require.NoError(t, err)

	pubT, err := pubG.Join(topic)
	require.NoError(t, err)

	subject, err := engine.New(
		engine.WithHost(pubHost),
		engine.WithPublisherKind(engine.DataTransferPublisher),
		engine.WithTopic(pubT),
		engine.WithTopicName(topic),
		engine.WithExtraGossipData(wantExtraGossipData),
	)
	require.NoError(t, err)
	err = subject.Start(ctx)
	require.NoError(t, err)
	defer subject.Shutdown()

	subG, err := pubsub.NewGossipSub(ctx, subHost,
		pubsub.WithDirectConnectTicks(1),
		pubsub.WithDirectPeers([]peer.AddrInfo{pubHost.Peerstore().PeerInfo(pubHost.ID())}),
	)
	require.NoError(t, err)

	subT, err := subG.Join(topic)
	require.NoError(t, err)

	subsc, err := subT.Subscribe()
	require.NoError(t, err)

	wantContextID := []byte("fish")
	subject.RegisterMultihashLister(func(ctx context.Context, contextID []byte) (provider.MultihashIterator, error) {
		if string(contextID) == string(wantContextID) {
			return &sliceMhIterator{
				mhs: mhs,
			}, nil
		}
		return nil, errors.New("not found")
	})

	// Await subscriber connection to publisher.
	requireTrueEventually(t, func() bool {
		pubPeers := pubG.ListPeers(topic)
		return len(pubPeers) == 1 && pubPeers[0] == subHost.ID()
	}, time.Second, 5*time.Second, "timed out waiting for subscriber peer ID to appear in publisher's gossipsub peer list")

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

	pubsubMsg, err := subsc.Next(ctx)
	require.NoError(t, err)
	require.Equal(t, pubsubMsg.GetFrom(), pubHost.ID())
	require.Equal(t, pubsubMsg.GetTopic(), topic)

	wantMessage := dtsync.Message{
		Cid:       gotPublishedAdCid,
		ExtraData: wantExtraGossipData,
	}
	wantMessage.SetAddrs(subject.Host().Addrs())

	gotMessage := dtsync.Message{}
	err = gotMessage.UnmarshalCBOR(bytes.NewBuffer(pubsubMsg.Data))
	require.NoError(t, err)
	requireEqualLegsMessage(t, wantMessage, gotMessage)

	gotRootCid, err := head.QueryRootCid(ctx, subHost, topic, pubHost.ID())
	require.NoError(t, err)
	require.Equal(t, gotPublishedAdCid, gotRootCid)

	ds := dssync.MutexWrap(datastore.NewMapDatastore())
	ls := cidlink.DefaultLinkSystem()
	store := &memstore.Store{}
	ls.SetReadStorage(store)
	ls.SetWriteStorage(store)

	sync, err := dtsync.NewSync(subHost, ds, ls, nil)
	require.NoError(t, err)
	syncer := sync.NewSyncer(subject.Host().ID(), topic)
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

func TestEngine_NotifyPutWithoutListerIsError(t *testing.T) {
	ctx := contextWithTimeout(t)
	subject, err := engine.New()
	require.NoError(t, err)
	err = subject.Start(ctx)
	require.NoError(t, err)
	defer subject.Shutdown()

	gotCid, err := subject.NotifyPut(ctx, []byte("fish"), metadata.BitswapMetadata)
	require.Error(t, err, provider.ErrNoMultihashLister)
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
	subject.RegisterMultihashLister(func(ctx context.Context, contextID []byte) (provider.MultihashIterator, error) {
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
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
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

func requireTrueEventually(t *testing.T, attempt func() bool, interval time.Duration, timeout time.Duration, msgAndArgs ...interface{}) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if attempt() {
			return
		}
		select {
		case <-ctx.Done():
			require.FailNow(t, "timed out awaiting eventual success", msgAndArgs...)
			return
		case <-ticker.C:
		}
	}
}
