package engine_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"path/filepath"
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
	leveldb "github.com/ipfs/go-ds-leveldb"
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
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
)

func TestEngine_NotifyRemoveWithUnknownContextIDIsError(t *testing.T) {
	subject, err := engine.New()
	require.NoError(t, err)
	c, err := subject.NotifyRemove(context.Background(), "", []byte("unknown context ID"))
	require.Equal(t, cid.Undef, c)
	require.Equal(t, provider.ErrContextIDNotFound, err)
}

func Test_NewEngineWithNoPublisherAndRoot(t *testing.T) {
	rng := rand.New(rand.NewSource(1413))
	ctx := contextWithTimeout(t)
	mhs := testutil.RandomMultihashes(t, rng, 1)
	contextID := []byte("fish")

	subject, err := engine.New(engine.WithPublisherKind(engine.NoPublisher))
	require.NoError(t, err)
	require.NoError(t, subject.Start(ctx))

	subject.RegisterMultihashLister(func(_ context.Context, _ peer.ID, _ []byte) (provider.MultihashIterator, error) {
		return provider.SliceMultihashIterator(mhs), nil
	})
	adCid, err := subject.NotifyPut(ctx, nil, contextID, metadata.New(metadata.Bitswap{}))
	require.NoError(t, err)
	require.NotNil(t, adCid)
	require.NotEqual(t, adCid, cid.Undef)

	err = subject.Shutdown()
	require.NoError(t, err)

	err = subject.Start(ctx)
	require.NoError(t, err)
}

func TestEngine_PublishLocal(t *testing.T) {
	ctx := contextWithTimeout(t)
	rng := rand.New(rand.NewSource(1413))

	mhs := testutil.RandomMultihashes(t, rng, 42)

	subject, err := engine.New()
	require.NoError(t, err)
	err = subject.Start(ctx)
	require.NoError(t, err)
	defer subject.Shutdown()

	chunkLnk, err := subject.Chunker().Chunk(ctx, provider.SliceMultihashIterator(mhs))
	require.NoError(t, err)

	md := metadata.New(metadata.Bitswap{})
	mdBytes, err := md.MarshalBinary()
	require.NoError(t, err)
	wantAd := schema.Advertisement{
		Provider:  subject.Host().ID().String(),
		Addresses: multiAddsToString(subject.Host().Addrs()),
		Entries:   chunkLnk,
		ContextID: []byte("fish"),
		Metadata:  mdBytes,
	}
	err = wantAd.Sign(subject.Key())
	require.NoError(t, err)

	gotPublishedAdCid, err := subject.PublishLocal(ctx, wantAd)
	require.NoError(t, err)
	require.NotEqual(t, cid.Undef, gotPublishedAdCid)

	gotLatestAdCid, gotLatestAd, err := subject.GetLatestAdv(ctx)
	require.NoError(t, err)
	require.Equal(t, &wantAd, gotLatestAd)
	require.Equal(t, gotLatestAdCid, gotPublishedAdCid)
}

func TestEngine_PublishWithDataTransferPublisher(t *testing.T) {
	ctx := contextWithTimeout(t)
	rng := rand.New(rand.NewSource(1413))

	mhs := testutil.RandomMultihashes(t, rng, 42)

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

	announceErrChan := make(chan error, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(announceErrChan)
		defer r.Body.Close()
		// Decode CID and originator addresses from message.
		an := dtsync.Message{}
		if err := an.UnmarshalCBOR(r.Body); err != nil {
			announceErrChan <- err
			http.Error(w, err.Error(), 400)
			return
		}

		if len(an.Addrs) == 0 {
			err = errors.New("must specify location to fetch on direct announcments")
			announceErrChan <- err
			http.Error(w, err.Error(), 400)
			return
		}

		addrs, err := an.GetAddrs()
		if err != nil {
			announceErrChan <- err
			http.Error(w, err.Error(), 400)
			return
		}

		ais, err := peer.AddrInfosFromP2pAddrs(addrs...)
		if err != nil {
			announceErrChan <- err
			http.Error(w, err.Error(), 400)
			return
		}
		if len(ais) > 1 {
			err = errors.New("peer id must be the same for all addresses")
			announceErrChan <- err
			http.Error(w, err.Error(), 400)
			return
		}
		addrInfo := ais[0]
		if addrInfo.ID != pubHost.ID() {
			err = errors.New("wrong publisher ID")
			announceErrChan <- err
			http.Error(w, err.Error(), 400)
			return
		}

		w.WriteHeader(http.StatusNoContent)
	}))
	defer ts.Close()

	subject, err := engine.New(
		engine.WithDirectAnnounce(ts.URL),
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
	subject.RegisterMultihashLister(func(ctx context.Context, p peer.ID, contextID []byte) (provider.MultihashIterator, error) {
		if string(contextID) == string(wantContextID) {
			return provider.SliceMultihashIterator(mhs), nil
		}
		return nil, errors.New("not found")
	})

	// Await subscriber connection to publisher.
	requireTrueEventually(t, func() bool {
		pubPeers := pubG.ListPeers(topic)
		return len(pubPeers) == 1 && pubPeers[0] == subHost.ID()
	}, time.Second, 8*time.Second, "timed out waiting for subscriber peer ID to appear in publisher's gossipsub peer list")

	chunkLnk, err := subject.Chunker().Chunk(ctx, provider.SliceMultihashIterator(mhs))
	require.NoError(t, err)
	md := metadata.New(metadata.Bitswap{})
	mdBytes, err := md.MarshalBinary()
	require.NoError(t, err)

	wantAd := schema.Advertisement{
		Provider:  subject.Host().ID().String(),
		Addresses: multiAddsToString(subject.Host().Addrs()),
		Entries:   chunkLnk,
		ContextID: wantContextID,
		Metadata:  mdBytes,
	}
	err = wantAd.Sign(subject.Key())
	require.NoError(t, err)

	gotPublishedAdCid, err := subject.Publish(ctx, wantAd)
	require.NoError(t, err)
	require.NotEqual(t, cid.Undef, gotPublishedAdCid)

	// Explicitly check for an error from test server receiving the announce
	// request. This way an error is detected whether or not a failure to send
	// an HTTP announce message causes engine.Publish() to fail.
	err = <-announceErrChan
	require.NoError(t, err)

	gotLatestAdCid, gotLatestAd, err := subject.GetLatestAdv(ctx)
	require.NoError(t, err)
	require.Equal(t, &wantAd, gotLatestAd)
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
	syncer := sync.NewSyncer(subject.Host().ID(), topic, rate.NewLimiter(100, 10))
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

	gotCid, err := subject.NotifyPut(ctx, nil, []byte("fish"), metadata.New(metadata.Bitswap{}))
	require.Error(t, err, provider.ErrNoMultihashLister)
	require.Equal(t, cid.Undef, gotCid)
}

func TestEngine_NotifyPutThenNotifyRemoveAndRemoveAll(t *testing.T) {
	ctx := contextWithTimeout(t)
	rng := rand.New(rand.NewSource(1413))

	mhs := testutil.RandomMultihashes(t, rng, 42)

	subject, err := engine.New()
	require.NoError(t, err)
	err = subject.Start(ctx)
	require.NoError(t, err)
	defer subject.Shutdown()

	wantContextID := []byte("fish")
	subject.RegisterMultihashLister(func(ctx context.Context, p peer.ID, contextID []byte) (provider.MultihashIterator, error) {
		if string(contextID) == string(wantContextID) {
			return provider.SliceMultihashIterator(mhs), nil
		}
		return nil, errors.New("not found")
	})

	// Put
	gotPutAdCid, err := subject.NotifyPut(ctx, nil, wantContextID, metadata.New(metadata.Bitswap{}))
	require.NoError(t, err)
	require.NotEqual(t, cid.Undef, gotPutAdCid)

	gotLatestAdCid, _, err := subject.GetLatestAdv(ctx)
	require.NoError(t, err)
	require.Equal(t, gotLatestAdCid, gotPutAdCid)

	// Remove
	gotRemoveAdCid, err := subject.NotifyRemove(ctx, "", wantContextID)
	require.NoError(t, err)
	require.NotEqual(t, gotPutAdCid, gotRemoveAdCid)

	gotLatestAfterRmAdCid, _, err := subject.GetLatestAdv(ctx)
	require.NoError(t, err)
	require.Equal(t, gotLatestAfterRmAdCid, gotRemoveAdCid)
	require.NotEqual(t, gotLatestAfterRmAdCid, gotLatestAdCid)

	// Remove All
	gotRemoveAllAdCid, err := subject.NotifyRemove(ctx, "", nil)
	require.NoError(t, err)
	require.NotEqual(t, gotRemoveAdCid, gotRemoveAllAdCid)

	gotLatestAfterRmAllAdCid, _, err := subject.GetLatestAdv(ctx)
	require.NoError(t, err)
	require.Equal(t, gotRemoveAllAdCid, gotLatestAfterRmAllAdCid)
	require.NotEqual(t, gotLatestAfterRmAdCid, gotLatestAfterRmAllAdCid)

	gotLatestAfterRmAllAd, err := subject.GetAdv(ctx, gotLatestAfterRmAllAdCid)
	require.NoError(t, err)
	verifyAd(t, ctx, subject, createAd(t, []byte{}, subject.ProviderID().String(), nil, "bafkreehdwdcefgh4dqkjv67uzcmw7oje", true, gotRemoveAdCid.String()), gotLatestAfterRmAllAd)
}

func TestEngine_IndexConsistencyTest(t *testing.T) {
	// This test verifies the index consistency after adding and removing advertised content.
	// The main goal is to make sure that with added complexity, the index is cleaned up and remains consistent
	// after put / remove / removeAll operations.
	ctx := contextWithTimeout(t)
	rng := rand.New(rand.NewSource(1413))

	subject, err := engine.New()
	require.NoError(t, err)
	err = subject.Start(ctx)
	require.NoError(t, err)
	defer subject.Shutdown()

	mhs := testutil.RandomMultihashes(t, rng, 42)
	mhs2 := testutil.RandomMultihashes(t, rng, 42)
	provider2ID := testutil.NewID(t)
	provider3ID := testutil.NewID(t)
	provider3Bytes, err := provider3ID.Marshal()
	require.NoError(t, err)
	provider2Bytes, err := provider2ID.Marshal()
	require.NoError(t, err)
	provider1Bytes, err := subject.ProviderID().Marshal()
	require.NoError(t, err)

	commonContextID := []byte("fish")
	provider3OnlyContextID := []byte("bird")
	subject.RegisterMultihashLister(func(ctx context.Context, p peer.ID, contextID []byte) (provider.MultihashIterator, error) {
		if bytes.Equal(contextID, commonContextID) {
			return provider.SliceMultihashIterator(mhs), nil
		} else if bytes.Equal(contextID, provider3OnlyContextID) {
			return provider.SliceMultihashIterator(mhs2), nil
		}
		return nil, errors.New("not found")
	})

	// Advertising content for the three providers. Default provider and provider 2 advertise a single piece of content with the same context IDs
	// while provider 3 advertises two pieces of content with different context IDs
	_, err = subject.NotifyPut(ctx, nil, commonContextID, metadata.New(metadata.Bitswap{}))
	require.NoError(t, err)
	_, err = subject.NotifyPut(ctx, &peer.AddrInfo{ID: provider2ID}, commonContextID, metadata.New(metadata.Bitswap{}))
	require.NoError(t, err)
	_, err = subject.NotifyPut(ctx, &peer.AddrInfo{ID: provider3ID}, commonContextID, metadata.New(metadata.Bitswap{}))
	require.NoError(t, err)
	_, err = subject.NotifyPut(ctx, &peer.AddrInfo{ID: provider3ID}, provider3OnlyContextID, metadata.New(metadata.Bitswap{}))
	require.NoError(t, err)

	ds := subject.Datastore()

	// verifying that for each provider + contextID there is a record in keyCid index
	cidBytes, err := ds.Get(ctx, datastore.NewKey("map/keyCid/fish"))
	require.NoError(t, err)
	cidBytes2, err := ds.Get(ctx, datastore.NewKey("map/keyCid/"+provider2ID.String()+"/fish"))
	require.NoError(t, err)
	cidBytes3, err := ds.Get(ctx, datastore.NewKey("map/keyCid/"+provider3ID.String()+"/fish"))
	require.NoError(t, err)
	cidBytes4, err := ds.Get(ctx, datastore.NewKey("map/keyCid/"+provider3ID.String()+"/bird"))
	require.NoError(t, err)
	require.Equal(t, cidBytes, cidBytes2)
	require.Equal(t, cidBytes, cidBytes3)
	require.NotEqual(t, cidBytes, cidBytes4)

	// verifying that for each provider + contextID there is a record in keyMD index
	_, err = ds.Get(ctx, datastore.NewKey("map/keyMD/fish"))
	require.NoError(t, err)
	_, err = ds.Get(ctx, datastore.NewKey("map/keyMD/"+provider2ID.String()+"/fish"))
	require.NoError(t, err)
	_, err = ds.Get(ctx, datastore.NewKey("map/keyMD/"+provider3ID.String()+"/fish"))
	require.NoError(t, err)
	_, err = ds.Get(ctx, datastore.NewKey("map/keyMD/"+provider3ID.String()+"/bird"))
	require.NoError(t, err)

	_, c, err := cid.CidFromBytes(cidBytes)
	require.NoError(t, err)

	// there should be no record in the legacy cidKey index
	_, err = ds.Get(ctx, datastore.NewKey("map/cidKey/"+c.String()))
	require.Error(t, err, datastore.ErrNotFound)

	// there should be 3 records in the cidProvAndKey linked list for "fish" context ID
	pAndCBytes, err := ds.Get(ctx, datastore.NewKey("map/cidProvAndKey/"+c.String()))
	require.NoError(t, err)

	pAndC := &engine.ProviderAndContext{}
	err = json.Unmarshal(pAndCBytes, &pAndC)
	require.NoError(t, err)

	require.Equal(t, commonContextID, pAndC.ContextID)
	require.Equal(t, provider3Bytes, pAndC.Provider)

	pAndC = pAndC.Next
	require.Equal(t, commonContextID, pAndC.ContextID)
	require.Equal(t, provider2Bytes, pAndC.Provider)

	pAndC = pAndC.Next
	require.Equal(t, commonContextID, pAndC.ContextID)
	require.Equal(t, provider1Bytes, pAndC.Provider)

	require.Nil(t, pAndC.Next)

	// there should be one record in the cidProvAndKey index for the "bird" context ID
	_, c, err = cid.CidFromBytes(cidBytes4)
	require.NoError(t, err)
	pAndCBytes, err = ds.Get(ctx, datastore.NewKey("map/cidProvAndKey/"+c.String()))
	require.NoError(t, err)

	pAndC = &engine.ProviderAndContext{}
	err = json.Unmarshal(pAndCBytes, &pAndC)
	require.NoError(t, err)

	require.Equal(t, provider3OnlyContextID, pAndC.ContextID)
	require.Equal(t, provider3Bytes, pAndC.Provider)
	require.Nil(t, pAndC.Next)

	// REMOVE ONE - removing a record for provider 2
	_, err = subject.NotifyRemove(ctx, provider2ID, commonContextID)
	require.NoError(t, err)
	_, err = ds.Get(ctx, datastore.NewKey("map/keyCid/"+provider2ID.String()+"/fish"))
	require.Error(t, err, datastore.ErrNotFound)
	_, err = ds.Get(ctx, datastore.NewKey("map/keyMD/"+provider2ID.String()+"/fish"))
	require.Error(t, err, datastore.ErrNotFound)

	// there should be only 2 records left in the cidProvAndKey index for the "fish" fontext id
	_, c, err = cid.CidFromBytes(cidBytes)
	require.NoError(t, err)
	pAndCBytes, err = ds.Get(ctx, datastore.NewKey("map/cidProvAndKey/"+c.String()))
	require.NoError(t, err)

	pAndC = &engine.ProviderAndContext{}
	err = json.Unmarshal(pAndCBytes, &pAndC)
	require.NoError(t, err)

	require.Equal(t, commonContextID, pAndC.ContextID)
	require.Equal(t, provider3Bytes, pAndC.Provider)

	pAndC = pAndC.Next
	require.Equal(t, commonContextID, pAndC.ContextID)
	require.Equal(t, provider1Bytes, pAndC.Provider)
	require.Nil(t, pAndC.Next)

	// REMOVE ALL - removing all records for provider 3
	_, err = subject.NotifyRemove(ctx, provider3ID, nil)
	require.NoError(t, err)
	_, err = ds.Get(ctx, datastore.NewKey("map/keyCid/"+provider3ID.String()+"/fish"))
	require.Error(t, err, datastore.ErrNotFound)
	_, err = ds.Get(ctx, datastore.NewKey("map/keyCid/"+provider3ID.String()+"/bird"))
	require.Error(t, err, datastore.ErrNotFound)
	_, err = ds.Get(ctx, datastore.NewKey("map/keyMD/"+provider3ID.String()+"/fish"))
	require.Error(t, err, datastore.ErrNotFound)
	_, err = ds.Get(ctx, datastore.NewKey("map/keyMD/"+provider3ID.String()+"/bird"))
	require.Error(t, err, datastore.ErrNotFound)

	// there should be only 1 record in the cidProvAndKey index left for the "fish" context
	_, c, err = cid.CidFromBytes(cidBytes)
	require.NoError(t, err)
	pAndCBytes, err = ds.Get(ctx, datastore.NewKey("map/cidProvAndKey/"+c.String()))
	require.NoError(t, err)

	pAndC = &engine.ProviderAndContext{}
	err = json.Unmarshal(pAndCBytes, &pAndC)
	require.NoError(t, err)

	require.Equal(t, commonContextID, pAndC.ContextID)
	require.Equal(t, provider1Bytes, pAndC.Provider)
	require.Nil(t, pAndC.Next)

	// there should be no records in the cidProvAndKey index left for the "bird" context
	_, c, err = cid.CidFromBytes(cidBytes4)
	require.NoError(t, err)
	_, err = ds.Get(ctx, datastore.NewKey("map/cidProvAndKey/"+c.String()))
	require.Error(t, err, datastore.ErrNotFound)

	// REMOVE ONE - removing a record for the default provider
	_, err = subject.NotifyRemove(ctx, "", commonContextID)
	require.NoError(t, err)
	_, err = ds.Get(ctx, datastore.NewKey("map/keyCid/fish"))
	require.Error(t, err, datastore.ErrNotFound)
	_, err = ds.Get(ctx, datastore.NewKey("map/keyMD/fish"))
	require.Error(t, err, datastore.ErrNotFound)

	// there should be no records left in the cidProvAndKey index for the "fish" fontext id
	_, c, err = cid.CidFromBytes(cidBytes)
	require.NoError(t, err)
	pAndCBytes, err = ds.Get(ctx, datastore.NewKey("map/cidProvAndKey/"+c.String()))
	require.Error(t, err, datastore.ErrNotFound)
}

func TestEngine_NotifyRemoveWithDefaultProvider(t *testing.T) {
	ctx := contextWithTimeout(t)
	rng := rand.New(rand.NewSource(1413))

	mhs := testutil.RandomMultihashes(t, rng, 42)

	subject, err := engine.New()
	require.NoError(t, err)
	err = subject.Start(ctx)
	require.NoError(t, err)
	defer subject.Shutdown()

	wantContextID := []byte("fish")
	subject.RegisterMultihashLister(func(ctx context.Context, p peer.ID, contextID []byte) (provider.MultihashIterator, error) {
		if string(contextID) == string(wantContextID) {
			return provider.SliceMultihashIterator(mhs), nil
		}
		return nil, errors.New("not found")
	})

	_, err = subject.NotifyPut(ctx, nil, wantContextID, metadata.New(metadata.Bitswap{}))
	require.NoError(t, err)
	gotRemoveAdCid, err := subject.NotifyRemove(ctx, "", wantContextID)
	require.NoError(t, err)

	ad, err := subject.GetAdv(ctx, gotRemoveAdCid)
	require.NoError(t, err)

	// verify that the provider is resolved to the default one when empty
	require.Equal(t, subject.ProviderID().String(), ad.Provider)
}

func TestEngine_NotifyRemoveWithCustomProvider(t *testing.T) {
	ctx := contextWithTimeout(t)
	rng := rand.New(rand.NewSource(1413))

	mhs := testutil.RandomMultihashes(t, rng, 42)

	subject, err := engine.New()
	require.NoError(t, err)
	err = subject.Start(ctx)
	require.NoError(t, err)
	defer subject.Shutdown()

	wantContextID := []byte("fish")
	subject.RegisterMultihashLister(func(ctx context.Context, p peer.ID, contextID []byte) (provider.MultihashIterator, error) {
		if string(contextID) == string(wantContextID) {
			return provider.SliceMultihashIterator(mhs), nil
		}
		return nil, errors.New("not found")
	})

	providerId := testutil.NewID(t)
	providerAddrs, _ := multiaddr.NewMultiaddr("/ip4/0.0.0.0/tcp/1234/http")

	_, err = subject.NotifyPut(ctx, &peer.AddrInfo{ID: providerId, Addrs: []multiaddr.Multiaddr{providerAddrs}}, wantContextID, metadata.New(metadata.Bitswap{}))
	require.NoError(t, err)
	gotRemoveAdCid, err := subject.NotifyRemove(ctx, providerId, wantContextID)
	require.NoError(t, err)

	ad, err := subject.GetAdv(ctx, gotRemoveAdCid)
	require.NoError(t, err)

	require.Equal(t, providerId.String(), ad.Provider)
}

func TestEngine_ProducesSingleChainForMultipleProviders(t *testing.T) {
	ctx := contextWithTimeout(t)
	rng := rand.New(rand.NewSource(1413))

	mhs1 := testutil.RandomMultihashes(t, rng, 42)
	mhs2 := testutil.RandomMultihashes(t, rng, 42)

	provider1id := testutil.NewID(t)
	provider1Addrs, _ := multiaddr.NewMultiaddr("/ip4/0.0.0.0/tcp/1234/http")
	provider2id := testutil.NewID(t)
	provider2Addrs, _ := multiaddr.NewMultiaddr("/ip4/0.0.0.0/tcp/4321/http")

	subject, err := engine.New()
	require.NoError(t, err)
	err = subject.Start(ctx)
	require.NoError(t, err)
	defer subject.Shutdown()

	wantContextID1 := []byte("fish")
	wantContextID2 := []byte("bird")
	subject.RegisterMultihashLister(func(ctx context.Context, p peer.ID, contextID []byte) (provider.MultihashIterator, error) {

		if string(contextID) == string(wantContextID1) && p == provider1id {
			return provider.SliceMultihashIterator(mhs1), nil
		} else if string(contextID) == string(wantContextID2) && p == provider2id {
			return provider.SliceMultihashIterator(mhs2), nil
		}
		return nil, errors.New("not found")
	})

	gotPutAdCid1, err := subject.NotifyPut(ctx, &peer.AddrInfo{ID: provider1id, Addrs: []multiaddr.Multiaddr{provider1Addrs}}, wantContextID1, metadata.New(metadata.Bitswap{}))
	require.NoError(t, err)
	require.NotEqual(t, cid.Undef, gotPutAdCid1)

	gotLatestAdCid, ad, err := subject.GetLatestAdv(ctx)
	require.NoError(t, err)
	require.Equal(t, gotLatestAdCid, gotPutAdCid1)
	require.Equal(t, ad.Provider, provider1id.String())
	require.Equal(t, ad.Addresses, []string{"/ip4/0.0.0.0/tcp/1234/http"})

	gotPutAdCid2, err := subject.NotifyPut(ctx, &peer.AddrInfo{ID: provider2id, Addrs: []multiaddr.Multiaddr{provider2Addrs}}, wantContextID2, metadata.New(metadata.Bitswap{}))
	require.NoError(t, err)
	require.NotEqual(t, cid.Undef, gotPutAdCid2)

	gotLatestAdCid, ad, err = subject.GetLatestAdv(ctx)
	require.NoError(t, err)
	require.Equal(t, gotLatestAdCid, gotPutAdCid2)
	require.Equal(t, ad.Provider, provider2id.String())
	require.Equal(t, ad.Addresses, []string{"/ip4/0.0.0.0/tcp/4321/http"})
	require.Equal(t, ad.PreviousID.(cidlink.Link).Cid, gotPutAdCid1)
}

func TestEngine_NotifyPutUseDefaultProviderAndAddressesWhenNoneGiven(t *testing.T) {
	ctx := contextWithTimeout(t)
	rng := rand.New(rand.NewSource(1413))

	mhs := testutil.RandomMultihashes(t, rng, 42)

	subject, err := engine.New()
	require.NoError(t, err)
	err = subject.Start(ctx)
	require.NoError(t, err)
	defer subject.Shutdown()

	wantContextID := []byte("fish")
	subject.RegisterMultihashLister(func(ctx context.Context, p peer.ID, contextID []byte) (provider.MultihashIterator, error) {
		if string(contextID) == string(wantContextID) {
			return provider.SliceMultihashIterator(mhs), nil
		}
		return nil, errors.New("not found")
	})

	// addresses should be ignored as provider is an empty string
	gotPutAdCid1, err := subject.NotifyPut(ctx, nil, wantContextID, metadata.New(metadata.Bitswap{}))
	require.NoError(t, err)
	require.NotEqual(t, cid.Undef, gotPutAdCid1)

	gotLatestAdCid, ad, err := subject.GetLatestAdv(ctx)
	require.NoError(t, err)
	require.Equal(t, gotLatestAdCid, gotPutAdCid1)
	require.Equal(t, ad.Provider, subject.ProviderID().String())
	require.Equal(t, ad.Addresses, subject.ProviderAddrs())
}

func TestEngine_VerifyErrAlreadyAdvertised(t *testing.T) {
	ctx := contextWithTimeout(t)
	rng := rand.New(rand.NewSource(1413))

	mhs := testutil.RandomMultihashes(t, rng, 42)

	subject, err := engine.New()
	require.NoError(t, err)
	err = subject.Start(ctx)
	require.NoError(t, err)
	defer subject.Shutdown()

	wantContextID := []byte("fish")
	subject.RegisterMultihashLister(func(ctx context.Context, p peer.ID, contextID []byte) (provider.MultihashIterator, error) {
		if string(contextID) == string(wantContextID) {
			return provider.SliceMultihashIterator(mhs), nil
		}
		return nil, errors.New("not found")
	})

	gotPutAdCid1, err := subject.NotifyPut(ctx, nil, wantContextID, metadata.New(metadata.Bitswap{}))
	require.NoError(t, err)
	require.NotEqual(t, cid.Undef, gotPutAdCid1)

	_, err = subject.NotifyPut(ctx, nil, wantContextID, metadata.New(metadata.Bitswap{}))
	require.Error(t, err, provider.ErrAlreadyAdvertised)

	p := testutil.NewID(t)
	_, err = subject.NotifyPut(ctx, &peer.AddrInfo{ID: p}, wantContextID, metadata.New(metadata.Bitswap{}))
	require.NoError(t, err, provider.ErrAlreadyAdvertised)
}

func TestEngine_ShouldHaveSameChunksInChunkerForSameCIDs(t *testing.T) {
	ctx := contextWithTimeout(t)
	rng := rand.New(rand.NewSource(1413))

	mhs := testutil.RandomMultihashes(t, rng, 42)

	provider1id := testutil.NewID(t)
	provider1Addrs, _ := multiaddr.NewMultiaddr("/ip4/0.0.0.0/tcp/1234/http")
	provider2id := testutil.NewID(t)
	provider2Addrs, _ := multiaddr.NewMultiaddr("/ip4/0.0.0.0/tcp/4321/http")

	subject, err := engine.New()
	require.NoError(t, err)
	err = subject.Start(ctx)
	require.NoError(t, err)
	defer subject.Shutdown()

	wantContextID := []byte("fish")
	subject.RegisterMultihashLister(func(ctx context.Context, p peer.ID, contextID []byte) (provider.MultihashIterator, error) {
		if string(contextID) == string(wantContextID) {
			return provider.SliceMultihashIterator(mhs), nil
		}
		return nil, errors.New("not found")
	})

	gotPutAdCid1, err := subject.NotifyPut(ctx, &peer.AddrInfo{ID: provider1id, Addrs: []multiaddr.Multiaddr{provider1Addrs}}, wantContextID, metadata.New(metadata.Bitswap{}))
	require.NoError(t, err)
	require.NotEqual(t, cid.Undef, gotPutAdCid1)
	require.Equal(t, 1, subject.Chunker().Len())

	gotPutAdCid2, err := subject.NotifyPut(ctx, &peer.AddrInfo{ID: provider2id, Addrs: []multiaddr.Multiaddr{provider2Addrs}}, wantContextID, metadata.New(metadata.Bitswap{}))
	require.NoError(t, err)
	require.NotEqual(t, cid.Undef, gotPutAdCid2)
	require.Equal(t, 1, subject.Chunker().Len())
	ad1, _ := subject.GetAdv(ctx, gotPutAdCid1)
	ad2, _ := subject.GetAdv(ctx, gotPutAdCid2)

	require.Equal(t, ad1.Entries, ad2.Entries)
}

func createAd(t *testing.T, contextID []byte, provider string, addrs []string, entries string, isRm bool, prevId string) *schema.Advertisement {
	var prevLink ipld.Link
	if prevId != "" {
		p, err := cid.Parse(prevId)
		require.NoError(t, err)
		prevLink = cidlink.Link{Cid: p}
	}
	entriesCID, err := cid.Parse(entries)
	require.NoError(t, err)
	return &schema.Advertisement{ContextID: contextID, Provider: provider, Addresses: addrs, IsRm: isRm, PreviousID: prevLink, Entries: cidlink.Link{Cid: entriesCID}}
}

func TestEngine_DatastoreBackwardsCompatibilityTest(t *testing.T) {
	tempDir := t.TempDir()

	// copying testdata into the test dir
	testutil.CopyDir(t, filepath.Join(testutil.ThisDir(t), "../testdata/datastore.ds"), tempDir)

	ds, _ := leveldb.NewDatastore(tempDir, nil)
	defer ds.Close()

	// setting up engine with the configuration that was used to generate the datastore
	ma1, _ := multiaddr.NewMultiaddr("/ip6/::1/tcp/62698")
	ma2, _ := multiaddr.NewMultiaddr("/ip4/192.168.1.161/tcp/62695")
	ma3, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/62695")
	pID, _ := peer.IDFromString("QmPxKFBM2A7VZURXZhZLCpEnhMFtZ7WSZwFLneFEiYneES")

	ctx := contextWithTimeout(t)
	subject, err := engine.New(engine.WithDatastore(ds), engine.WithProvider(peer.AddrInfo{ID: pID, Addrs: []multiaddr.Multiaddr{ma1, ma2, ma3}}))
	require.NoError(t, err)
	err = subject.Start(ctx)
	require.NoError(t, err)
	defer subject.Shutdown()

	// walking back the ad chain
	existingRoot, _ := cid.Parse("baguqeeraix5q35zho3z2x5hqsa2iga3372qj4txsr4ooc2zvbyownka57gzq")
	ad, err := subject.GetAdv(ctx, existingRoot)
	require.NoError(t, err)

	verifyAd(t, ctx, subject, createAd(t, []byte("byte"), "QmPxKFBM2A7VZURXZhZLCpEnhMFtZ7WSZwFLneFEiYneES", []string{"/ip6/::1/tcp/62698", "/ip4/192.168.1.161/tcp/62695", "/ip4/127.0.0.1/tcp/62695"}, "bafkreehdwdcefgh4dqkjv67uzcmw7oje", true, "baguqeerahjpn2qtt3qwbzcwbytruxqtqjoitv7zv4d7dksppw7swl4rt3dqq"), ad)

	ad, err = subject.GetAdv(ctx, ad.PreviousID.(cidlink.Link).Cid)
	require.NoError(t, err)
	verifyAd(t, ctx, subject, createAd(t, []byte("tree"), "QmPxKFBM2A7VZURXZhZLCpEnhMFtZ7WSZwFLneFEiYneES", []string{"/ip6/::1/tcp/62698", "/ip4/192.168.1.161/tcp/62695", "/ip4/127.0.0.1/tcp/62695"}, "baguqeerapsyb2pobz7vmcc2i4f5pe7chjs7bhgmfueqiuqwzsscmliwop45q", false, "baguqeerattk5otsdvvlpnno3a5x5ruowvjt7iqxtwqrwv5jhqbkq62mloc7a"), ad)

	ad, err = subject.GetAdv(ctx, ad.PreviousID.(cidlink.Link).Cid)
	require.NoError(t, err)
	verifyAd(t, ctx, subject, createAd(t, []byte("star"), "QmPxKFBM2A7VZURXZhZLCpEnhMFtZ7WSZwFLneFEiYneES", []string{"/ip6/::1/tcp/62698", "/ip4/192.168.1.161/tcp/62695", "/ip4/127.0.0.1/tcp/62695"}, "baguqeeraurc5vmmbt33jqiij27k2m4ajrcmlvitaohkbczgikctg4ajekieq", false, "baguqeerazskx6vqznzcjhuucvrmspr53dkrwkz3o7mhamdtdw6plrwsavuwq"), ad)

	ad, err = subject.GetAdv(ctx, ad.PreviousID.(cidlink.Link).Cid)
	require.NoError(t, err)
	verifyAd(t, ctx, subject, createAd(t, []byte("ping"), "QmPxKFBM2A7VZURXZhZLCpEnhMFtZ7WSZwFLneFEiYneES", []string{"/ip6/::1/tcp/62698", "/ip4/192.168.1.161/tcp/62695", "/ip4/127.0.0.1/tcp/62695"}, "baguqeeraihesu6l6ob3c5gn3wwpmju356ius4354uh6aefmb3sfk3fnpjhdq", false, "baguqeeraqafrbk3ffaeaipcy7zms7wivqgj6l52f2xw7hwnl7w4qqhxrv2oq"), ad)

	ad, err = subject.GetAdv(ctx, ad.PreviousID.(cidlink.Link).Cid)
	require.NoError(t, err)
	verifyAd(t, ctx, subject, createAd(t, []byte("byte"), "QmPxKFBM2A7VZURXZhZLCpEnhMFtZ7WSZwFLneFEiYneES", []string{"/ip6/::1/tcp/62698", "/ip4/192.168.1.161/tcp/62695", "/ip4/127.0.0.1/tcp/62695"}, "baguqeera7k7x5kayh2yzp44wq6cd3i2z24o5rxyyedkwtgmwkaq63npcig4q", false, ""), ad)

	// try to create a deplicate to make sure that they are processed correctly against the previously created datastore
	_, err = subject.NotifyPut(ctx, nil, []byte("tree"), metadata.New(metadata.Bitswap{}))
	require.Equal(t, provider.ErrAlreadyAdvertised, err)

	mmap := make(map[string][]multihash.Multihash)
	rng := rand.New(rand.NewSource(1413))
	subject.RegisterMultihashLister(func(ctx context.Context, p peer.ID, contextID []byte) (provider.MultihashIterator, error) {
		if _, ok := mmap[string(contextID)]; !ok {
			mmap[string(contextID)] = testutil.RandomMultihashes(t, rng, 42)
		}
		return provider.SliceMultihashIterator(mmap[string(contextID)]), nil
	})

	// publishing new add for the default provider
	adId, err := subject.NotifyPut(ctx, nil, []byte("pong"), metadata.New(metadata.Bitswap{}))
	require.NoError(t, err)
	ad, _ = subject.GetAdv(ctx, adId)
	require.Equal(t, existingRoot, ad.PreviousID.(cidlink.Link).Cid)

	// try deleting record of the existing provider
	_, err = subject.NotifyRemove(ctx, pID, []byte("star"))
	require.NoError(t, err)

	// try publishing for new provider
	newPID := testutil.NewID(t)
	_, err = subject.NotifyPut(ctx, &peer.AddrInfo{ID: newPID}, []byte("has"), metadata.New(metadata.Bitswap{}))
	require.NoError(t, err)
}

func verifyAd(t *testing.T, ctx context.Context, subject *engine.Engine, expected, actual *schema.Advertisement) {
	require.Equal(t, expected.ContextID, actual.ContextID)
	require.Equal(t, expected.Provider, actual.Provider)
	require.Equal(t, expected.Addresses, actual.Addresses)
	require.Equal(t, expected.Entries, actual.Entries)
	require.Equal(t, expected.IsRm, actual.IsRm)
	require.Equal(t, expected.PreviousID, actual.PreviousID)
	if !actual.IsRm {
		chunk, err := subject.Chunker().GetRawCachedChunk(ctx, actual.Entries)
		require.NoError(t, err)
		require.NotNil(t, chunk)
	}
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
