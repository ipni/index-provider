package engine_test

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	leveldb "github.com/ipfs/go-ds-leveldb"
	"github.com/ipfs/go-test/random"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/storage/memstore"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	selectorbuilder "github.com/ipld/go-ipld-prime/traversal/selector/builder"
	"github.com/ipni/go-libipni/announce/message"
	"github.com/ipni/go-libipni/dagsync/ipnisync"
	"github.com/ipni/go-libipni/ingest/schema"
	"github.com/ipni/go-libipni/metadata"
	provider "github.com/ipni/index-provider"
	"github.com/ipni/index-provider/engine"
	"github.com/ipni/index-provider/testutil"
	"github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

func TestEngine_NotifyRemoveWithUnknownContextIDIsError(t *testing.T) {
	subject, err := engine.New()
	require.NoError(t, err)
	c, err := subject.NotifyRemove(context.Background(), "", []byte("unknown context ID"))
	require.Equal(t, cid.Undef, c)
	require.Equal(t, provider.ErrContextIDNotFound, err)
}

func Test_NewEngineWithNoPublisherAndRoot(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	t.Cleanup(cancel)
	mhs := random.Multihashes(1)
	contextID := []byte("fish")

	subject, err := engine.New(engine.WithPublisherKind(engine.NoPublisher))
	require.NoError(t, err)
	require.NoError(t, subject.Start(ctx))

	subject.RegisterMultihashLister(func(_ context.Context, _ peer.ID, _ []byte) (provider.MultihashIterator, error) {
		return provider.SliceMultihashIterator(mhs), nil
	})
	adCid, err := subject.NotifyPut(ctx, nil, contextID, metadata.Default.New(metadata.Bitswap{}))
	require.NoError(t, err)
	require.NotNil(t, adCid)
	require.NotEqual(t, adCid, cid.Undef)

	err = subject.Shutdown()
	require.NoError(t, err)

	err = subject.Start(ctx)
	require.NoError(t, err)
}

func TestEngine_PublishLocal(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	t.Cleanup(cancel)

	mhs := random.Multihashes(42)

	subject, err := engine.New()
	require.NoError(t, err)
	err = subject.Start(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, subject.Shutdown())
	})

	chunkLnk, err := subject.Chunker().Chunk(ctx, provider.SliceMultihashIterator(mhs))
	require.NoError(t, err)

	md := metadata.Default.New(metadata.Bitswap{})
	mdBytes, err := md.MarshalBinary()
	require.NoError(t, err)
	wantAd := schema.Advertisement{
		Provider:  subject.Host().ID().String(),
		Addresses: testutil.MultiAddsToString(subject.Host().Addrs()),
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

func TestEngine_PublishWithLibp2pHttpPublisher(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	t.Cleanup(cancel)

	mhs := random.Multihashes(42)

	wantExtraGossipData := []byte("🐠")
	// Use test name as gossip topic name for uniqueness per test.
	topic := t.Name()

	pubHost, err := libp2p.New()
	require.NoError(t, err)
	t.Cleanup(func() {
		pubHost.Close()
	})

	subHost, err := libp2p.New()
	require.NoError(t, err)
	t.Cleanup(func() {
		subHost.Close()
	})

	// DirectConnectTicks set to 5 here, because if the gossub for the subHost
	// does not start within n ticks then they never peer. So, if
	// DirectConnectTicks is set to 1, the subHost gossipsub must start within
	// 1 second in order to peer. This can be confirmed by setting
	// DirectConnectTicks to 1 and placing a 1 second sleep anywhere between
	// this and the subHost gossipsub creation, and seeing that it always
	// fails. With slow CI machine this sometimes failes, so setting
	// DirectConnectTicks to 5 is enough to make it reliable.
	pubG, err := pubsub.NewGossipSub(ctx, pubHost,
		pubsub.WithDirectConnectTicks(10),
		pubsub.WithDirectPeers([]peer.AddrInfo{testutil.WaitForAddrs(subHost)}),
	)
	require.NoError(t, err)

	announceErrChan := make(chan error, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(announceErrChan)
		defer r.Body.Close()
		// Decode CID and originator addresses from message.
		an := message.Message{}
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
		// Since the message is coming from a libp2p publisher, the addresses
		// should include a p2p ID.
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
	t.Cleanup(func() {
		ts.Close()
	})

	pubT, err := pubG.Join(topic)
	require.NoError(t, err)

	subject, err := engine.New(
		engine.WithDirectAnnounce(ts.URL),
		engine.WithHost(pubHost),
		engine.WithPublisherKind(engine.Libp2pPublisher),
		engine.WithTopic(pubT),
		engine.WithTopicName(topic),
		engine.WithExtraGossipData(wantExtraGossipData),
	)
	require.NoError(t, err)
	err = subject.Start(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		subject.Shutdown()
	})

	subG, err := pubsub.NewGossipSub(ctx, subHost,
		pubsub.WithDirectConnectTicks(1),
		pubsub.WithDirectPeers([]peer.AddrInfo{testutil.WaitForAddrs(pubHost)}),
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
	require.Eventually(t, func() bool {
		pubPeers := pubG.ListPeers(topic)
		for i := range pubPeers {
			if pubPeers[i] == subHost.ID() {
				return true
			}
		}
		return false
	}, 10*time.Second, time.Second, "timed out waiting for subscriber peer ID to appear in publisher's gossipsub peer list")

	chunkLnk, err := subject.Chunker().Chunk(ctx, provider.SliceMultihashIterator(mhs))
	require.NoError(t, err)
	md := metadata.Default.New(metadata.Bitswap{})
	mdBytes, err := md.MarshalBinary()
	require.NoError(t, err)

	wantAd := schema.Advertisement{
		Provider:  subject.Host().ID().String(),
		Addresses: testutil.MultiAddsToString(subject.Host().Addrs()),
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

	wantMessage := message.Message{
		Cid:       gotPublishedAdCid,
		ExtraData: wantExtraGossipData,
	}
	wantMessage.SetAddrs(subject.Host().Addrs())

	gotMessage := message.Message{}
	err = gotMessage.UnmarshalCBOR(bytes.NewBuffer(pubsubMsg.Data))
	require.NoError(t, err)
	requireEqualDagsyncMessage(t, wantMessage, gotMessage)

	ls := cidlink.DefaultLinkSystem()
	store := &memstore.Store{}
	ls.SetReadStorage(store)
	ls.SetWriteStorage(store)

	sync := ipnisync.NewSync(ls, nil, ipnisync.ClientStreamHost(subHost))
	t.Cleanup(func() {
		sync.Close()
	})
	subjectInfo := peer.AddrInfo{
		ID: subject.Host().ID(),
	}
	syncer, err := sync.NewSyncer(subjectInfo)
	require.NoError(t, err)
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
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	t.Cleanup(cancel)
	subject, err := engine.New()
	require.NoError(t, err)
	err = subject.Start(ctx)
	require.NoError(t, err)
	defer subject.Shutdown()

	gotCid, err := subject.NotifyPut(ctx, nil, []byte("fish"), metadata.Default.New(metadata.Bitswap{}))
	require.Error(t, err, provider.ErrNoMultihashLister)
	require.Equal(t, cid.Undef, gotCid)
}

func TestEngine_NotifyPutThenNotifyRemove(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	t.Cleanup(cancel)

	mhs := random.Multihashes(42)

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

	gotPutAdCid, err := subject.NotifyPut(ctx, nil, wantContextID, metadata.Default.New(metadata.Bitswap{}))
	require.NoError(t, err)
	require.NotEqual(t, cid.Undef, gotPutAdCid)

	gotLatestAdCid, _, err := subject.GetLatestAdv(ctx)
	require.NoError(t, err)
	require.Equal(t, gotLatestAdCid, gotPutAdCid)

	gotRemoveAdCid, err := subject.NotifyRemove(ctx, "", wantContextID)
	require.NoError(t, err)
	require.NotEqual(t, gotPutAdCid, gotRemoveAdCid)

	gotLatestAfterRmAdCid, _, err := subject.GetLatestAdv(ctx)
	require.NoError(t, err)
	require.Equal(t, gotLatestAfterRmAdCid, gotRemoveAdCid)
	require.NotEqual(t, gotLatestAfterRmAdCid, gotLatestAdCid)
}

func TestEngine_NotifyRemoveWithDefaultProvider(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	t.Cleanup(cancel)

	mhs := random.Multihashes(42)

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

	_, err = subject.NotifyPut(ctx, nil, wantContextID, metadata.Default.New(metadata.Bitswap{}))
	require.NoError(t, err)
	gotRemoveAdCid, err := subject.NotifyRemove(ctx, "", wantContextID)
	require.NoError(t, err)

	ad, err := subject.GetAdv(ctx, gotRemoveAdCid)
	require.NoError(t, err)

	// verify that the provider is resolved to the default one when empty
	require.Equal(t, subject.ProviderID().String(), ad.Provider)
}

func TestEngine_NotifyRemoveWithCustomProvider(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	t.Cleanup(cancel)

	mhs := random.Multihashes(42)

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

	providerId, _, _ := random.Identity()
	providerAddrs, _ := multiaddr.NewMultiaddr("/ip4/0.0.0.0/tcp/1234/http")

	_, err = subject.NotifyPut(ctx, &peer.AddrInfo{ID: providerId, Addrs: []multiaddr.Multiaddr{providerAddrs}}, wantContextID, metadata.Default.New(metadata.Bitswap{}))
	require.NoError(t, err)
	gotRemoveAdCid, err := subject.NotifyRemove(ctx, providerId, wantContextID)
	require.NoError(t, err)

	ad, err := subject.GetAdv(ctx, gotRemoveAdCid)
	require.NoError(t, err)

	require.Equal(t, providerId.String(), ad.Provider)
}

func TestEngine_ProducesSingleChainForMultipleProviders(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	t.Cleanup(cancel)

	randMhs := random.Multihashes(84)
	mhs1 := randMhs[:42]
	mhs2 := randMhs[42:]

	provider1id, _, _ := random.Identity()
	provider1Addrs, _ := multiaddr.NewMultiaddr("/ip4/0.0.0.0/tcp/1234/http")
	provider2id, _, _ := random.Identity()
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
		}
		if string(contextID) == string(wantContextID2) && p == provider2id {
			return provider.SliceMultihashIterator(mhs2), nil
		}
		return nil, errors.New("not found")
	})

	gotPutAdCid1, err := subject.NotifyPut(ctx, &peer.AddrInfo{ID: provider1id, Addrs: []multiaddr.Multiaddr{provider1Addrs}}, wantContextID1, metadata.Default.New(metadata.Bitswap{}))
	require.NoError(t, err)
	require.NotEqual(t, cid.Undef, gotPutAdCid1)

	gotLatestAdCid, ad, err := subject.GetLatestAdv(ctx)
	require.NoError(t, err)
	require.Equal(t, gotLatestAdCid, gotPutAdCid1)
	require.Equal(t, ad.Provider, provider1id.String())
	require.Equal(t, ad.Addresses, []string{"/ip4/0.0.0.0/tcp/1234/http"})

	gotPutAdCid2, err := subject.NotifyPut(ctx, &peer.AddrInfo{ID: provider2id, Addrs: []multiaddr.Multiaddr{provider2Addrs}}, wantContextID2, metadata.Default.New(metadata.Bitswap{}))
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
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	t.Cleanup(cancel)

	mhs := random.Multihashes(42)

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
	gotPutAdCid1, err := subject.NotifyPut(ctx, nil, wantContextID, metadata.Default.New(metadata.Bitswap{}))
	require.NoError(t, err)
	require.NotEqual(t, cid.Undef, gotPutAdCid1)

	gotLatestAdCid, ad, err := subject.GetLatestAdv(ctx)
	require.NoError(t, err)
	require.Equal(t, gotLatestAdCid, gotPutAdCid1)
	require.Equal(t, ad.Provider, subject.ProviderID().String())
	require.Equal(t, ad.Addresses, subject.ProviderAddrs())
}

func TestEngine_VerifyErrAlreadyAdvertised(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	t.Cleanup(cancel)

	mhs := random.Multihashes(42)

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

	gotPutAdCid1, err := subject.NotifyPut(ctx, nil, wantContextID, metadata.Default.New(metadata.Bitswap{}))
	require.NoError(t, err)
	require.NotEqual(t, cid.Undef, gotPutAdCid1)

	_, err = subject.NotifyPut(ctx, nil, wantContextID, metadata.Default.New(metadata.Bitswap{}))
	require.Error(t, err, provider.ErrAlreadyAdvertised)

	p, _, _ := random.Identity()
	_, err = subject.NotifyPut(ctx, &peer.AddrInfo{ID: p}, wantContextID, metadata.Default.New(metadata.Bitswap{}))
	require.NoError(t, err, provider.ErrAlreadyAdvertised)
}

func TestEngine_ShouldHaveSameChunksInChunkerForSameCIDs(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	t.Cleanup(cancel)

	mhs := random.Multihashes(42)

	provider1id, _, _ := random.Identity()
	provider1Addrs, _ := multiaddr.NewMultiaddr("/ip4/0.0.0.0/tcp/1234/http")
	provider2id, _, _ := random.Identity()
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

	gotPutAdCid1, err := subject.NotifyPut(ctx, &peer.AddrInfo{ID: provider1id, Addrs: []multiaddr.Multiaddr{provider1Addrs}}, wantContextID, metadata.Default.New(metadata.Bitswap{}))
	require.NoError(t, err)
	require.NotEqual(t, cid.Undef, gotPutAdCid1)
	require.Equal(t, 1, subject.Chunker().Len())

	gotPutAdCid2, err := subject.NotifyPut(ctx, &peer.AddrInfo{ID: provider2id, Addrs: []multiaddr.Multiaddr{provider2Addrs}}, wantContextID, metadata.Default.New(metadata.Bitswap{}))
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
	pID, _ := peer.Decode("QmPxKFBM2A7VZURXZhZLCpEnhMFtZ7WSZwFLneFEiYneES")

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	t.Cleanup(cancel)
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
	_, err = subject.NotifyPut(ctx, nil, []byte("tree"), metadata.Default.New(metadata.Bitswap{}))
	require.Equal(t, provider.ErrAlreadyAdvertised, err)

	mmap := make(map[string][]multihash.Multihash)
	subject.RegisterMultihashLister(func(ctx context.Context, p peer.ID, contextID []byte) (provider.MultihashIterator, error) {
		if _, ok := mmap[string(contextID)]; !ok {
			mmap[string(contextID)] = random.Multihashes(42)
		}
		return provider.SliceMultihashIterator(mmap[string(contextID)]), nil
	})

	// publishing new add for the default provider
	adId, err := subject.NotifyPut(ctx, nil, []byte("pong"), metadata.Default.New(metadata.Bitswap{}))
	require.NoError(t, err)
	ad, _ = subject.GetAdv(ctx, adId)
	require.Equal(t, existingRoot, ad.PreviousID.(cidlink.Link).Cid)

	// try deleting record of the existing provider
	_, err = subject.NotifyRemove(ctx, pID, []byte("star"))
	require.NoError(t, err)

	// try publishing for new provider
	newPID, _, _ := random.Identity()
	_, err = subject.NotifyPut(ctx, &peer.AddrInfo{ID: newPID}, []byte("has"), metadata.Default.New(metadata.Bitswap{}))
	require.NoError(t, err)

}

func TestEngine_RegenrateEntryChunksFromOldDatastore(t *testing.T) {
	tempDir := t.TempDir()

	// copying testdata into the test dir
	testutil.CopyDir(t, filepath.Join(testutil.ThisDir(t), "../testdata/datastore.ds"), tempDir)

	ds, _ := leveldb.NewDatastore(tempDir, nil)
	defer ds.Close()

	// setting up engine with the configuration that was used to generate the datastore
	ma1, _ := multiaddr.NewMultiaddr("/ip6/::1/tcp/62698")
	ma2, _ := multiaddr.NewMultiaddr("/ip4/192.168.1.161/tcp/62695")
	ma3, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/62695")
	pID, _ := peer.Decode("QmPxKFBM2A7VZURXZhZLCpEnhMFtZ7WSZwFLneFEiYneES")

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	t.Cleanup(cancel)
	subject, err := engine.New(engine.WithDatastore(ds), engine.WithProvider(peer.AddrInfo{ID: pID, Addrs: []multiaddr.Multiaddr{ma1, ma2, ma3}}))
	require.NoError(t, err)
	err = subject.Start(ctx)
	require.NoError(t, err)
	defer subject.Shutdown()

	// Existing root advertisement
	existingRoot, _ := cid.Parse("baguqeeraix5q35zho3z2x5hqsa2iga3372qj4txsr4ooc2zvbyownka57gzq")

	// Gettig the first advertisement with entries
	ad, _ := subject.GetAdv(ctx, existingRoot)
	ad, _ = subject.GetAdv(ctx, ad.PreviousID.(cidlink.Link).Cid)

	mhs := make([]multihash.Multihash, 0)
	// register multihash lister that is going to return the extracted multihashes
	subject.RegisterMultihashLister(func(ctx context.Context, p peer.ID, contextID []byte) (provider.MultihashIterator, error) {
		return provider.SliceMultihashIterator(mhs), nil
	})

	// read cached entries and extract multihashes
	entryChunks := requireLoadEntryChunkFromEngine(t, subject, ad.Entries)
	for _, ch := range entryChunks {
		mhs = append(mhs, ch.Entries...)
	}

	// verify that link system can generate entries from old index
	err = subject.Datastore().Delete(ctx, datastore.NewKey(ad.Entries.(cidlink.Link).Cid.String()))
	require.NoError(t, err)
	err = subject.Datastore().Delete(ctx, datastore.NewKey("/cache/links/"+ad.Entries.(cidlink.Link).Cid.String()))
	require.NoError(t, err)
	requireLoadEntryChunkFromEngine(t, subject, ad.Entries)
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

func requireEqualDagsyncMessage(t *testing.T, want, got message.Message) {
	require.Equal(t, want.Cid, got.Cid)
	require.Equal(t, want.ExtraData, got.ExtraData)
	wantAddrs, err := want.GetAddrs()
	require.NoError(t, err)
	gotAddrs, err := got.GetAddrs()
	require.NoError(t, err)
	require.ElementsMatch(t, wantAddrs, gotAddrs)
}
