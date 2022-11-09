package reframe_test

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net/http/httptest"
	"sort"
	"testing"
	"time"

	"github.com/filecoin-project/index-provider/engine"
	"github.com/filecoin-project/index-provider/metadata"
	mock_provider "github.com/filecoin-project/index-provider/mock"
	reframelistener "github.com/filecoin-project/index-provider/reframe"
	"github.com/filecoin-project/index-provider/testutil"
	"github.com/golang/mock/gomock"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-delegated-routing/client"
	"github.com/ipfs/go-delegated-routing/gen/proto"
	"github.com/ipfs/go-delegated-routing/server"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

var defaultMetadata metadata.Metadata = metadata.Default.New(metadata.Bitswap{})

func testNonceGen() []byte {
	return []byte{1, 2, 3, 4, 5}
}

func newProvider(t *testing.T, pID peer.ID) *client.Provider {
	ma, err := multiaddr.NewMultiaddr("/ip4/0.0.0.0/tcp/5001")
	require.NoError(t, err)

	return &client.Provider{
		Peer: peer.AddrInfo{
			ID:    pID,
			Addrs: []multiaddr.Multiaddr{ma},
		},
		ProviderProto: []client.TransferProtocol{{Codec: multicodec.TransportBitswap}},
	}
}

// TestReframeMultihashLister verifies that multihash lister returns correct number of multihashes in deterministic order
func TestReframeMultihashLister(t *testing.T) {
	cids := make(map[cid.Cid]struct{})
	cids[newCid("test1")] = struct{}{}
	cids[newCid("test2")] = struct{}{}
	cids[newCid("test3")] = struct{}{}

	_, _, pID := testutil.GenerateKeysAndIdentity(t)

	lister := &reframelistener.ReframeMultihashLister{
		CidFetcher: func(contextID []byte) (map[cid.Cid]struct{}, error) {
			if string(contextID) == "test" {
				return cids, nil
			}
			return nil, fmt.Errorf("unknown context id")
		},
	}

	iterator, err := lister.MultihashLister(context.Background(), pID, []byte("test"))

	require.NoError(t, err)
	mhs := make([]multihash.Multihash, 0, len(cids))
	for {
		next, err := iterator.Next()
		if err != nil {
			break
		}
		mhs = append(mhs, next)
	}

	require.Equal(t, 3, len(mhs))
	require.Equal(t, []multihash.Multihash{newCid("test1").Hash(), newCid("test2").Hash(), newCid("test3").Hash()}, mhs)
}

func TestRetryWithBackOffKeepsRetryingOnError(t *testing.T) {
	// this test verifies that RetryWithBackOff keeps retrying as long as an error is returned or until the max number of attenpts is reached
	start := time.Now()
	attempts := 0
	err := reframelistener.RetryWithBackoff(func() error {
		attempts++
		return fmt.Errorf("test")
	}, time.Second, 3)

	elapsed := time.Since(start)
	require.Equal(t, 3, attempts)
	// allow some error (1sec)
	require.True(t, elapsed-3*time.Second < time.Second)
	require.Equal(t, fmt.Errorf("test"), err)
}

func TestRetryWithBackOffStopsRetryingOnSuccess(t *testing.T) {
	start := time.Now()
	attempts := 0
	err := reframelistener.RetryWithBackoff(func() error {
		attempts++
		return nil
	}, time.Second, 3)

	elapsed := time.Since(start)
	require.Equal(t, 1, attempts)
	require.True(t, elapsed < time.Second)
	require.Nil(t, err)
}

func TestProvideRoundtrip(t *testing.T) {
	ttl := 24 * time.Hour
	chunkSize := 2
	snapshotSize := 1000

	h, err := libp2p.New()
	require.NoError(t, err)
	priv, _, pID := testutil.GenerateKeysAndIdentity(t)
	ctx := context.Background()

	engine, err := engine.New(engine.WithHost(h), engine.WithPublisherKind(engine.DataTransferPublisher))
	require.NoError(t, err)
	err = engine.Start(ctx)
	defer engine.Shutdown()
	require.NoError(t, err)

	ip, err := reframelistener.New(ctx, engine, ttl, chunkSize, snapshotSize, "", nil, datastore.NewMapDatastore(), testNonceGen)
	require.NoError(t, err)

	errorClient, errorServer := createClientAndServer(t, ip, nil, nil)
	defer errorServer.Close()

	testCid1 := newCid("test1")
	testCid2 := newCid("test2")
	testCid3 := newCid("test3")
	testCid4 := newCid("test4")
	testCid5 := newCid("test5")

	_, err = errorClient.Provide(ctx, []cid.Cid{testCid1}, time.Hour)
	require.Error(t, err, "should get sync error on unsigned provide request.")
	errorServer.Close()

	client, server := createClientAndServer(t, ip, newProvider(t, pID), priv)
	defer server.Close()

	provideMany(t, client, ctx, []cid.Cid{testCid1, testCid2})

	c, _, err := engine.GetLatestAdv(ctx)
	require.NoError(t, err)
	require.Equal(t, cid.Undef, c, "there should have been no advertisement published")

	// two more cids should have pushed the first ad through
	provideMany(t, client, ctx, []cid.Cid{testCid3, testCid4})

	c, firstAd, err := engine.GetLatestAdv(ctx)
	require.NoError(t, err)
	require.NotEqual(t, cid.Undef, c)
	require.NotNil(t, firstAd)
	require.Equal(t, firstAd.ContextID, generateContextID([]string{testCid1.String(), testCid2.String()}, testNonceGen()))
	require.Nil(t, firstAd.PreviousID)

	// pushing the second ad through
	provide(t, client, ctx, testCid5)
	_, secondAd, err := engine.GetLatestAdv(ctx)
	require.NoError(t, err)
	require.NotNil(t, secondAd.PreviousID)
	require.Equal(t, secondAd.ContextID, generateContextID([]string{testCid3.String(), testCid4.String()}, testNonceGen()))
}

func TestProvideRoundtripWithRemove(t *testing.T) {
	ttl := time.Second
	chunkSize := 2
	snapshotSize := 1000

	h, err := libp2p.New()
	require.NoError(t, err)
	priv, _, pID := testutil.GenerateKeysAndIdentity(t)
	ctx := context.Background()

	engine, err := engine.New(engine.WithHost(h), engine.WithPublisherKind(engine.DataTransferPublisher))
	require.NoError(t, err)
	err = engine.Start(ctx)
	defer engine.Shutdown()
	require.NoError(t, err)

	ip, err := reframelistener.New(ctx, engine, ttl, chunkSize, snapshotSize, "", nil, datastore.NewMapDatastore(), testNonceGen)
	require.NoError(t, err)

	errorClient, errorServer := createClientAndServer(t, ip, nil, nil)
	defer errorServer.Close()

	testCid1 := newCid("test1")
	testCid2 := newCid("test2")
	testCid3 := newCid("test3")

	_, err = errorClient.Provide(ctx, []cid.Cid{testCid1}, time.Hour)
	require.Error(t, err, "should get sync error on unsigned provide request.")
	errorServer.Close()

	client, server := createClientAndServer(t, ip, newProvider(t, pID), priv)
	defer server.Close()

	provideMany(t, client, ctx, []cid.Cid{testCid1, testCid2, testCid3})
	time.Sleep(ttl)
	provide(t, client, ctx, testCid1)

	_, replacementAd, err := engine.GetLatestAdv(ctx)
	require.NoError(t, err)
	require.NotNil(t, replacementAd)
	require.Equal(t, replacementAd.ContextID, generateContextID([]string{testCid1.String()}, testNonceGen()))
	require.False(t, replacementAd.IsRm)
	require.NotNil(t, replacementAd.PreviousID)

	rmAd, err := engine.GetAdv(ctx, replacementAd.PreviousID.(cidlink.Link).Cid)
	require.NoError(t, err)
	require.Equal(t, rmAd.ContextID, generateContextID([]string{testCid1.String(), testCid2.String()}, testNonceGen()))
	require.True(t, rmAd.IsRm)
	require.NotNil(t, rmAd.PreviousID)

	firstAd, err := engine.GetAdv(ctx, rmAd.PreviousID.(cidlink.Link).Cid)
	require.NoError(t, err)
	require.NotNil(t, firstAd)
	require.Equal(t, firstAd.ContextID, generateContextID([]string{testCid1.String(), testCid2.String()}, testNonceGen()))
	require.False(t, firstAd.IsRm)
	require.Nil(t, firstAd.PreviousID)
}

func TestAdvertiseTwoChunksWithOneCidInEach(t *testing.T) {
	ttl := 24 * time.Hour
	chunkSize := 1
	snapshotSize := 1000

	priv, _, pID := testutil.GenerateKeysAndIdentity(t)

	ctx := context.Background()
	defer ctx.Done()
	testCid1 := newCid("test1")
	testCid2 := newCid("test2")
	testCid3 := newCid("test3")
	prov := newProvider(t, pID)

	mc := gomock.NewController(t)
	defer mc.Finish()
	mockEng := mock_provider.NewMockInterface(mc)

	mockEng.EXPECT().RegisterMultihashLister(gomock.Any())
	mockEng.EXPECT().NotifyPut(gomock.Any(), gomock.Eq(&prov.Peer), gomock.Eq(generateContextID([]string{testCid1.String()}, testNonceGen())), gomock.Eq(defaultMetadata))
	mockEng.EXPECT().NotifyPut(gomock.Any(), gomock.Eq(&prov.Peer), gomock.Eq(generateContextID([]string{testCid2.String()}, testNonceGen())), gomock.Eq(defaultMetadata))

	ip, err := reframelistener.New(ctx, mockEng, ttl, chunkSize, snapshotSize, "", nil, datastore.NewMapDatastore(), testNonceGen)
	require.NoError(t, err)

	c, s := createClientAndServer(t, ip, prov, priv)
	defer s.Close()

	provideMany(t, c, ctx, []cid.Cid{testCid1, testCid2, testCid3})
}

func TestAdvertiseUsingAddrsFromParameters(t *testing.T) {
	ttl := 24 * time.Hour
	chunkSize := 1
	snapshotSize := 1000

	priv, _, pID := testutil.GenerateKeysAndIdentity(t)

	ctx := context.Background()
	defer ctx.Done()

	testCid1 := newCid("test1")
	testCid2 := newCid("test2")
	testCid3 := newCid("test3")
	prov := newProvider(t, pID)

	mc := gomock.NewController(t)
	defer mc.Finish()
	mockEng := mock_provider.NewMockInterface(mc)

	randomMultiaddr, err := multiaddr.NewMultiaddr("/ip4/0.0.0.0/tcp/1001")
	require.NoError(t, err)

	mockEng.EXPECT().RegisterMultihashLister(gomock.Any())
	mockEng.EXPECT().NotifyPut(gomock.Any(), gomock.Eq(&peer.AddrInfo{ID: pID, Addrs: []multiaddr.Multiaddr{randomMultiaddr}}), gomock.Eq(generateContextID([]string{testCid1.String()}, testNonceGen())), gomock.Eq(defaultMetadata))
	mockEng.EXPECT().NotifyPut(gomock.Any(), gomock.Eq(&peer.AddrInfo{ID: pID, Addrs: []multiaddr.Multiaddr{randomMultiaddr}}), gomock.Eq(generateContextID([]string{testCid2.String()}, testNonceGen())), gomock.Eq(defaultMetadata))

	ip, err := reframelistener.New(ctx, mockEng, ttl, chunkSize, snapshotSize, pID.String(), []string{"/ip4/0.0.0.0/tcp/1001"}, datastore.NewMapDatastore(), testNonceGen)
	require.NoError(t, err)

	c, s := createClientAndServer(t, ip, prov, priv)
	defer s.Close()

	provideMany(t, c, ctx, []cid.Cid{testCid1, testCid2, testCid3})
}

func TestProvideRegistersCidInDatastore(t *testing.T) {
	ttl := 24 * time.Hour
	chunkSize := 2
	snapshotSize := 1000

	priv, _, pID := testutil.GenerateKeysAndIdentity(t)

	ctx := context.Background()
	defer ctx.Done()
	testCid1 := newCid("test1")
	prov := newProvider(t, pID)

	mc := gomock.NewController(t)
	defer mc.Finish()
	mockEng := mock_provider.NewMockInterface(mc)

	mockEng.EXPECT().RegisterMultihashLister(gomock.Any())

	listener, err := reframelistener.New(ctx, mockEng, ttl, chunkSize, snapshotSize, "", nil, datastore.NewMapDatastore(), testNonceGen)
	require.NoError(t, err)

	c, s := createClientAndServer(t, listener, prov, priv)
	defer s.Close()

	provide(t, c, ctx, testCid1)

	require.True(t, reframelistener.CidExist(ctx, listener, testCid1, false))

	// verifying that the CID has a current timestamp
	tt, err := reframelistener.GetCidTimestampFromDatastore(ctx, listener, testCid1)
	require.NoError(t, err)
	require.True(t, time.Since(tt) < time.Second)
	require.Equal(t, []cid.Cid{testCid1}, reframelistener.GetExpiryQueue(ctx, listener))
}

func TestCidsAreOrderedByArrivalInExpiryQueue(t *testing.T) {
	ttl := 24 * time.Hour
	chunkSize := 1000
	snapshotSize := 1000

	priv, _, pID := testutil.GenerateKeysAndIdentity(t)

	ctx := context.Background()
	defer ctx.Done()
	testCid1 := newCid("test1")
	testCid2 := newCid("test2")
	testCid3 := newCid("test3")
	prov := newProvider(t, pID)

	mc := gomock.NewController(t)
	defer mc.Finish()
	mockEng := mock_provider.NewMockInterface(mc)

	mockEng.EXPECT().RegisterMultihashLister(gomock.Any())

	listener, err := reframelistener.New(ctx, mockEng, ttl, chunkSize, snapshotSize, "", nil, datastore.NewMapDatastore(), testNonceGen)
	require.NoError(t, err)

	c, s := createClientAndServer(t, listener, prov, priv)
	defer s.Close()

	provide(t, c, ctx, testCid1)
	provide(t, c, ctx, testCid2)
	provide(t, c, ctx, testCid3)
	require.Equal(t, []cid.Cid{testCid3, testCid2, testCid1}, reframelistener.GetExpiryQueue(ctx, listener))

	provide(t, c, ctx, testCid2)
	require.Equal(t, []cid.Cid{testCid2, testCid3, testCid1}, reframelistener.GetExpiryQueue(ctx, listener))
}

func TestFullChunkAdvertisedAndRegisteredInDatastore(t *testing.T) {
	ttl := 24 * time.Hour
	chunkSize := 2
	snapshotSize := 1000

	priv, _, pID := testutil.GenerateKeysAndIdentity(t)

	ctx := context.Background()
	defer ctx.Done()
	testCid1 := newCid("test1")
	testCid2 := newCid("test2")
	testCid3 := newCid("test3")

	prov := newProvider(t, pID)

	mc := gomock.NewController(t)
	defer mc.Finish()
	mockEng := mock_provider.NewMockInterface(mc)

	mockEng.EXPECT().RegisterMultihashLister(gomock.Any())
	mockEng.EXPECT().NotifyPut(gomock.Any(), gomock.Eq(&prov.Peer), gomock.Eq(generateContextID([]string{testCid1.String(), testCid2.String()}, testNonceGen())), gomock.Eq(defaultMetadata))

	listener, err := reframelistener.New(ctx, mockEng, ttl, chunkSize, snapshotSize, "", nil, datastore.NewMapDatastore(), testNonceGen)
	require.NoError(t, err)

	c, s := createClientAndServer(t, listener, prov, priv)
	defer s.Close()

	provideMany(t, c, ctx, []cid.Cid{testCid1, testCid2, testCid3})

	require.True(t, reframelistener.CidExist(ctx, listener, testCid1, true))
	require.True(t, reframelistener.CidExist(ctx, listener, testCid2, true))
	require.True(t, reframelistener.CidExist(ctx, listener, testCid3, false))
	require.True(t, reframelistener.ChunkExists(ctx, listener, []cid.Cid{testCid1, testCid2}, testNonceGen))
	require.Equal(t, []cid.Cid{testCid3, testCid2, testCid1}, reframelistener.GetExpiryQueue(ctx, listener))
}

func TestRemovedChunkIsRemovedFromIndexes(t *testing.T) {
	ttl := time.Second
	chunkSize := 2
	snapshotSize := 1000

	priv, _, pID := testutil.GenerateKeysAndIdentity(t)

	ctx := context.Background()
	defer ctx.Done()
	testCid1 := newCid("test1")
	testCid2 := newCid("test2")
	testCid3 := newCid("test3")
	prov := newProvider(t, pID)

	mc := gomock.NewController(t)
	defer mc.Finish()
	mockEng := mock_provider.NewMockInterface(mc)

	mockEng.EXPECT().RegisterMultihashLister(gomock.Any())
	mockEng.EXPECT().NotifyPut(gomock.Any(), gomock.Eq(&prov.Peer), gomock.Eq(generateContextID([]string{testCid1.String(), testCid2.String()}, testNonceGen())), gomock.Eq(defaultMetadata))
	mockEng.EXPECT().NotifyRemove(gomock.Any(), gomock.Eq(pID), gomock.Eq(generateContextID([]string{testCid1.String(), testCid2.String()}, testNonceGen())))

	listener, err := reframelistener.New(ctx, mockEng, ttl, chunkSize, snapshotSize, "", nil, datastore.NewMapDatastore(), testNonceGen)
	require.NoError(t, err)

	c, s := createClientAndServer(t, listener, prov, priv)
	defer s.Close()

	provideMany(t, c, ctx, []cid.Cid{testCid1, testCid2})
	time.Sleep(ttl)
	provide(t, c, ctx, testCid3)

	require.True(t, reframelistener.ChunkNotExist(ctx, listener, []cid.Cid{testCid1, testCid2}, testNonceGen))
	require.True(t, reframelistener.CidExist(ctx, listener, testCid3, false))
	require.True(t, reframelistener.CidNotExist(ctx, listener, testCid1))
	require.True(t, reframelistener.CidNotExist(ctx, listener, testCid2))
	require.Equal(t, []cid.Cid{testCid3}, reframelistener.GetExpiryQueue(ctx, listener))
}

func TestAdvertiseOneChunkWithTwoCidsInIt(t *testing.T) {
	ttl := 24 * time.Hour
	chunkSize := 2
	snapshotSize := 1000

	priv, _, pID := testutil.GenerateKeysAndIdentity(t)

	ctx := context.Background()
	defer ctx.Done()
	testCid1 := newCid("test1")
	testCid2 := newCid("test2")
	testCid3 := newCid("test3")
	prov := newProvider(t, pID)

	mc := gomock.NewController(t)
	defer mc.Finish()
	mockEng := mock_provider.NewMockInterface(mc)

	mockEng.EXPECT().RegisterMultihashLister(gomock.Any())
	mockEng.EXPECT().NotifyPut(gomock.Any(), gomock.Eq(&prov.Peer), gomock.Eq(generateContextID([]string{testCid1.String(), testCid2.String()}, testNonceGen())), gomock.Eq(defaultMetadata))

	listener, err := reframelistener.New(ctx, mockEng, ttl, chunkSize, snapshotSize, "", nil, datastore.NewMapDatastore(), testNonceGen)
	require.NoError(t, err)

	c, s := createClientAndServer(t, listener, prov, priv)
	defer s.Close()

	provideMany(t, c, ctx, []cid.Cid{testCid1, testCid2, testCid3})
}

func TestDoNotReAdvertiseRepeatedCids(t *testing.T) {
	ttl := 24 * time.Hour
	chunkSize := 1
	snapshotSize := 1000

	priv, _, pID := testutil.GenerateKeysAndIdentity(t)

	ctx := context.Background()
	defer ctx.Done()
	testCid1 := newCid("test1")
	testCid2 := newCid("test2")
	prov := newProvider(t, pID)

	mc := gomock.NewController(t)
	defer mc.Finish()
	mockEng := mock_provider.NewMockInterface(mc)

	mockEng.EXPECT().RegisterMultihashLister(gomock.Any())
	mockEng.EXPECT().NotifyPut(gomock.Any(), gomock.Eq(&prov.Peer), gomock.Eq(generateContextID([]string{testCid1.String()}, testNonceGen())), gomock.Eq(defaultMetadata))

	listener, err := reframelistener.New(ctx, mockEng, ttl, chunkSize, snapshotSize, "", nil, datastore.NewMapDatastore(), testNonceGen)
	require.NoError(t, err)

	c, s := createClientAndServer(t, listener, prov, priv)
	defer s.Close()

	provide(t, c, ctx, testCid1)
	provide(t, c, ctx, testCid1)
	provide(t, c, ctx, testCid2)
	provide(t, c, ctx, testCid2)
	provide(t, c, ctx, testCid2)
}

func TestAdvertiseExpiredCidsIfProvidedAgain(t *testing.T) {
	ttl := time.Second
	chunkSize := 1
	snapshotSize := 1000

	priv, _, pID := testutil.GenerateKeysAndIdentity(t)

	ctx := context.Background()
	defer ctx.Done()
	testCid1 := newCid("test1")
	testCid2 := newCid("test2")
	testCid3 := newCid("test3")
	prov := newProvider(t, pID)

	mc := gomock.NewController(t)
	defer mc.Finish()
	mockEng := mock_provider.NewMockInterface(mc)

	mockEng.EXPECT().RegisterMultihashLister(gomock.Any())
	mockEng.EXPECT().NotifyPut(gomock.Any(), gomock.Eq(&prov.Peer), gomock.Eq(generateContextID([]string{testCid1.String()}, testNonceGen())), gomock.Eq(defaultMetadata))
	mockEng.EXPECT().NotifyRemove(gomock.Any(), gomock.Eq(pID), gomock.Eq(generateContextID([]string{testCid1.String()}, testNonceGen())))
	mockEng.EXPECT().NotifyPut(gomock.Any(), gomock.Eq(&prov.Peer), gomock.Eq(generateContextID([]string{testCid2.String()}, testNonceGen())), gomock.Eq(defaultMetadata))
	mockEng.EXPECT().NotifyPut(gomock.Any(), gomock.Eq(&prov.Peer), gomock.Eq(generateContextID([]string{testCid1.String()}, testNonceGen())), gomock.Eq(defaultMetadata))

	listener, err := reframelistener.New(ctx, mockEng, ttl, chunkSize, snapshotSize, "", nil, datastore.NewMapDatastore(), testNonceGen)
	require.NoError(t, err)

	c, s := createClientAndServer(t, listener, prov, priv)
	defer s.Close()

	provide(t, c, ctx, testCid1)
	time.Sleep(time.Second)
	// by the time testCid2 gets published, testCid1 already expired that should geenrate a remove ad
	provide(t, c, ctx, testCid2)
	provide(t, c, ctx, testCid1)
	provide(t, c, ctx, testCid3)
}

func TestRemoveExpiredCidAndReadvertiseChunk(t *testing.T) {
	ttl := 3 * time.Second
	chunkSize := 2
	snapshotSize := 1000

	priv, _, pID := testutil.GenerateKeysAndIdentity(t)

	ctx := context.Background()
	defer ctx.Done()
	testCid1 := newCid("test1")
	testCid2 := newCid("test2")
	testCid3 := newCid("test3")
	prov := newProvider(t, pID)

	mc := gomock.NewController(t)
	defer mc.Finish()
	mockEng := mock_provider.NewMockInterface(mc)

	mockEng.EXPECT().RegisterMultihashLister(gomock.Any())
	mockEng.EXPECT().NotifyPut(gomock.Any(), gomock.Eq(&prov.Peer), gomock.Eq(generateContextID([]string{testCid1.String(), testCid2.String()}, testNonceGen())), gomock.Eq(defaultMetadata))
	mockEng.EXPECT().NotifyRemove(gomock.Any(), gomock.Eq(pID), gomock.Eq(generateContextID([]string{testCid1.String(), testCid2.String()}, testNonceGen())))
	mockEng.EXPECT().NotifyPut(gomock.Any(), gomock.Any(), gomock.Eq(generateContextID([]string{testCid2.String()}, testNonceGen())), gomock.Eq(defaultMetadata))

	listener, err := reframelistener.New(ctx, mockEng, ttl, chunkSize, snapshotSize, "", nil, datastore.NewMapDatastore(), testNonceGen)
	require.NoError(t, err)

	c, s := createClientAndServer(t, listener, prov, priv)
	defer s.Close()

	provide(t, c, ctx, testCid1)
	time.Sleep(2 * time.Second)
	provide(t, c, ctx, testCid2)
	time.Sleep(2 * time.Second)
	provide(t, c, ctx, testCid3)

	// verifying ds and indexes
	require.True(t, reframelistener.CidExist(ctx, listener, testCid2, true))
	require.True(t, reframelistener.CidExist(ctx, listener, testCid3, false))
	require.True(t, reframelistener.CidNotExist(ctx, listener, testCid1))
	require.True(t, reframelistener.ChunkExists(ctx, listener, []cid.Cid{testCid2}, testNonceGen))
	require.True(t, reframelistener.ChunkNotExist(ctx, listener, []cid.Cid{testCid1, testCid2}, testNonceGen))
	require.Equal(t, []cid.Cid{testCid3, testCid2}, reframelistener.GetExpiryQueue(ctx, listener))
}

func TestExpireMultipleChunks(t *testing.T) {
	ttl := time.Second
	chunkSize := 1
	snapshotSize := 1000

	priv, _, pID := testutil.GenerateKeysAndIdentity(t)

	ctx := context.Background()
	defer ctx.Done()
	testCid1 := newCid("test1")
	testCid2 := newCid("test2")
	testCid3 := newCid("test3")
	testCid4 := newCid("test4")
	prov := newProvider(t, pID)

	mc := gomock.NewController(t)
	defer mc.Finish()
	mockEng := mock_provider.NewMockInterface(mc)

	mockEng.EXPECT().RegisterMultihashLister(gomock.Any())
	mockEng.EXPECT().NotifyPut(gomock.Any(), gomock.Eq(&prov.Peer), gomock.Eq(generateContextID([]string{testCid1.String()}, testNonceGen())), gomock.Eq(defaultMetadata))
	mockEng.EXPECT().NotifyPut(gomock.Any(), gomock.Eq(&prov.Peer), gomock.Eq(generateContextID([]string{testCid2.String()}, testNonceGen())), gomock.Eq(defaultMetadata))
	mockEng.EXPECT().NotifyPut(gomock.Any(), gomock.Eq(&prov.Peer), gomock.Eq(generateContextID([]string{testCid3.String()}, testNonceGen())), gomock.Eq(defaultMetadata))
	mockEng.EXPECT().NotifyRemove(gomock.Any(), gomock.Eq(pID), gomock.Eq(generateContextID([]string{testCid1.String()}, testNonceGen())))
	mockEng.EXPECT().NotifyRemove(gomock.Any(), gomock.Eq(pID), gomock.Eq(generateContextID([]string{testCid2.String()}, testNonceGen())))
	mockEng.EXPECT().NotifyRemove(gomock.Any(), gomock.Eq(pID), gomock.Eq(generateContextID([]string{testCid3.String()}, testNonceGen())))

	listener, err := reframelistener.New(ctx, mockEng, ttl, chunkSize, snapshotSize, "", nil, datastore.NewMapDatastore(), testNonceGen)
	require.NoError(t, err)

	c, s := createClientAndServer(t, listener, prov, priv)
	defer s.Close()

	provideMany(t, c, ctx, []cid.Cid{testCid1, testCid2, testCid3})
	time.Sleep(1 * time.Second)
	provide(t, c, ctx, testCid4)
}

func TestDoNotReadvertiseChunkIfAllCidsExpired(t *testing.T) {
	ttl := time.Second
	chunkSize := 1
	snapshotSize := 1000

	priv, _, pID := testutil.GenerateKeysAndIdentity(t)

	ctx := context.Background()
	defer ctx.Done()
	testCid1 := newCid("test1")
	testCid2 := newCid("test2")
	prov := newProvider(t, pID)

	mc := gomock.NewController(t)
	defer mc.Finish()
	mockEng := mock_provider.NewMockInterface(mc)

	mockEng.EXPECT().RegisterMultihashLister(gomock.Any())
	mockEng.EXPECT().NotifyPut(gomock.Any(), gomock.Eq(&prov.Peer), gomock.Eq(generateContextID([]string{testCid1.String()}, testNonceGen())), gomock.Eq(defaultMetadata))
	mockEng.EXPECT().NotifyRemove(gomock.Any(), gomock.Eq(pID), gomock.Eq(generateContextID([]string{testCid1.String()}, testNonceGen())))

	listener, err := reframelistener.New(ctx, mockEng, ttl, chunkSize, snapshotSize, "", nil, datastore.NewMapDatastore(), testNonceGen)
	require.NoError(t, err)

	c, s := createClientAndServer(t, listener, prov, priv)
	defer s.Close()

	provide(t, c, ctx, testCid1)
	time.Sleep(2 * time.Second)
	provide(t, c, ctx, testCid2)

	// verifying ds and indexes
	require.True(t, reframelistener.CidExist(ctx, listener, testCid2, false))
	require.True(t, reframelistener.CidNotExist(ctx, listener, testCid1))
	require.True(t, reframelistener.ChunkNotExist(ctx, listener, []cid.Cid{testCid1}, testNonceGen))
	require.Equal(t, []cid.Cid{testCid2}, reframelistener.GetExpiryQueue(ctx, listener))
}

func TestDoNoLoadRemovedChunksOnInitialisation(t *testing.T) {
	ttl := time.Second
	chunkSize := 1
	snapshotSize := 1000

	priv, _, pID := testutil.GenerateKeysAndIdentity(t)

	ctx := context.Background()
	defer ctx.Done()
	testCid1 := newCid("test1")
	testCid2 := newCid("test2")
	prov := newProvider(t, pID)

	mc := gomock.NewController(t)
	defer mc.Finish()
	mockEng := mock_provider.NewMockInterface(mc)

	mockEng.EXPECT().RegisterMultihashLister(gomock.Any())
	mockEng.EXPECT().NotifyPut(gomock.Any(), gomock.Eq(&prov.Peer), gomock.Eq(generateContextID([]string{testCid1.String()}, testNonceGen())), gomock.Eq(defaultMetadata))
	mockEng.EXPECT().NotifyRemove(gomock.Any(), gomock.Eq(pID), gomock.Eq(generateContextID([]string{testCid1.String()}, testNonceGen())))
	mockEng.EXPECT().RegisterMultihashLister(gomock.Any())

	ds := datastore.NewMapDatastore()
	listener1, err := reframelistener.New(ctx, mockEng, ttl, chunkSize, snapshotSize, "", nil, ds, testNonceGen)
	require.NoError(t, err)

	c, s := createClientAndServer(t, listener1, prov, priv)

	provide(t, c, ctx, testCid1)
	time.Sleep(ttl)
	provide(t, c, ctx, testCid2)

	s.Close()

	listener2, err := reframelistener.New(ctx, mockEng, ttl, chunkSize, snapshotSize, "", nil, ds, testNonceGen)
	require.NoError(t, err)

	require.True(t, reframelistener.ChunkNotExist(ctx, listener2, []cid.Cid{testCid1}, testNonceGen))
}

func TestMissingCidTimestampsBackfilledOnIntialisation(t *testing.T) {
	ttl := time.Hour
	chunkSize := 1
	snapshotSize := 1000

	priv, _, pID := testutil.GenerateKeysAndIdentity(t)

	ctx := context.Background()
	defer ctx.Done()
	testCid1 := newCid("test1")
	testCid2 := newCid("test2")
	testCid3 := newCid("test3")
	prov := newProvider(t, pID)

	mc := gomock.NewController(t)
	defer mc.Finish()
	mockEng := mock_provider.NewMockInterface(mc)

	mockEng.EXPECT().RegisterMultihashLister(gomock.Any())
	mockEng.EXPECT().NotifyPut(gomock.Any(), gomock.Eq(&prov.Peer), gomock.Eq(generateContextID([]string{testCid1.String()}, testNonceGen())), gomock.Eq(defaultMetadata))
	mockEng.EXPECT().NotifyPut(gomock.Any(), gomock.Eq(&prov.Peer), gomock.Eq(generateContextID([]string{testCid2.String()}, testNonceGen())), gomock.Eq(defaultMetadata))
	mockEng.EXPECT().RegisterMultihashLister(gomock.Any())

	ds := datastore.NewMapDatastore()
	listener1, err := reframelistener.New(ctx, mockEng, ttl, chunkSize, snapshotSize, "", nil, ds, testNonceGen)
	require.NoError(t, err)

	c, s := createClientAndServer(t, listener1, prov, priv)

	provide(t, c, ctx, testCid1)
	time.Sleep(100 * time.Millisecond)
	provide(t, c, ctx, testCid2)
	time.Sleep(100 * time.Millisecond)
	provide(t, c, ctx, testCid3)

	t1Before, err := reframelistener.GetCidTimestampFromDatastore(ctx, listener1, testCid1)
	require.NoError(t, err)

	t2Before, err := reframelistener.GetCidTimestampFromDatastore(ctx, listener1, testCid2)
	require.NoError(t, err)

	// cid2 timestamp should be after cid1 timestamp as it has been provided later
	require.True(t, t1Before.Before(t2Before))

	s.Close()

	reframelistener.WrappedDatastore(listener1).Delete(ctx, datastore.NewKey("tc/"+testCid1.String()))

	listener2, err := reframelistener.New(ctx, mockEng, ttl, chunkSize, snapshotSize, "", nil, ds, testNonceGen)
	require.NoError(t, err)

	t1After, err := reframelistener.GetCidTimestampFromCache(ctx, listener2, testCid1)
	require.NoError(t, err)

	t2After, err := reframelistener.GetCidTimestampFromCache(ctx, listener2, testCid2)
	require.NoError(t, err)

	require.NotEqual(t, t1After, t2After)
	require.NotEqual(t, t1Before, t1After)
	require.Equal(t, t2Before, t2After)
	// even though cid2 has been provided after cid1, cid1 shoudl have a higher timestamp as it has been backfilled
	require.True(t, t1After.After(t2After))

}

func TestSameCidNotDuplicatedInTheCurrentChunkIfProvidedTwice(t *testing.T) {
	ttl := time.Hour
	chunkSize := 2
	snapshotSize := 1000

	priv, _, pID := testutil.GenerateKeysAndIdentity(t)

	ctx := context.Background()
	defer ctx.Done()
	testCid1 := newCid("test1")

	mc := gomock.NewController(t)
	defer mc.Finish()
	mockEng := mock_provider.NewMockInterface(mc)

	mockEng.EXPECT().RegisterMultihashLister(gomock.Any())

	listener, err := reframelistener.New(ctx, mockEng, ttl, chunkSize, snapshotSize, "", nil, datastore.NewMapDatastore(), nil)
	require.NoError(t, err)

	c, s := createClientAndServer(t, listener, newProvider(t, pID), priv)
	defer s.Close()

	provide(t, c, ctx, testCid1)
	provide(t, c, ctx, testCid1)
	provide(t, c, ctx, testCid1)
	provide(t, c, ctx, testCid1)
}

func TestShouldStoreSnapshotInDatastore(t *testing.T) {
	snapshotSize := 2
	ttl := time.Hour
	chunkSize := 1000

	priv, _, pID := testutil.GenerateKeysAndIdentity(t)

	ctx := context.Background()
	defer ctx.Done()
	testCid1 := newCid("test1")
	testCid2 := newCid("test2")
	testCid3 := newCid("test3")
	testCid4 := newCid("test4")
	testCid5 := newCid("test5")
	prov := newProvider(t, pID)

	mc := gomock.NewController(t)
	defer mc.Finish()
	mockEng := mock_provider.NewMockInterface(mc)

	mockEng.EXPECT().RegisterMultihashLister(gomock.Any())

	ds := datastore.NewMapDatastore()
	listener, err := reframelistener.New(ctx, mockEng, ttl, chunkSize, snapshotSize, "", nil, ds, testNonceGen)
	require.NoError(t, err)

	client, server := createClientAndServer(t, listener, prov, priv)
	defer server.Close()

	provideMany(t, client, ctx, []cid.Cid{testCid1, testCid2, testCid3, testCid4, testCid5})
	provide(t, client, ctx, testCid1)
	provide(t, client, ctx, testCid2)

	require.True(t, reframelistener.HasSnapshot(ctx, listener))
	require.True(t, reframelistener.HasCidTimestamp(ctx, listener, testCid1))
	require.True(t, reframelistener.HasCidTimestamp(ctx, listener, testCid2))
	require.False(t, reframelistener.HasCidTimestamp(ctx, listener, testCid3))
	require.False(t, reframelistener.HasCidTimestamp(ctx, listener, testCid4))
	require.False(t, reframelistener.HasCidTimestamp(ctx, listener, testCid5))
}

func TestShouldNotStoreSnapshotInDatastore(t *testing.T) {
	snapshotSize := 10
	ttl := time.Hour
	chunkSize := 1000

	priv, _, pID := testutil.GenerateKeysAndIdentity(t)

	ctx := context.Background()
	defer ctx.Done()
	testCid1 := newCid("test1")
	testCid2 := newCid("test2")
	testCid3 := newCid("test3")
	testCid4 := newCid("test4")
	testCid5 := newCid("test5")
	prov := newProvider(t, pID)

	mc := gomock.NewController(t)
	defer mc.Finish()
	mockEng := mock_provider.NewMockInterface(mc)

	mockEng.EXPECT().RegisterMultihashLister(gomock.Any())

	ds := datastore.NewMapDatastore()
	listener, err := reframelistener.New(ctx, mockEng, ttl, chunkSize, snapshotSize, "", nil, ds, testNonceGen)
	require.NoError(t, err)

	client, server := createClientAndServer(t, listener, prov, priv)
	defer server.Close()

	provideMany(t, client, ctx, []cid.Cid{testCid1, testCid2, testCid3, testCid4, testCid5})

	require.False(t, reframelistener.HasSnapshot(ctx, listener))
	require.True(t, reframelistener.HasCidTimestamp(ctx, listener, testCid1))
	require.True(t, reframelistener.HasCidTimestamp(ctx, listener, testCid2))
	require.True(t, reframelistener.HasCidTimestamp(ctx, listener, testCid3))
	require.True(t, reframelistener.HasCidTimestamp(ctx, listener, testCid4))
	require.True(t, reframelistener.HasCidTimestamp(ctx, listener, testCid5))
}

func TestShouldCleanUpTimestampMappingsFromDatastore(t *testing.T) {
	snapshotSize := 2
	ttl := time.Hour
	chunkSize := 1000

	priv, _, pID := testutil.GenerateKeysAndIdentity(t)

	ctx := context.Background()
	defer ctx.Done()
	testCid1 := newCid("test1")
	testCid2 := newCid("test2")
	testCid3 := newCid("test3")
	testCid4 := newCid("test4")
	testCid5 := newCid("test5")
	prov := newProvider(t, pID)

	mc := gomock.NewController(t)
	defer mc.Finish()
	mockEng := mock_provider.NewMockInterface(mc)

	mockEng.EXPECT().RegisterMultihashLister(gomock.Any())
	mockEng.EXPECT().RegisterMultihashLister(gomock.Any())

	ds := datastore.NewMapDatastore()
	listener1, err := reframelistener.New(ctx, mockEng, ttl, chunkSize, snapshotSize, "", nil, ds, testNonceGen)
	require.NoError(t, err)

	client, server := createClientAndServer(t, listener1, prov, priv)

	provideMany(t, client, ctx, []cid.Cid{testCid1, testCid2, testCid3, testCid4, testCid5})
	provide(t, client, ctx, testCid1)
	provide(t, client, ctx, testCid2)

	server.Close()

	listener2, err := reframelistener.New(ctx, mockEng, ttl, chunkSize, snapshotSize, "", nil, ds, testNonceGen)
	require.NoError(t, err)

	require.True(t, reframelistener.HasSnapshot(ctx, listener2))
	require.False(t, reframelistener.HasCidTimestamp(ctx, listener2, testCid1))
	require.False(t, reframelistener.HasCidTimestamp(ctx, listener2, testCid2))
	require.False(t, reframelistener.HasCidTimestamp(ctx, listener2, testCid3))
	require.False(t, reframelistener.HasCidTimestamp(ctx, listener2, testCid4))
	require.False(t, reframelistener.HasCidTimestamp(ctx, listener2, testCid5))
}

func TestShouldCorrectlyMergeSnapshotAndCidTimestamps(t *testing.T) {
	snapshotSize := 2
	ttl := time.Hour
	chunkSize := 1000

	priv, _, pID := testutil.GenerateKeysAndIdentity(t)

	ctx := context.Background()
	defer ctx.Done()
	testCid1 := newCid("test1")
	testCid2 := newCid("test2")
	testCid3 := newCid("test3")
	testCid4 := newCid("test4")
	testCid5 := newCid("test5")
	prov := newProvider(t, pID)

	mc := gomock.NewController(t)
	defer mc.Finish()
	mockEng := mock_provider.NewMockInterface(mc)

	mockEng.EXPECT().RegisterMultihashLister(gomock.Any())
	mockEng.EXPECT().RegisterMultihashLister(gomock.Any())

	ds := datastore.NewMapDatastore()
	listener1, err := reframelistener.New(ctx, mockEng, ttl, chunkSize, snapshotSize, "", nil, ds, testNonceGen)
	require.NoError(t, err)

	client, server := createClientAndServer(t, listener1, prov, priv)

	provideMany(t, client, ctx, []cid.Cid{testCid1, testCid2, testCid3, testCid4, testCid5})
	provide(t, client, ctx, testCid3)
	time.Sleep(100 * time.Millisecond)
	provide(t, client, ctx, testCid1)
	time.Sleep(100 * time.Millisecond)
	provide(t, client, ctx, testCid4)
	time.Sleep(100 * time.Millisecond)
	provide(t, client, ctx, testCid5)
	time.Sleep(100 * time.Millisecond)
	provide(t, client, ctx, testCid2)

	server.Close()

	listener2, err := reframelistener.New(ctx, mockEng, ttl, chunkSize, snapshotSize, "", nil, ds, testNonceGen)
	require.NoError(t, err)

	require.Equal(t, []cid.Cid{testCid2, testCid5, testCid4, testCid1, testCid3}, reframelistener.GetExpiryQueue(ctx, listener2))
}

func TestInitialiseFromDatastoreWithoutSnapshot(t *testing.T) {
	verifyInitialisationFromDatastore(t, 10, time.Hour, 2)
}

func TestInitialiseFromDatastoreWithSnapshot(t *testing.T) {
	verifyInitialisationFromDatastore(t, 2, time.Hour, 2)
}

func verifyInitialisationFromDatastore(t *testing.T, snapshotSize int, ttl time.Duration, chunkSize int) {
	priv, _, pID := testutil.GenerateKeysAndIdentity(t)

	ctx := context.Background()
	defer ctx.Done()
	testCid1 := newCid("test1")
	testCid2 := newCid("test2")
	testCid3 := newCid("test3")
	testCid4 := newCid("test4")
	testCid5 := newCid("test5")
	prov := newProvider(t, pID)

	mc := gomock.NewController(t)
	defer mc.Finish()
	mockEng := mock_provider.NewMockInterface(mc)

	mockEng.EXPECT().RegisterMultihashLister(gomock.Any())
	mockEng.EXPECT().NotifyPut(gomock.Any(), gomock.Eq(&prov.Peer), gomock.Eq(generateContextID([]string{testCid1.String(), testCid2.String()}, testNonceGen())), gomock.Eq(defaultMetadata))
	mockEng.EXPECT().NotifyPut(gomock.Any(), gomock.Eq(&prov.Peer), gomock.Eq(generateContextID([]string{testCid3.String(), testCid4.String()}, testNonceGen())), gomock.Eq(defaultMetadata))
	mockEng.EXPECT().RegisterMultihashLister(gomock.Any())

	ds := datastore.NewMapDatastore()
	listener1, err := reframelistener.New(ctx, mockEng, ttl, chunkSize, snapshotSize, "", nil, ds, testNonceGen)
	require.NoError(t, err)

	client, server := createClientAndServer(t, listener1, prov, priv)

	provide(t, client, ctx, testCid1)
	time.Sleep(100 * time.Millisecond)
	provide(t, client, ctx, testCid2)
	time.Sleep(100 * time.Millisecond)
	provide(t, client, ctx, testCid3)
	time.Sleep(100 * time.Millisecond)
	provide(t, client, ctx, testCid4)
	time.Sleep(100 * time.Millisecond)
	provide(t, client, ctx, testCid5)

	server.Close()

	listener2, err := reframelistener.New(ctx, mockEng, ttl, chunkSize, snapshotSize, "", nil, ds, testNonceGen)
	require.NoError(t, err)

	require.True(t, reframelistener.ChunkExists(ctx, listener2, []cid.Cid{testCid1, testCid2}, testNonceGen))
	require.True(t, reframelistener.ChunkExists(ctx, listener2, []cid.Cid{testCid3, testCid4}, testNonceGen))
	require.True(t, reframelistener.CidExist(ctx, listener2, testCid1, true))
	require.True(t, reframelistener.CidExist(ctx, listener2, testCid2, true))
	require.True(t, reframelistener.CidExist(ctx, listener2, testCid3, true))
	require.True(t, reframelistener.CidExist(ctx, listener2, testCid4, true))
	require.True(t, reframelistener.CidExist(ctx, listener2, testCid5, false))
	require.Equal(t, []cid.Cid{testCid5, testCid4, testCid3, testCid2, testCid1}, reframelistener.GetExpiryQueue(ctx, listener2))
}

func TestCleanUpExpiredCidsThatDontHaveChunk(t *testing.T) {
	ttl := time.Second
	chunkSize := 2
	snapshotSize := 1000
	priv, _, pID := testutil.GenerateKeysAndIdentity(t)

	ctx := context.Background()
	defer ctx.Done()
	testCid1 := newCid("test1")
	testCid2 := newCid("test2")
	testCid3 := newCid("test3")
	testCid100 := newCid("test100")
	prov := newProvider(t, pID)

	mc := gomock.NewController(t)
	defer mc.Finish()
	mockEng := mock_provider.NewMockInterface(mc)

	mockEng.EXPECT().RegisterMultihashLister(gomock.Any())
	mockEng.EXPECT().NotifyPut(gomock.Any(), gomock.Eq(&prov.Peer), gomock.Eq(generateContextID([]string{testCid1.String(), testCid2.String()}, testNonceGen())), gomock.Eq(defaultMetadata))
	mockEng.EXPECT().NotifyRemove(gomock.Any(), gomock.Eq(pID), gomock.Eq(generateContextID([]string{testCid1.String(), testCid2.String()}, testNonceGen())))

	ds := datastore.NewMapDatastore()
	listener, err := reframelistener.New(ctx, mockEng, ttl, chunkSize, snapshotSize, "", nil, ds, testNonceGen)
	require.NoError(t, err)

	c, s := createClientAndServer(t, listener, prov, priv)
	defer s.Close()

	provideMany(t, c, ctx, []cid.Cid{testCid1, testCid2, testCid3})

	time.Sleep(2 * time.Second)

	provide(t, c, ctx, testCid100)

	require.True(t, reframelistener.CidNotExist(ctx, listener, testCid3))
	require.True(t, reframelistener.CidNotExist(ctx, listener, testCid2))
	require.True(t, reframelistener.CidNotExist(ctx, listener, testCid1))
	require.Equal(t, []cid.Cid{testCid100}, reframelistener.GetExpiryQueue(ctx, listener))
}

func TestCidsWithoutChunkAreRegisteredInDsAndIndexes(t *testing.T) {
	ttl := 1 * time.Hour
	chunkSize := 2
	snapshotSize := 1000
	priv, _, pID := testutil.GenerateKeysAndIdentity(t)

	ctx := context.Background()
	defer ctx.Done()
	testCid1 := newCid("test1")

	mc := gomock.NewController(t)
	defer mc.Finish()
	mockEng := mock_provider.NewMockInterface(mc)

	mockEng.EXPECT().RegisterMultihashLister(gomock.Any())

	listener, err := reframelistener.New(ctx, mockEng, ttl, chunkSize, snapshotSize, "", nil, datastore.NewMapDatastore(), nil)
	require.NoError(t, err)

	c, s := createClientAndServer(t, listener, newProvider(t, pID), priv)
	defer s.Close()

	provide(t, c, ctx, testCid1)

	require.True(t, reframelistener.CidExist(ctx, listener, testCid1, false))
	require.Equal(t, []cid.Cid{testCid1}, reframelistener.GetExpiryQueue(ctx, listener))
}

func TestShouldSplitSnapshotIntoMultipleChunksAndReadThemBack(t *testing.T) {
	// this test can cause race detecting issues because of the stats reporter that gets accessed from multiple goroutines
	cidsNumber := 10
	chunkSize := 10000
	snapshotSize := 1
	ttl := 24 * time.Hour

	h, err := libp2p.New()
	require.NoError(t, err)
	priv, _, pID := testutil.GenerateKeysAndIdentity(t)
	ctx := context.Background()

	engine, err := engine.New(engine.WithHost(h), engine.WithPublisherKind(engine.DataTransferPublisher))
	require.NoError(t, err)
	err = engine.Start(ctx)
	defer engine.Shutdown()
	require.NoError(t, err)

	ds := datastore.NewMapDatastore()

	listener, err := reframelistener.New(ctx,
		engine,
		ttl,
		chunkSize,
		snapshotSize,
		"",
		nil,
		ds,
		testNonceGen,
		reframelistener.WithSnapshotMaxChunkSize(1))

	require.NoError(t, err)

	cids := make([]cid.Cid, cidsNumber)
	for i := 0; i < len(cids); i++ {
		cids[i] = newCid(fmt.Sprintf("test%d", i))
	}

	client, server := createClientAndServer(t, listener, newProvider(t, pID), priv)
	defer server.Close()

	provideMany(t, client, ctx, cids)

	require.Equal(t, cidsNumber, reframelistener.SnapshotsQty(ctx, listener))

	// create a new listener and verify that it has initialised correctly
	listener, err = reframelistener.New(ctx,
		engine,
		ttl,
		chunkSize,
		snapshotSize,
		"",
		nil,
		ds,
		testNonceGen)

	require.NoError(t, err)

	queue := reframelistener.GetExpiryQueue(ctx, listener)
	sort.Slice(queue, func(i, j int) bool {
		return queue[i].String() < queue[j].String()
	})
	require.Equal(t, cids, queue)
}

func TestShouldCleanUpOldSnapshotChunksAfterStoringNewOnes(t *testing.T) {
	// this test can cause race detecting issues because of the stats reporter that gets accessed from multiple goroutines
	cidsNumber := 10
	chunkSize := 10000
	snapshotSize := 1
	ttl := time.Second

	h, err := libp2p.New()
	require.NoError(t, err)
	priv, _, pID := testutil.GenerateKeysAndIdentity(t)
	ctx := context.Background()

	engine, err := engine.New(engine.WithHost(h), engine.WithPublisherKind(engine.DataTransferPublisher))
	require.NoError(t, err)
	err = engine.Start(ctx)
	defer engine.Shutdown()
	require.NoError(t, err)

	ds := datastore.NewMapDatastore()

	listener, err := reframelistener.New(ctx,
		engine,
		ttl,
		chunkSize,
		snapshotSize,
		"",
		nil,
		ds,
		testNonceGen,
		reframelistener.WithSnapshotMaxChunkSize(1))

	require.NoError(t, err)

	cids := make([]cid.Cid, cidsNumber)
	for i := 0; i < len(cids); i++ {
		cids[i] = newCid(fmt.Sprintf("test%d", i))
	}

	client, server := createClientAndServer(t, listener, newProvider(t, pID), priv)
	defer server.Close()

	provideMany(t, client, ctx, cids)
	require.Equal(t, cidsNumber, reframelistener.SnapshotsQty(ctx, listener))
	time.Sleep(ttl)
	provideMany(t, client, ctx, cids[0:2])
	require.Equal(t, 2, reframelistener.SnapshotsQty(ctx, listener))
}

func TestShouldRecogniseLegacySnapshot(t *testing.T) {
	// this test can cause race detecting issues because of the stats reporter that gets accessed from multiple goroutines
	chunkSize := 10000
	snapshotSize := 1
	ttl := time.Second

	h, err := libp2p.New()
	require.NoError(t, err)
	priv, _, pID := testutil.GenerateKeysAndIdentity(t)
	ctx := context.Background()

	engine, err := engine.New(engine.WithHost(h), engine.WithPublisherKind(engine.DataTransferPublisher))
	require.NoError(t, err)
	err = engine.Start(ctx)
	defer engine.Shutdown()
	require.NoError(t, err)

	ds := datastore.NewMapDatastore()

	listener, err := reframelistener.New(ctx,
		engine,
		ttl,
		chunkSize,
		snapshotSize,
		"",
		nil,
		ds,
		testNonceGen,
		reframelistener.WithSnapshotMaxChunkSize(1))

	require.NoError(t, err)

	client, server := createClientAndServer(t, listener, newProvider(t, pID), priv)
	defer server.Close()

	provide(t, client, ctx, newCid("test"))

	snapshot, err := reframelistener.WrappedDatastore(listener).Get(ctx, datastore.NewKey("ts/0"))
	require.NoError(t, err)

	err = reframelistener.WrappedDatastore(listener).Put(ctx, datastore.NewKey("ts"), snapshot)
	require.NoError(t, err)

	err = reframelistener.WrappedDatastore(listener).Delete(ctx, datastore.NewKey("ts/0"))
	require.NoError(t, err)

	// create a new listener and verify that it has initialised correctly
	listener, err = reframelistener.New(ctx,
		engine,
		ttl,
		chunkSize,
		snapshotSize,
		"",
		nil,
		ds,
		testNonceGen)

	require.NoError(t, err)

	queue := reframelistener.GetExpiryQueue(ctx, listener)
	require.Equal(t, []cid.Cid{newCid("test")}, queue)
}

func provide(t *testing.T, cc *client.Client, ctx context.Context, c cid.Cid) time.Duration {
	return provideMany(t, cc, ctx, []cid.Cid{c})
}

func provideMany(t *testing.T, cc *client.Client, ctx context.Context, cids []cid.Cid) time.Duration {
	rc, err := cc.Provide(ctx, cids, 2*time.Hour)
	require.NoError(t, err)
	return rc
}

func generateContextID(cids []string, nonce []byte) []byte {
	sort.Strings(cids)
	hasher := sha256.New()
	for _, c := range cids {
		hasher.Write([]byte(c))
	}
	hasher.Write(nonce)
	return hasher.Sum(nil)
}

func newCid(s string) cid.Cid {
	testMH1, _ := multihash.Encode([]byte(s), multihash.IDENTITY)
	return cid.NewCidV1(cid.Raw, testMH1)
}

func createClientAndServer(t *testing.T, service server.DelegatedRoutingService, p *client.Provider, identity crypto.PrivKey) (*client.Client, *httptest.Server) {
	// start a server
	s := httptest.NewServer(server.DelegatedRoutingAsyncHandler(service))

	// start a client
	q, err := proto.New_DelegatedRouting_Client(s.URL, proto.DelegatedRouting_Client_WithHTTPClient(s.Client()))
	require.NoError(t, err)
	c, err := client.NewClient(q, p, identity)
	require.NoError(t, err)

	return c, s
}
