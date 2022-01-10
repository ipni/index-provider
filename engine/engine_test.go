package engine

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math/rand"
	"testing"
	"time"

	"github.com/filecoin-project/go-legs"
	"github.com/filecoin-project/go-legs/dtsync"
	provider "github.com/filecoin-project/index-provider"
	"github.com/filecoin-project/index-provider/config"
	"github.com/filecoin-project/index-provider/metadata"
	"github.com/filecoin-project/index-provider/testutil"
	stiapi "github.com/filecoin-project/storetheindex/api/v0"
	"github.com/filecoin-project/storetheindex/api/v0/ingest/schema"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/ipld/go-ipld-prime"
	_ "github.com/ipld/go-ipld-prime/codec/dagjson"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/test"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pubsubpb "github.com/libp2p/go-libp2p-pubsub/pb"
	mh "github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/blake2b"
)

const (
	testTopic                = "indexer/test"
	protocolID               = 0x300000
	syncFinishedTimeout      = time.Second * 20
	pubSubPropagationTimeout = time.Second * 10
)

var _ provider.MultihashIterator = (*sliceMhIterator)(nil)

type sliceMhIterator struct {
	mhs    []mh.Multihash
	offset int
}

func (s *sliceMhIterator) Next() (mh.Multihash, error) {
	if s.offset < len(s.mhs) {
		next := s.mhs[s.offset]
		s.offset++
		return next, nil
	}
	return nil, io.EOF
}

// toCallback simply returns the list of multihashes for
// testing purposes. A more complex callback could read
// from the CID index and return the list of multihashes.
func toCallback(mhs []mh.Multihash) provider.Callback {
	return func(_ context.Context, _ []byte) (provider.MultihashIterator, error) {
		return &sliceMhIterator{mhs: mhs}, nil
	}
}

func mkLinkSystem(ds datastore.Batching) ipld.LinkSystem {
	lsys := cidlink.DefaultLinkSystem()
	lsys.StorageReadOpener = func(lctx ipld.LinkContext, lnk ipld.Link) (io.Reader, error) {
		c := lnk.(cidlink.Link).Cid
		val, err := ds.Get(lctx.Ctx, datastore.NewKey(c.String()))
		if err != nil {
			return nil, err
		}
		return bytes.NewBuffer(val), nil
	}
	lsys.StorageWriteOpener = func(lctx ipld.LinkContext) (io.Writer, ipld.BlockWriteCommitter, error) {
		buf := bytes.NewBuffer(nil)
		return buf, func(lnk ipld.Link) error {
			c := lnk.(cidlink.Link).Cid
			return ds.Put(lctx.Ctx, datastore.NewKey(c.String()), buf.Bytes())
		}, nil
	}
	return lsys
}

func mkMockSubscriber(t *testing.T, h host.Host, tracer *testPubSubTracer) *legs.Subscriber {
	store := dssync.MutexWrap(datastore.NewMapDatastore())
	lsys := mkLinkSystem(store)
	pst := newTestTopic(t, h, testTopic, tracer)
	ls, err := legs.NewSubscriber(h, store, lsys, testTopic, nil, legs.Topic(pst))
	require.NoError(t, err)
	return ls
}

func mkTestHost(t *testing.T) host.Host {
	h, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/0.0.0.0/tcp/0"))
	require.NoError(t, err)
	return h
}

func TestToCallback(t *testing.T) {
	rng := rand.New(rand.NewSource(1413))
	wantMhs, err := testutil.RandomMultihashes(rng, 10)
	require.NoError(t, err)

	subject := toCallback(wantMhs)
	mhIter, err := subject(context.Background(), []byte("fish"))
	require.NoError(t, err)
	var i int
	for {
		gotCid, err := mhIter.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		require.Equal(t, wantMhs[i], gotCid)
		i++
	}
}

func TestEngine_NotifyRemoveWithUnknownContextIDIsError(t *testing.T) {
	rng := rand.New(rand.NewSource(1413))
	subject := newTestEngine(t, nil)
	mhs, err := testutil.RandomMultihashes(rng, 10)
	require.NoError(t, err)
	subject.RegisterCallback(toCallback(mhs))
	c, err := subject.NotifyRemove(context.Background(), []byte("unknown context ID"))
	require.Equal(t, cid.Undef, c)
	require.Equal(t, provider.ErrContextIDNotFound, err)
}

func newTestEngine(t *testing.T, tracer *testPubSubTracer) *Engine {
	ingestCfg := config.NewIngest()
	ingestCfg.PubSubTopic = testTopic
	return mkEngineWithConfig(t, ingestCfg, tracer)
}

func mkEngineWithConfig(t *testing.T, cfg config.Ingest, tracer *testPubSubTracer) *Engine {
	priv, _, err := test.RandTestKeyPair(crypto.Ed25519, 256)
	require.NoError(t, err)
	h := mkTestHost(t)

	store := dssync.MutexWrap(datastore.NewMapDatastore())

	dt := testutil.SetupDataTransferOnHost(t, h, store, cidlink.DefaultLinkSystem())
	engine, err := New(cfg, priv, dt, h, store, nil)
	require.NoError(t, err)
	if tracer != nil {
		pst := newTestTopic(t, h, cfg.PubSubTopic, tracer)
		engine.dtsyncOptions = []dtsync.Option{dtsync.Topic(pst)}
	}
	err = engine.Start(context.Background())
	require.NoError(t, err)

	return engine
}

func newTestTopic(t *testing.T, h host.Host, topic string, tracer *testPubSubTracer) *pubsub.Topic {
	p, err := pubsub.NewGossipSub(context.Background(), h,
		pubsub.WithPeerExchange(true),
		pubsub.WithMessageIdFn(func(pmsg *pubsubpb.Message) string {
			h, _ := blake2b.New256(nil)
			h.Write(pmsg.Data)
			return string(h.Sum(nil))
		}),
		pubsub.WithFloodPublish(true),
		pubsub.WithDirectConnectTicks(100),
		pubsub.WithEventTracer(tracer),
	)
	require.NoError(t, err)

	pst, err := p.Join(topic)
	require.NoError(t, err)
	return pst
}

func connectHosts(t *testing.T, srcHost, dstHost host.Host) {
	srcHost.Peerstore().AddAddrs(dstHost.ID(), dstHost.Addrs(), time.Hour)
	dstHost.Peerstore().AddAddrs(srcHost.ID(), srcHost.Addrs(), time.Hour)
	err := srcHost.Connect(context.Background(), dstHost.Peerstore().PeerInfo(dstHost.ID()))
	require.NoError(t, err)
}

func genRandomIndexAndAdv(t *testing.T, e *Engine, rng *rand.Rand) (ipld.Link, schema.Advertisement, schema.Link_Advertisement) {
	priv, _, err := test.RandTestKeyPair(crypto.Ed25519, 256)
	require.NoError(t, err)
	mhs, err := testutil.RandomMultihashes(rng, 10)
	require.NoError(t, err)
	p, err := peer.Decode("12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA")
	require.NoError(t, err)
	ctxID := mhs[0]
	metadata := stiapi.Metadata{
		ProtocolID: protocolID,
		Data:       []byte("test-metadata"),
	}

	entries, err := e.entriesChunker.Chunk(context.TODO(), &sliceMhIterator{mhs: mhs})
	require.NoError(t, err)
	addrs := []string{"/ip4/127.0.0.1/tcp/3103"}
	// Generate the advertisement.
	adv, advLnk, err := schema.NewAdvertisementWithLink(e.lsys, priv, nil, entries, ctxID, metadata, false, p.String(), addrs)
	require.NoError(t, err)
	return entries, adv, advLnk
}

func TestPublishLocal(t *testing.T) {
	ctx := context.Background()
	rng := rand.New(rand.NewSource(1413))
	tracer := &testPubSubTracer{}
	e := newTestEngine(t, tracer)

	_, adv, advLnk := genRandomIndexAndAdv(t, e, rng)
	advCid, err := e.PublishLocal(ctx, adv)
	require.NoError(t, err)
	// Check that the Cid has been generated successfully
	require.Equal(t, advCid, advLnk.ToCid(), "advertisement CID from link and published CID not equal")
	// Check that latest advertisement is set correctly
	latest, err := e.getLatestAdCid(ctx)
	require.NoError(t, err)
	require.Equal(t, latest, advCid, "latest advertisement pointer not updated correctly")
	// Publish new advertisement.
	_, adv2, _ := genRandomIndexAndAdv(t, e, rng)
	advCid2, err := e.PublishLocal(ctx, adv2)
	require.NoError(t, err)
	// Latest advertisement should be updates and we are able to still fetch the previous one.
	latest, err = e.getLatestAdCid(ctx)
	require.NoError(t, err)
	require.Equal(t, latest, advCid2, "latest advertisement pointer not updated correctly")
	// Check that we can fetch the latest advertisement
	_, fetchAdv2, err := e.GetLatestAdv(ctx)
	require.NoError(t, err)
	require.Equal(t, ipld.DeepEqual(fetchAdv2, adv2), true, "fetched advertisement is not equal to published one")
	// Check that we can fetch previous ones
	fetchAdv, err := e.GetAdv(ctx, advCid)
	require.NoError(t, err)
	require.Equal(t, ipld.DeepEqual(fetchAdv, adv), true, "fetched advertisement is not equal to published one")
	// Check that latest can be republished.
	err = e.PublishLatest(ctx)
	if err != nil {
		t.Fatal(err)
	}
}

func TestEngine_PublishIsErrorWhenNoCallbackIsRegistered(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	tracer := &testPubSubTracer{}
	subject := newTestEngine(t, tracer)
	ctxID := []byte("some context ID")

	_, err := subject.NotifyPut(ctx, ctxID, metadata.BitswapMetadata)
	require.Error(t, err, provider.ErrNoCallback)
	_, err = subject.NotifyRemove(ctx, ctxID)
	require.Error(t, err, provider.ErrNoCallback)
}

func TestNotifyPutAndRemoveCids(t *testing.T) {
	rng := rand.New(rand.NewSource(1413))
	ctx := context.Background()
	tracer := &testPubSubTracer{}
	e := newTestEngine(t, tracer)
	eHost := e.host.ID()

	// Create mockSubscriber
	lh := mkTestHost(t)
	ls := mkMockSubscriber(t, lh, tracer)
	watcher, cncl := ls.OnSyncFinished()

	t.Cleanup(clean(ls, e, cncl))
	// Connect subscribe with provider engine.
	connectHosts(t, e.host, lh)

	mhs, err := testutil.RandomMultihashes(rng, 10)
	require.NoError(t, err)
	md := stiapi.Metadata{
		ProtocolID: protocolID,
		Data:       []byte("metadata"),
	}
	ctxID := "some context"
	e.RegisterCallback(func(_ context.Context, contextID []byte) (provider.MultihashIterator, error) {

		if string(contextID) == ctxID {
			return &sliceMhIterator{mhs: mhs}, nil
		}
		return nil, errors.New("no content for context ID")
	})

	// Expect a pubsub message to be delivered originating from engine's peer ID.
	delivered := tracer.requireDeliverMessageEventually(e.host.ID(), testTopic, pubSubPropagationTimeout)

	c, err := e.NotifyPut(ctx, []byte(ctxID), md)
	require.NoError(t, err)
	require.True(t, <-delivered, "expected pubsub message delivery")

	// Check that the update has been published and can be fetched from subscriber
	requireNextSyncFinished(t, watcher, legs.SyncFinished{
		Cid:    c,
		PeerID: eHost,
	}, syncFinishedTimeout)

	// NotifyPut second time
	mhs, err = testutil.RandomMultihashes(rng, 10)
	require.NoError(t, err)
	c, err = e.NotifyPut(ctx, []byte(ctxID), metadata.BitswapMetadata)
	require.NoError(t, err)
	// Check that the update has been published and can be fetched from subscriber
	requireNextSyncFinished(t, watcher, legs.SyncFinished{
		Cid:    c,
		PeerID: eHost,
	}, syncFinishedTimeout)
	// TODO: Add a sanity-check to see if the list of cids have been set correctly.

	t.Skip("Skip removal test due to https://github.com/filecoin-project/index-provider/issues/174")

	// Expect a pubsub message to be delivered originating from engine's peer ID.
	delivered = tracer.requireDeliverMessageEventually(e.host.ID(), testTopic, pubSubPropagationTimeout)

	// NotifyRemove the previous ones
	c, err = e.NotifyRemove(ctx, []byte(ctxID))
	require.NoError(t, err)
	require.True(t, <-delivered, "expected pubsub message delivery")

	// Check that the update has been published and can be fetched from subscriber
	requireNextSyncFinished(t, watcher, legs.SyncFinished{
		Cid:    c,
		PeerID: eHost,
	}, syncFinishedTimeout*100)
	// TODO: Add a sanity-check to see if the list of cids have been set correctly.
}

func TestRegisterCallback(t *testing.T) {
	e := newTestEngine(t, nil)
	e.RegisterCallback(toCallback([]mh.Multihash{}))
	require.NotNil(t, e.cb)
}

func TestNotifyPutWithCallback(t *testing.T) {
	rng := rand.New(rand.NewSource(1413))
	ctx := context.Background()
	tracer := &testPubSubTracer{}
	e := newTestEngine(t, tracer)

	// Create mockSubscriber
	lh := mkTestHost(t)
	ls := mkMockSubscriber(t, lh, tracer)
	watcher, cncl := ls.OnSyncFinished()

	t.Cleanup(clean(ls, e, cncl))
	// Connect subscribe with provider engine.
	connectHosts(t, e.host, lh)

	// NotifyPut of cids
	mhs, err := testutil.RandomMultihashes(rng, 20)
	require.NoError(t, err)
	e.RegisterCallback(toCallback(mhs))
	cidsLnk, _, err := schema.NewLinkedListOfMhs(e.lsys, mhs, nil)
	require.NoError(t, err)
	metadata := stiapi.Metadata{
		ProtocolID: protocolID,
		Data:       []byte("metadata"),
	}

	// Expect a pubsub message to be delivered originating from engine's peer ID.
	delivered := tracer.requireDeliverMessageEventually(e.host.ID(), testTopic, pubSubPropagationTimeout)

	c, err := e.NotifyPut(ctx, cidsLnk.(cidlink.Link).Cid.Bytes(), metadata)
	require.NoError(t, err)
	require.True(t, <-delivered, "expected pubsub message delivery")

	// Check that the update has been published and can be fetched from subscriber
	wantSyncFinished := legs.SyncFinished{
		Cid:    c,
		PeerID: e.host.ID(),
	}
	requireNextSyncFinished(t, watcher, wantSyncFinished, syncFinishedTimeout)

	// TODO: Add a test that generates more than one chunk of links (changing the number
	// of CIDs to include so its over 100, the default maxNum of entries)
	// We had to remove this test because it was making the CI unhappy,
	// the sleep was not enough for the list link to propagate. I am deferring
}

// Tests and end-to-end flow of the main linksystem
func TestLinkedStructure(t *testing.T) {
	rng := rand.New(rand.NewSource(1413))
	e := newTestEngine(t, nil)
	mhs, err := testutil.RandomMultihashes(rng, 200)
	require.NoError(t, err)
	// Register simple callback.
	e.RegisterCallback(toCallback(mhs))
	// Sample lookup key
	k := []byte("a")

	// Generate the linked list
	ctx := context.Background()
	mhIter, err := e.cb(ctx, k)
	require.NoError(t, err)
	lnk, err := e.entriesChunker.Chunk(ctx, mhIter)
	require.NoError(t, err)
	err = e.putKeyCidMap(ctx, k, lnk.(cidlink.Link).Cid)
	require.NoError(t, err)
	// Check if the linksystem is able to load it. Demonstrating and e2e
	// flow, from generation and storage to lsys loading.
	n, err := e.lsys.Load(ipld.LinkContext{}, lnk, basicnode.Prototype.Any)
	require.NotNil(t, n)
	require.NoError(t, err)
}

func clean(ls *legs.Subscriber, e *Engine, cncl context.CancelFunc) func() {
	return func() {
		cncl()
		ls.Close()
		if err := e.Shutdown(); err != nil {
			panic(err.Error())
		}
	}
}

func requireNextSyncFinished(t *testing.T, watcher <-chan legs.SyncFinished, want legs.SyncFinished, timeout time.Duration) {
	select {
	case <-time.After(timeout):
		require.Failf(t, "timed out waiting for sync to finish", "expected next sync event to be %v", want)
	case got := <-watcher:
		require.Equal(t, got, want, "expected next finished sync to be %v but got %v", want, got)
	}
}

func Test_EmptyConfigSetsDefaults(t *testing.T) {
	engine, err := New(config.Ingest{}, nil, nil, mkTestHost(t), nil, nil)
	require.NoError(t, err)
	require.True(t, engine.linkedChunkSize > 0)
	require.True(t, engine.linkCacheSize > 0)
	require.True(t, engine.pubSubTopic != "")
	require.True(t, engine.pubSubTopic != "")
}
