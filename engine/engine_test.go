package engine

import (
	"bytes"
	"context"
	"io"
	"os"
	"testing"
	"time"

	"github.com/filecoin-project/go-legs"
	provider "github.com/filecoin-project/index-provider"
	"github.com/filecoin-project/index-provider/config"
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
	mh "github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

const (
	testTopic  = "indexer/test"
	protocolID = 0x300000
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

func mkMockSubscriber(t *testing.T, h host.Host) *legs.Subscriber {
	store := dssync.MutexWrap(datastore.NewMapDatastore())
	lsys := mkLinkSystem(store)
	ls, err := legs.NewSubscriber(h, store, lsys, testTopic, nil)
	require.NoError(t, err)
	return ls
}

func mkTestHost(t *testing.T) host.Host {
	h, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/0.0.0.0/tcp/0"))
	require.NoError(t, err)
	return h
}

func TestToCallback(t *testing.T) {
	wantMhs, err := testutil.RandomMultihashes(10)
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
	subject := mkEngine(t)
	mhs, err := testutil.RandomMultihashes(10)
	require.NoError(t, err)
	subject.RegisterCallback(toCallback(mhs))
	c, err := subject.NotifyRemove(context.Background(), []byte("unknown context ID"))
	require.Equal(t, cid.Undef, c)
	require.Equal(t, provider.ErrContextIDNotFound, err)
}

func mkEngine(t *testing.T) *Engine {
	priv, _, err := test.RandTestKeyPair(crypto.Ed25519, 256)
	require.NoError(t, err)
	h := mkTestHost(t)

	store := dssync.MutexWrap(datastore.NewMapDatastore())

	ingestCfg := config.Ingest{
		PubSubTopic: testTopic,
	}
	dt := testutil.SetupDataTransferOnHost(t, h, store, cidlink.DefaultLinkSystem())
	engine, err := New(ingestCfg, priv, dt, h, store, nil)
	require.NoError(t, err)
	err = engine.Start(context.Background())
	require.NoError(t, err)
	return engine
}

func connectHosts(t *testing.T, srcHost, dstHost host.Host) {
	srcHost.Peerstore().AddAddrs(dstHost.ID(), dstHost.Addrs(), time.Hour)
	dstHost.Peerstore().AddAddrs(srcHost.ID(), srcHost.Addrs(), time.Hour)
	err := srcHost.Connect(context.Background(), dstHost.Peerstore().PeerInfo(dstHost.ID()))
	require.NoError(t, err)
}

// Prepares list of multihashes so it can be used in callback and conveniently registered
// in the engine.
func prepareMhsForCallback(t *testing.T, e *Engine, mhs []mh.Multihash) ipld.Link {
	// Register a callback that returns the randomly generated
	// list of cids.
	e.RegisterCallback(toCallback(mhs))
	// Use a random contextID for the list of cids.
	contextID := []byte(mhs[0])
	mhIter, err := e.cb(context.Background(), contextID)
	require.NoError(t, err)
	cidsLnk, err := e.generateChunks(mhIter)
	require.NoError(t, err)
	// Store the relationship between contextID and CID
	// of the advertised list of Cids so it is available
	// for the engine.
	err = e.putKeyCidMap(context.Background(), contextID, cidsLnk.(cidlink.Link).Cid)
	require.NoError(t, err)
	return cidsLnk
}

func genRandomIndexAndAdv(t *testing.T, e *Engine) (ipld.Link, schema.Advertisement, schema.Link_Advertisement) {
	priv, _, err := test.RandTestKeyPair(crypto.Ed25519, 256)
	require.NoError(t, err)
	mhs, err := testutil.RandomMultihashes(10)
	require.NoError(t, err)
	p, err := peer.Decode("12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA")
	require.NoError(t, err)
	ctxID := mhs[0]
	metadata := stiapi.Metadata{
		ProtocolID: protocolID,
		Data:       []byte("test-metadata"),
	}
	cidsLnk := prepareMhsForCallback(t, e, mhs)
	addrs := []string{"/ip4/127.0.0.1/tcp/3103"}
	// Generate the advertisement.
	adv, advLnk, err := schema.NewAdvertisementWithLink(e.lsys, priv, nil, cidsLnk, ctxID, metadata, false, p.String(), addrs)
	require.NoError(t, err)
	return cidsLnk, adv, advLnk
}

func TestPublishLocal(t *testing.T) {
	ctx := context.Background()
	e := mkEngine(t)

	_, adv, advLnk := genRandomIndexAndAdv(t, e)
	advCid, err := e.PublishLocal(ctx, adv)
	require.NoError(t, err)
	// Check that the Cid has been generated successfully
	require.Equal(t, advCid, advLnk.ToCid(), "advertisement CID from link and published CID not equal")
	// Check that latest advertisement is set correctly
	latest, err := e.getLatestAdv(ctx)
	require.NoError(t, err)
	require.Equal(t, latest, advCid, "latest advertisement pointer not updated correctly")
	// Publish new advertisement.
	_, adv2, _ := genRandomIndexAndAdv(t, e)
	advCid2, err := e.PublishLocal(ctx, adv2)
	require.NoError(t, err)
	// Latest advertisement should be updates and we are able to still fetch the previous one.
	latest, err = e.getLatestAdv(ctx)
	require.NoError(t, err)
	require.Equal(t, latest, advCid2, "latest advertisement pointer not updated correctly")
	// Check that we can fetch the latest advertisement
	_, fetchAdv2, err := e.GetLatestAdv(ctx)
	require.NoError(t, err)
	fAdv2 := schema.Advertisement(fetchAdv2)
	require.Equal(t, ipld.DeepEqual(fAdv2, adv2), true, "fetched advertisement is not equal to published one")
	// Check that we can fetch previous ones
	fetchAdv, err := e.GetAdv(ctx, advCid)
	require.NoError(t, err)
	fAdv := schema.Advertisement(fetchAdv)
	require.Equal(t, ipld.DeepEqual(fAdv, adv), true, "fetched advertisement is not equal to published one")
}

func TestNotifyPublish(t *testing.T) {
	skipFlaky(t)
	ctx := context.Background()
	e := mkEngine(t)

	// Create mockSubscriber
	lh := mkTestHost(t)
	_, adv, advLnk := genRandomIndexAndAdv(t, e)
	ls := mkMockSubscriber(t, lh)
	watcher, cncl := ls.OnSyncFinished()

	t.Cleanup(clean(ls, e, cncl))

	// Connect subscribe with provider engine.
	connectHosts(t, e.host, lh)

	// per https://github.com/libp2p/go-libp2p-pubsub/blob/e6ad80cf4782fca31f46e3a8ba8d1a450d562f49/gossipsub_test.go#L103
	// we don't seem to have a way to manually trigger needed gossip-sub heartbeats for mesh establishment.
	time.Sleep(time.Second)

	// Publish advertisement
	_, err := e.Publish(ctx, adv)
	require.NoError(t, err)

	// Check that the update has been published and can be fetched from subscriber
	c := advLnk.ToCid()
	select {
	case <-time.After(time.Second * 10):
		t.Fatal("timed out waiting for sync to propogate")
	case syncFin := <-watcher:
		if !syncFin.Cid.Equals(c) {
			t.Fatalf("not the right advertisement published %s vs %s", syncFin.Cid, c)
		}
	}

	// Check that we can fetch the latest advertisement locally
	_, fetchAdv, err := e.GetLatestAdv(ctx)
	require.NoError(t, err)
	fAdv := schema.Advertisement(fetchAdv)
	require.Equal(t, ipld.DeepEqual(fAdv, adv), true, "latest fetched advertisement is not equal to published one")
}

func TestNotifyPutAndRemoveCids(t *testing.T) {
	skipFlaky(t)
	ctx := context.Background()
	e := mkEngine(t)

	// Create mockSubscriber
	lh := mkTestHost(t)
	ls := mkMockSubscriber(t, lh)
	watcher, cncl := ls.OnSyncFinished()

	t.Cleanup(clean(ls, e, cncl))
	// Connect subscribe with provider engine.
	connectHosts(t, e.host, lh)

	// per https://github.com/libp2p/go-libp2p-pubsub/blob/e6ad80cf4782fca31f46e3a8ba8d1a450d562f49/gossipsub_test.go#L103
	// we don't seem to have a way to manually trigger needed gossip-sub heartbeats for mesh establishment.
	time.Sleep(time.Second)

	// Fail if not callback has been registered.
	mhs, err := testutil.RandomMultihashes(10)
	require.NoError(t, err)
	metadata := stiapi.Metadata{
		ProtocolID: protocolID,
		Data:       []byte("metadata"),
	}
	_, err = e.NotifyPut(ctx, mhs[0], metadata)
	require.Error(t, err, provider.ErrNoCallback)

	// NotifyPut of cids
	mhs, err = testutil.RandomMultihashes(10)
	require.NoError(t, err)
	cidsLnk := prepareMhsForCallback(t, e, mhs)
	c, err := e.NotifyPut(ctx, cidsLnk.(cidlink.Link).Cid.Bytes(), metadata)
	require.NoError(t, err)

	// Check that the update has been published and can be fetched from subscriber
	select {
	case <-time.After(time.Second * 10):
		t.Fatal("timed out waiting for sync to propogate")
	case syncFin := <-watcher:
		if !syncFin.Cid.Equals(c) {
			t.Fatalf("not the right advertisement published %s vs %s", syncFin.Cid, c)
		}
	}

	// NotifyPut second time
	mhs, err = testutil.RandomMultihashes(10)
	require.NoError(t, err)
	cidsLnk = prepareMhsForCallback(t, e, mhs)
	require.NoError(t, err)
	c, err = e.NotifyPut(ctx, cidsLnk.(cidlink.Link).Cid.Bytes(), metadata)
	require.NoError(t, err)
	// Check that the update has been published and can be fetched from subscriber
	select {
	case <-time.After(time.Second * 10):
		t.Fatal("timed out waiting for sync to propogate")
	case syncFin := <-watcher:
		if !syncFin.Cid.Equals(c) {
			t.Fatalf("not the right advertisement published %s vs %s", syncFin.Cid, c)
		}
		// TODO: Add a sanity-check to see if the list of cids have been set correctly.
	}

	// NotifyRemove the previous ones
	c, err = e.NotifyRemove(ctx, cidsLnk.(cidlink.Link).Cid.Bytes())
	require.NoError(t, err)
	// Check that the update has been published and can be fetched from subscriber
	select {
	case <-time.After(time.Second * 10):
		t.Fatal("timed out waiting for sync to propogate")
	case syncFin := <-watcher:
		if !syncFin.Cid.Equals(c) {
			t.Fatalf("not the right advertisement published %s vs %s", syncFin.Cid, c)
		}
		// TODO: Add a sanity-check to see if the list of cids have been set correctly.
	}
}

func TestRegisterCallback(t *testing.T) {
	e := mkEngine(t)
	e.RegisterCallback(toCallback([]mh.Multihash{}))
	require.NotNil(t, e.cb)
}

func TestNotifyPutWithCallback(t *testing.T) {
	skipFlaky(t)
	ctx := context.Background()
	e := mkEngine(t)

	// Create mockSubscriber
	lh := mkTestHost(t)
	ls := mkMockSubscriber(t, lh)
	watcher, cncl := ls.OnSyncFinished()

	t.Cleanup(clean(ls, e, cncl))
	// Connect subscribe with provider engine.
	connectHosts(t, e.host, lh)

	// per https://github.com/libp2p/go-libp2p-pubsub/blob/e6ad80cf4782fca31f46e3a8ba8d1a450d562f49/gossipsub_test.go#L103
	// we don't seem to have a way to manually trigger needed gossip-sub heartbeats for mesh establishment.
	time.Sleep(time.Second)

	// NotifyPut of cids
	mhs, err := testutil.RandomMultihashes(20)
	require.NoError(t, err)
	e.RegisterCallback(toCallback(mhs))
	cidsLnk, _, err := schema.NewLinkedListOfMhs(e.lsys, mhs, nil)
	require.NoError(t, err)
	metadata := stiapi.Metadata{
		ProtocolID: protocolID,
		Data:       []byte("metadata"),
	}
	c, err := e.NotifyPut(ctx, cidsLnk.(cidlink.Link).Cid.Bytes(), metadata)
	require.NoError(t, err)

	// Check that the update has been published and can be fetched from subscriber
	select {
	case <-time.After(time.Second * 20):
		t.Fatal("timed out waiting for sync to propogate")
	case syncFin := <-watcher:
		if !syncFin.Cid.Equals(c) {
			t.Fatalf("not the right advertisement published %s vs %s", syncFin.Cid, c)
		}
	}

	// TODO: Add a test that generates more than one chunk of links (changing the number
	// of CIDs to include so its over 100, the default maxNum of entries)
	// We had to remove this test because it was making the CI unhappy,
	// the sleep was not enough for the list link to propagate. I am deferring
}

// Tests and end-to-end flow of the main linksystem
func TestLinkedStructure(t *testing.T) {
	skipFlaky(t)
	e := mkEngine(t)
	mhs, err := testutil.RandomMultihashes(200)
	require.NoError(t, err)
	// Register simple callback.
	e.RegisterCallback(toCallback(mhs))
	// Sample lookup key
	k := []byte("a")

	// Generate the linked list
	mhIter, err := e.cb(context.Background(), k)
	require.NoError(t, err)
	lnk, err := e.generateChunks(mhIter)
	require.NoError(t, err)
	err = e.putKeyCidMap(context.Background(), k, lnk.(cidlink.Link).Cid)
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

func skipFlaky(t *testing.T) {
	if os.Getenv("DONT_SKIP") == "" {
		t.Skip("skipping test since it is flaky on the CI. See https://github.com/filecoin-project/index-provider/issues/12")
	}
}
