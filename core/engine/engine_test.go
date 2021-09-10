package engine

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/filecoin-project/go-indexer-core"
	"github.com/filecoin-project/indexer-reference-provider/core"
	"github.com/filecoin-project/indexer-reference-provider/internal/utils"
	"github.com/filecoin-project/storetheindex/api/v0/ingest/schema"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/ipld/go-ipld-prime"
	_ "github.com/ipld/go-ipld-prime/codec/dagjson"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/libp2p/go-libp2p"
	crypto "github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/host"
	peer "github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/test"
	"github.com/stretchr/testify/require"
	legs "github.com/willscott/go-legs"
)

const testTopic = "indexer/test"

func mkLinkSystem(ds datastore.Batching) ipld.LinkSystem {
	lsys := cidlink.DefaultLinkSystem()
	lsys.StorageReadOpener = func(lctx ipld.LinkContext, lnk ipld.Link) (io.Reader, error) {
		c := lnk.(cidlink.Link).Cid
		val, err := ds.Get(datastore.NewKey(c.String()))
		if err != nil {
			return nil, err
		}
		return bytes.NewBuffer(val), nil
	}
	lsys.StorageWriteOpener = func(lctx ipld.LinkContext) (io.Writer, ipld.BlockWriteCommitter, error) {
		buf := bytes.NewBuffer(nil)
		return buf, func(lnk ipld.Link) error {
			c := lnk.(cidlink.Link).Cid
			return ds.Put(datastore.NewKey(c.String()), buf.Bytes())
		}, nil
	}
	return lsys
}

func mkMockSubscriber(t *testing.T, h host.Host) (legs.LegSubscriber, *legs.LegTransport) {
	store := dssync.MutexWrap(datastore.NewMapDatastore())
	lsys := mkLinkSystem(store)
	lt, err := legs.MakeLegTransport(context.Background(), h, store, lsys, testTopic)
	require.NoError(t, err)
	ls, err := legs.NewSubscriber(context.Background(), lt, nil)
	require.NoError(t, err)
	return ls, lt
}

func mkTestHost() host.Host {
	h, _ := libp2p.New(context.Background(), libp2p.ListenAddrStrings("/ip4/0.0.0.0/tcp/0"))
	return h
}

func TestToCallback(t *testing.T) {
	wantCids, err := utils.RandomCids(10)
	require.NoError(t, err)

	subject := toCallback(wantCids)
	cidChan, errChan := subject([]byte("fish"))
	var i int
	for gotCid := range cidChan {
		require.Equal(t, wantCids[i], gotCid)
		i++
	}

	gotErr, isOpen := <-errChan
	require.False(t, isOpen)
	require.Nil(t, gotErr)
}

// toCallback simply returns the list of CIDs for
// testing purposes. A more complex callback could read
// from the CID index and return the list of CIDs.
func toCallback(cids []cid.Cid) core.CidCallback {
	return func(k core.LookupKey) (chan cid.Cid, chan error) {
		chcid := make(chan cid.Cid, 1)
		err := make(chan error, 1)
		go func() {
			defer close(chcid)
			defer close(err)
			for _, c := range cids {
				chcid <- c
			}
		}()
		return chcid, err
	}
}

func mkEngine(t *testing.T) (*Engine, error) {
	priv, _, err := test.RandTestKeyPair(crypto.Ed25519, 256)
	require.NoError(t, err)
	h := mkTestHost()
	store := dssync.MutexWrap(datastore.NewMapDatastore())

	return New(context.Background(), priv, h, store, testTopic)
}

func connectHosts(t *testing.T, srcHost, dstHost host.Host) {
	srcHost.Peerstore().AddAddrs(dstHost.ID(), dstHost.Addrs(), time.Hour)
	dstHost.Peerstore().AddAddrs(srcHost.ID(), srcHost.Addrs(), time.Hour)
	if err := srcHost.Connect(context.Background(), dstHost.Peerstore().PeerInfo(dstHost.ID())); err != nil {
		t.Fatal(err)
	}
}

func genRandomIndexAndAdv(t *testing.T, e *Engine) (ipld.Link, schema.Advertisement, schema.Link_Advertisement) {
	priv, _, err := test.RandTestKeyPair(crypto.Ed25519, 256)
	require.NoError(t, err)
	cids, _ := utils.RandomCids(10)
	p, _ := peer.Decode("12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA")
	val := indexer.MakeValue(p, 0, cids[0].Bytes())
	cidsLnk, err := schema.NewListOfCids(e.lsys, cids)
	require.NoError(t, err)
	adv, advLnk, err := schema.NewAdvertisementWithLink(e.lsys, priv, nil, cidsLnk, val.Metadata, false, p.String())
	require.NoError(t, err)
	return cidsLnk, adv, advLnk
}

func TestPublishLocal(t *testing.T) {
	ctx := context.Background()
	e, err := mkEngine(t)
	require.NoError(t, err)

	_, adv, advLnk := genRandomIndexAndAdv(t, e)
	advCid, err := e.PublishLocal(ctx, adv)
	require.NoError(t, err)
	// Check that the Cid has been generated successfully
	require.Equal(t, advCid, advLnk.ToCid(), "advertisement CID from link and published CID not equal")
	// Check that latest advertisement is set correctly
	latest, err := e.getLatestAdv()
	require.NoError(t, err)
	require.Equal(t, latest, advCid, "latest advertisement pointer not updated correctly")
	// Publish new advertisement.
	_, adv2, _ := genRandomIndexAndAdv(t, e)
	advCid2, err := e.PublishLocal(ctx, adv2)
	require.NoError(t, err)
	// Latest advertisement should be updates and we are able to still fetch the previous one.
	latest, err = e.getLatestAdv()
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
	ctx := context.Background()
	e, err := mkEngine(t)
	require.NoError(t, err)

	// Create mockSubscriber
	lh := mkTestHost()
	_, adv, advLnk := genRandomIndexAndAdv(t, e)
	ls, lt := mkMockSubscriber(t, lh)
	watcher, cncl := ls.OnChange()

	t.Cleanup(clean(ls, lt, e, cncl))

	// Connect subscribe with provider engine.
	connectHosts(t, e.host, lh)

	// per https://github.com/libp2p/go-libp2p-pubsub/blob/e6ad80cf4782fca31f46e3a8ba8d1a450d562f49/gossipsub_test.go#L103
	// we don't seem to have a way to manually trigger needed gossip-sub heartbeats for mesh establishment.
	time.Sleep(time.Second)

	// Publish advertisement
	_, err = e.Publish(ctx, adv)
	require.NoError(t, err)

	// Check that the update has been published and can be fetched from subscriber
	c := advLnk.ToCid()
	select {
	case <-time.After(time.Second * 10):
		t.Fatal("timed out waiting for sync to propogate")
	case downstream := <-watcher:
		if !downstream.Equals(c) {
			t.Fatalf("not the right advertisement published %s vs %s", downstream, c)
		}
	}

	// Check that we can fetch the latest advertisement locally
	_, fetchAdv, err := e.GetLatestAdv(ctx)
	require.NoError(t, err)
	fAdv := schema.Advertisement(fetchAdv)
	require.Equal(t, ipld.DeepEqual(fAdv, adv), true, "latest fetched advertisement is not equal to published one")
}

func TestNotifyPutAndRemoveCids(t *testing.T) {
	// t.Skip("skipping test since it is flaky on the CI. See https://github.com/filecoin-project/indexer-reference-provider/issues/12")
	ctx := context.Background()
	e, err := mkEngine(t)
	require.NoError(t, err)

	// Create mockSubscriber
	lh := mkTestHost()
	ls, lt := mkMockSubscriber(t, lh)
	watcher, cncl := ls.OnChange()

	t.Cleanup(clean(ls, lt, e, cncl))
	// Connect subscribe with provider engine.
	connectHosts(t, e.host, lh)

	// per https://github.com/libp2p/go-libp2p-pubsub/blob/e6ad80cf4782fca31f46e3a8ba8d1a450d562f49/gossipsub_test.go#L103
	// we don't seem to have a way to manually trigger needed gossip-sub heartbeats for mesh establishment.
	time.Sleep(time.Second)

	// NotifyPut of cids
	cids, _ := utils.RandomCids(10)
	cidsLnk, err := schema.NewListOfCids(e.lsys, cids)
	require.NoError(t, err)
	c, err := e.NotifyPut(ctx, cidsLnk.(cidlink.Link).Cid.Bytes(), []byte("metadata"))
	require.NoError(t, err)

	// Check that the update has been published and can be fetched from subscriber
	select {
	case <-time.After(time.Second * 10):
		t.Fatal("timed out waiting for sync to propogate")
	case downstream := <-watcher:
		if !downstream.Equals(c) {
			t.Fatalf("not the right advertisement published %s vs %s", downstream, c)
		}
	}

	// NotifyPut second time
	cids, _ = utils.RandomCids(10)
	cidsLnk, err = schema.NewListOfCids(e.lsys, cids)
	require.NoError(t, err)
	c, err = e.NotifyPut(ctx, cidsLnk.(cidlink.Link).Cid.Bytes(), []byte("metadata"))
	require.NoError(t, err)
	// Check that the update has been published and can be fetched from subscriber
	select {
	case <-time.After(time.Second * 10):
		t.Fatal("timed out waiting for sync to propogate")
	case downstream := <-watcher:
		if !downstream.Equals(c) {
			t.Fatalf("not the right advertisement published %s vs %s", downstream, c)
		}
		// TODO: Add a sanity-check to see if the list of cids have been set correctly.
	}

	// NotifyRemove the previous ones
	c, err = e.NotifyRemove(ctx, cidsLnk.(cidlink.Link).Cid.Bytes(), []byte("metadata"))
	require.NoError(t, err)
	// Check that the update has been published and can be fetched from subscriber
	select {
	case <-time.After(time.Second * 10):
		t.Fatal("timed out waiting for sync to propogate")
	case downstream := <-watcher:
		if !downstream.Equals(c) {
			t.Fatalf("not the right advertisement published %s vs %s", downstream, c)
		}
		// TODO: Add a sanity-check to see if the list of cids have been set correctly.
	}
}

func TestRegisterCallback(t *testing.T) {
	e, err := mkEngine(t)
	require.NoError(t, err)
	e.RegisterCidCallback(toCallback([]cid.Cid{}))
	require.NotNil(t, e.cb)
}

func TestNotifyPutWithCallback(t *testing.T) {
	ctx := context.Background()
	e, err := mkEngine(t)
	require.NoError(t, err)

	// Create mockSubscriber
	lh := mkTestHost()
	ls, lt := mkMockSubscriber(t, lh)
	watcher, cncl := ls.OnChange()

	t.Cleanup(clean(ls, lt, e, cncl))
	// Connect subscribe with provider engine.
	connectHosts(t, e.host, lh)

	// per https://github.com/libp2p/go-libp2p-pubsub/blob/e6ad80cf4782fca31f46e3a8ba8d1a450d562f49/gossipsub_test.go#L103
	// we don't seem to have a way to manually trigger needed gossip-sub heartbeats for mesh establishment.
	time.Sleep(time.Second)

	// NotifyPut of cids
	cids, _ := utils.RandomCids(20)
	e.RegisterCidCallback(toCallback(cids))
	cidsLnk, _, err := schema.NewLinkedListOfCids(e.lsys, cids, nil)
	require.NoError(t, err)
	c, err := e.NotifyPut(ctx, cidsLnk.(cidlink.Link).Cid.Bytes(), []byte("metadata"))
	require.NoError(t, err)

	// Check that the update has been published and can be fetched from subscriber
	select {
	case <-time.After(time.Second * 20):
		t.Fatal("timed out waiting for sync to propogate")
	case downstream := <-watcher:
		if !downstream.Equals(c) {
			t.Fatalf("not the right advertisement published %s vs %s", downstream, c)
		}
	}
}

// Tests and end-to-end flow of the main linksystem
func TestLinkedStructure(t *testing.T) {
	e, err := mkEngine(t)
	require.NoError(t, err)
	cids, _ := utils.RandomCids(200)
	// Register simple callback.
	e.RegisterCidCallback(toCallback(cids))
	// Sample lookup key
	k := []byte("a")

	// Generate the linked list
	chcids, cherr := e.cb(k)
	lnk, err := generateChunks(noStoreLinkSystem(), chcids, cherr, MaxCidsInChunk)
	require.NoError(t, err)
	e.putKeyCidMap(k, lnk.(cidlink.Link).Cid)
	// Check if the linksystem is able to load it. Demonstrating and e2e
	// flow, from generation and storage to lsys loading.
	n, err := e.lsys.Load(ipld.LinkContext{}, lnk, basicnode.Prototype.Any)
	require.NotNil(t, n)
	require.NoError(t, err)

}

func clean(ls legs.LegSubscriber, lt *legs.LegTransport, e *Engine, cncl context.CancelFunc) func() {
	return func() {
		cncl()
		ls.Close()
		lt.Close(context.Background())
		e.Close(context.Background())
	}
}
