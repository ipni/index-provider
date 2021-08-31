package engine

import (
	"context"
	"testing"
	"time"

	"github.com/filecoin-project/go-indexer-core"
	"github.com/filecoin-project/indexer-reference-provider/internal/utils"
	schema "github.com/filecoin-project/storetheindex/api/v0/ingest/schema"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/ipld/go-ipld-prime"
	_ "github.com/ipld/go-ipld-prime/codec/dagjson"
	"github.com/libp2p/go-libp2p"
	crypto "github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/host"
	peer "github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/test"
	"github.com/stretchr/testify/require"
	legs "github.com/willscott/go-legs"
)

const testTopic = "indexer/test"

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

func genRandomIndexAndAdv(t *testing.T, e *Engine) (schema.Index, schema.Link_Index, schema.Advertisement, schema.Link_Advertisement) {
	cids, _ := utils.RandomCids(10)
	p, _ := peer.Decode("12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA")
	val := indexer.MakeValue(p, 0, cids[0].Bytes())
	index, indexLnk, err := schema.NewIndexFromCids(e.lsys, cids, nil, val.Metadata, nil)
	if err != nil {
		t.Fatal(err)
	}
	adv, advLnk, err := schema.NewAdvertisementWithLink(e.lsys, e.privKey, nil, indexLnk, p.String())
	if err != nil {
		t.Fatal(err)
	}
	return index, indexLnk, adv, advLnk
}

func TestPublishLocal(t *testing.T) {
	ctx := context.Background()
	e, err := mkEngine(t)
	require.NoError(t, err)

	_, _, adv, advLnk := genRandomIndexAndAdv(t, e)
	advCid, err := e.PublishLocal(ctx, adv)
	require.NoError(t, err)
	// Check that the Cid has been generated successfully
	require.Equal(t, advCid, advLnk.ToCid(), "advertisement CID from link and published CID not equal")
	// Check that latest advertisement is set correctly
	latest, err := e.getLatest(false)
	require.NoError(t, err)
	require.Equal(t, latest, advCid, "latest advertisement pointer not updated correctly")
	// Publish new advertisement.
	_, _, adv2, _ := genRandomIndexAndAdv(t, e)
	advCid2, err := e.PublishLocal(ctx, adv2)
	require.NoError(t, err)
	// Latest advertisement should be updates and we are able to still fetch the previous one.
	latest, err = e.getLatest(false)
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
	_, _, adv, advLnk := genRandomIndexAndAdv(t, e)
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
	case <-time.After(time.Second * 5):
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
	c, err := e.NotifyPutCids(ctx, cids, []byte("metadata"))
	require.NoError(t, err)

	// Check that the update has been published and can be fetched from subscriber
	select {
	case <-time.After(time.Second * 5):
		t.Fatal("timed out waiting for sync to propogate")
	case downstream := <-watcher:
		if !downstream.Equals(c) {
			t.Fatalf("not the right advertisement published %s vs %s", downstream, c)
		}
	}

	// NotifyPut second time
	cids, _ = utils.RandomCids(10)
	c, err = e.NotifyPutCids(ctx, cids, []byte("metadata"))
	require.NoError(t, err)
	// Check that the update has been published and can be fetched from subscriber
	select {
	case <-time.After(time.Second * 5):
		t.Fatal("timed out waiting for sync to propogate")
	case downstream := <-watcher:
		if !downstream.Equals(c) {
			t.Fatalf("not the right advertisement published %s vs %s", downstream, c)
		}
		// TODO: Add a sanity-check to see if the list of cids have been set correctly.
	}

	// NotifyRemove the previous ones
	c, err = e.NotifyRemoveCids(ctx, cids)
	require.NoError(t, err)
	// Check that the update has been published and can be fetched from subscriber
	select {
	case <-time.After(time.Second * 5):
		t.Fatal("timed out waiting for sync to propogate")
	case downstream := <-watcher:
		if !downstream.Equals(c) {
			t.Fatalf("not the right advertisement published %s vs %s", downstream, c)
		}
		// TODO: Add a sanity-check to see if the list of cids have been set correctly.
	}
}

func clean(ls legs.LegSubscriber, lt *legs.LegTransport, e *Engine, cncl context.CancelFunc) func() {
	return func() {
		cncl()
		ls.Close()
		lt.Close(context.Background())
		e.Close(context.Background())
	}
}
