package provider_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/filecoin-project/go-legs"
	provider "github.com/filecoin-project/index-provider"
	"github.com/filecoin-project/index-provider/cardatatransfer"
	"github.com/filecoin-project/index-provider/config"
	"github.com/filecoin-project/index-provider/engine"
	"github.com/filecoin-project/index-provider/metadata"
	"github.com/filecoin-project/index-provider/supplier"
	"github.com/filecoin-project/index-provider/testutil"
	stiapi "github.com/filecoin-project/storetheindex/api/v0"
	"github.com/filecoin-project/storetheindex/api/v0/ingest/schema"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/ipfs/go-graphsync/storeutil"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/ipld/go-car/v2"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/test"
	"github.com/stretchr/testify/require"
)

const testTopic = "test/topic"

func TestRetrievalRoundTrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Initialize everything
	server := newTestServer(t, ctx)
	client := newTestClient(t, ctx)
	disseminateNetworkState(server.h, client.h)

	carBs := testutil.OpenSampleCar(t, "sample-v1-2.car")
	roots, err := carBs.Roots()
	require.NoError(t, err)
	require.Len(t, roots, 1)
	carBs.Close()

	contextID := []byte("applesauce")
	md, err := cardatatransfer.MetadataFromContextID(contextID)
	require.NoError(t, err)
	adv, err := server.cs.Put(ctx, contextID, filepath.Join(testutil.ThisDir(t), "./testdata/sample-v1-2.car"), md)
	require.NoError(t, err)

	// Get first advertisement
	r := client.getAdvViaLegs(t, ctx, adv, server.h.ID())

	mdb, err := r.FieldMetadata().AsBytes()
	require.NoError(t, err)

	var receivedMd stiapi.Metadata
	err = receivedMd.UnmarshalBinary(mdb)
	require.NoError(t, err)
	dtm, err := metadata.FromIndexerMetadata(receivedMd)
	require.NoError(t, err)
	fv1, err := metadata.DecodeFilecoinV1Data(dtm)
	require.NoError(t, err)

	proposal := &cardatatransfer.DealProposal{
		PayloadCID: roots[0],
		ID:         1,
		Params: cardatatransfer.Params{
			PieceCID: &fv1.PieceCID,
		},
	}
	done := make(chan bool, 1)
	unsub := client.dt.SubscribeToEvents(func(event datatransfer.Event, channelState datatransfer.ChannelState) {
		switch channelState.Status() {
		case datatransfer.Failed, datatransfer.Cancelled:
			done <- false
		case datatransfer.Completed:
			done <- true
		}
	})
	defer unsub()
	err = client.dt.RegisterVoucherResultType(&cardatatransfer.DealResponse{})
	require.NoError(t, err)
	err = client.dt.RegisterVoucherType(&cardatatransfer.DealProposal{}, nil)
	require.NoError(t, err)
	_, err = client.dt.OpenPullDataChannel(ctx, server.h.ID(), proposal, roots[0], selectorparse.CommonSelector_ExploreAllRecursively)
	require.NoError(t, err)

	select {
	case <-ctx.Done():
		require.FailNow(t, "context closed")
	case result := <-done:
		require.True(t, result)
	}
}

func TestReimportCar(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	server := newTestServer(t, ctx)
	client := newTestClient(t, ctx)
	disseminateNetworkState(server.h, client.h)

	contextID := []byte("applesauce")
	md, err := cardatatransfer.MetadataFromContextID(contextID)
	require.NoError(t, err)
	adv, err := server.cs.Put(ctx, contextID, filepath.Join(testutil.ThisDir(t), "./testdata/sample-v1-2.car"), md)
	require.NoError(t, err)

	// Get first advertisement
	r := client.getAdvViaLegs(t, ctx, adv, server.h.ID())
	require.NoError(t, err)

	mdb, err := r.FieldMetadata().AsBytes()
	require.NoError(t, err)

	var receivedMd stiapi.Metadata
	err = receivedMd.UnmarshalBinary(mdb)
	require.NoError(t, err)

	// Check the reimporting CAR with same contextID and metadata does not
	// result in advertisement.
	_, err = server.cs.Put(ctx, contextID, filepath.Join(testutil.ThisDir(t), "./testdata/sample-v1-2.car"), md)
	require.Equal(t, err, provider.ErrAlreadyAdvertised)

	// Test that reimporting CAR with same contextID and different metadata generates new advertisement.
	contextID2 := []byte("applesauce2")
	md2, err := cardatatransfer.MetadataFromContextID(contextID2)
	require.NoError(t, err)
	adv2, err := server.cs.Put(ctx, contextID, filepath.Join(testutil.ThisDir(t), "./testdata/sample-v1-2.car"), md2)
	require.NoError(t, err)

	// Get second advertisement
	r2 := client.getAdvViaLegs(t, ctx, adv2, server.h.ID())
	require.NoError(t, err)

	mdb2, err := r2.FieldMetadata().AsBytes()
	require.NoError(t, err)

	var receivedMd2 stiapi.Metadata
	err = receivedMd2.UnmarshalBinary(mdb2)
	require.NoError(t, err)

	require.False(t, receivedMd2.Equal(receivedMd))

	// Check that both advertisements have the same entries link.
	lnk, err := r.FieldEntries().AsLink()
	require.NoError(t, err)
	lnk2, err := r2.FieldEntries().AsLink()
	require.NoError(t, err)
	linkCid := lnk.(cidlink.Link).Cid
	linkCid2 := lnk2.(cidlink.Link).Cid
	require.True(t, linkCid.Equals(linkCid2))
}

func disseminateNetworkState(hosts ...host.Host) {
	for _, one := range hosts {
		for _, other := range hosts {
			if one.ID() != other.ID() {
				one.Peerstore().AddAddrs(other.ID(), other.Addrs(), time.Hour)
			}
		}
	}
}

type testServer struct {
	h  host.Host
	cs *supplier.CarSupplier
	e  *engine.Engine
}

func newTestServer(t *testing.T, ctx context.Context) *testServer {
	h, err := libp2p.New(ctx, libp2p.ListenAddrStrings("/ip4/0.0.0.0/tcp/0"))
	require.NoError(t, err)
	priv, _, err := test.RandTestKeyPair(crypto.Ed25519, 256)
	require.NoError(t, err)
	store := dssync.MutexWrap(datastore.NewMapDatastore())

	dt := testutil.SetupDataTransferOnHost(t, h, store, cidlink.DefaultLinkSystem())
	ingestCfg := config.Ingest{
		PubSubTopic: testTopic,
	}
	e, err := engine.New(ingestCfg, priv, dt, h, store, nil)
	require.NoError(t, err)
	require.NoError(t, e.Start(ctx))

	cs := supplier.NewCarSupplier(e, store, car.ZeroLengthSectionAsEOF(false))
	require.NoError(t, cardatatransfer.StartCarDataTransfer(dt, cs))

	return &testServer{
		h:  h,
		cs: cs,
		e:  e,
	}
}

type testClient struct {
	h    host.Host
	dt   datatransfer.Manager
	lsys ipld.LinkSystem
}

func newTestClient(t *testing.T, ctx context.Context) *testClient {
	store := dssync.MutexWrap(datastore.NewMapDatastore())
	blockStore := blockstore.NewBlockstore(store)
	lsys := storeutil.LinkSystemForBlockstore(blockStore)
	h, err := libp2p.New(ctx, libp2p.ListenAddrStrings("/ip4/0.0.0.0/tcp/0"))
	require.NoError(t, err)

	dt := testutil.SetupDataTransferOnHost(t, h, store, lsys)
	require.NoError(t, dt.RegisterVoucherResultType(&legs.VoucherResult{}))
	require.NoError(t, dt.RegisterVoucherType(&legs.Voucher{}, nil))

	return &testClient{
		h:    h,
		dt:   dt,
		lsys: lsys,
	}
}

func (tc *testClient) getAdvViaLegs(t *testing.T, ctx context.Context, adv cid.Cid, from peer.ID) schema.Advertisement {
	done := make(chan bool, 1)
	unsub := tc.dt.SubscribeToEvents(func(event datatransfer.Event, channelState datatransfer.ChannelState) {
		switch channelState.Status() {
		case datatransfer.Failed, datatransfer.Cancelled:
			t.Logf("%v", event)
			done <- false
		case datatransfer.Completed:
			done <- true
		}
	})
	defer unsub()

	_, err := tc.dt.OpenPullDataChannel(ctx, from, &legs.Voucher{Head: &adv}, adv, selectorparse.CommonSelector_ExploreAllRecursively)
	require.NoError(t, err)
	select {
	case <-ctx.Done():
		require.FailNow(t, "context closed")
	case result := <-done:
		require.True(t, result)
	}

	adb, err := tc.lsys.Load(ipld.LinkContext{}, cidlink.Link{Cid: adv}, schema.Type.Advertisement)
	require.NoError(t, err)
	return adb.(schema.Advertisement)
}
