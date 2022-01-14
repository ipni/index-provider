package provider_test

import (
	"context"
	"net"
	"path/filepath"
	"strings"
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
	"github.com/libp2p/go-libp2p-core/test"
	"github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/require"
)

const testTopic = "test/topic"

type testCase struct {
	name             string
	serverConfigOpts []func(*config.Ingest)
}

var testCases = []testCase{
	{
		name:             "DT Publisher",
		serverConfigOpts: nil,
	},
	{
		name: "HTTP Publisher",
		serverConfigOpts: []func(*config.Ingest){
			func(c *config.Ingest) {
				httpPublisherCfg := config.NewHttpPublisher()
				c.PublisherKind = config.HttpPublisherKind
				c.HttpPublisher = httpPublisherCfg
			},
		},
	},
}

func TestRetrievalRoundTrip(t *testing.T) {
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			testRetrievalRoundTripWithTestCase(t, tc)
		})
	}
}

func testRetrievalRoundTripWithTestCase(t *testing.T, tc testCase) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Initialize everything
	server := newTestServer(t, ctx, tc.serverConfigOpts...)
	client := newTestClient(t)
	disseminateNetworkState(server.h, client.h)

	carBs := testutil.OpenSampleCar(t, "sample-v1-2.car")
	roots, err := carBs.Roots()
	require.NoError(t, err)
	require.Len(t, roots, 1)
	carBs.Close()

	contextID := []byte("applesauce")
	md, err := cardatatransfer.MetadataFromContextID(contextID)
	require.NoError(t, err)
	advCid, err := server.cs.Put(ctx, contextID, filepath.Join(testutil.ThisDir(t), "./testdata/sample-v1-2.car"), md)
	require.NoError(t, err)

	sub, err := legs.NewSubscriber(client.h, nil, client.lsys, testTopic, nil, legs.DtManager(client.dt))
	require.NoError(t, err)

	headCid, err := sub.Sync(ctx, server.h.ID(), cid.Undef, nil, server.publisherAddr)
	require.NoError(t, err)
	require.Equal(t, advCid, headCid)

	// Close the subscriber so it doesn't interfere with the next data transfer.
	err = sub.Close()
	require.NoError(t, err)

	// Get first advertisement
	advNode, err := client.lsys.Load(ipld.LinkContext{}, cidlink.Link{Cid: advCid}, schema.Type.Advertisement)
	require.NoError(t, err)
	adv := advNode.(schema.Advertisement)

	mdb, err := adv.FieldMetadata().AsBytes()
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
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			testReimportCarWtihTestCase(t, tc)
		})
	}
}

func testReimportCarWtihTestCase(t *testing.T, tc testCase) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	server := newTestServer(t, ctx, tc.serverConfigOpts...)
	client := newTestClient(t)
	disseminateNetworkState(server.h, client.h)

	contextID := []byte("applesauce")
	md, err := cardatatransfer.MetadataFromContextID(contextID)
	require.NoError(t, err)
	advCid, err := server.cs.Put(ctx, contextID, filepath.Join(testutil.ThisDir(t), "./testdata/sample-v1-2.car"), md)
	require.NoError(t, err)

	sub, err := legs.NewSubscriber(client.h, nil, client.lsys, testTopic, nil, legs.DtManager(client.dt))
	require.NoError(t, err)

	headCid, err := sub.Sync(ctx, server.h.ID(), cid.Undef, nil, server.publisherAddr)
	require.NoError(t, err)
	require.Equal(t, advCid, headCid)

	// Get first advertisement
	advNode, err := client.lsys.Load(ipld.LinkContext{}, cidlink.Link{Cid: advCid}, schema.Type.Advertisement)
	require.NoError(t, err)
	adv := advNode.(schema.Advertisement)

	mdb, err := adv.FieldMetadata().AsBytes()
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
	advCid2, err := server.cs.Put(ctx, contextID, filepath.Join(testutil.ThisDir(t), "./testdata/sample-v1-2.car"), md2)
	require.NoError(t, err)

	// Sync the new advertisement
	headCid, err = sub.Sync(ctx, server.h.ID(), cid.Undef, nil, server.publisherAddr)
	require.NoError(t, err)
	require.Equal(t, advCid2, headCid)

	// Close the subscriber so it doesn't interfere with the next data transfer.
	err = sub.Close()
	require.NoError(t, err)

	// Get second advertisement
	advNode2, err := client.lsys.Load(ipld.LinkContext{}, cidlink.Link{Cid: advCid2}, schema.Type.Advertisement)
	require.NoError(t, err)
	adv2 := advNode2.(schema.Advertisement)

	mdb2, err := adv2.FieldMetadata().AsBytes()
	require.NoError(t, err)

	var receivedMd2 stiapi.Metadata
	err = receivedMd2.UnmarshalBinary(mdb2)
	require.NoError(t, err)

	require.False(t, receivedMd2.Equal(receivedMd))

	// Check that both advertisements have the same entries link.
	lnk, err := adv.FieldEntries().AsLink()
	require.NoError(t, err)
	lnk2, err := adv2.FieldEntries().AsLink()
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
	h             host.Host
	cs            *supplier.CarSupplier
	e             *engine.Engine
	publisherAddr multiaddr.Multiaddr
}

func newTestServer(t *testing.T, ctx context.Context, cfgOpts ...func(*config.Ingest)) *testServer {
	priv, _, err := test.RandTestKeyPair(crypto.Ed25519, 256)
	require.NoError(t, err)
	h, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/0.0.0.0/tcp/0"), libp2p.Identity(priv))
	require.NoError(t, err)
	store := dssync.MutexWrap(datastore.NewMapDatastore())
	t.Cleanup(func() {
		h.Close()
	})

	dt := testutil.SetupDataTransferOnHost(t, h, store, cidlink.DefaultLinkSystem())
	ingestCfg := config.Ingest{
		PubSubTopic: testTopic,
	}
	for _, f := range cfgOpts {
		f(&ingestCfg)
	}

	var publisherAddr multiaddr.Multiaddr
	if ingestCfg.PublisherKind == config.HttpPublisherKind {
		port := findOpenPort(t)
		publisherAddr, err = multiaddr.NewMultiaddr(ingestCfg.HttpPublisher.ListenMultiaddr)
		require.NoError(t, err)

		// Replace the default port with a port we know is open so that tests can
		// run in parallel.
		parts := multiaddr.Split(publisherAddr)
		for i, p := range parts {
			if p.Protocols()[0].Code == multiaddr.P_TCP {
				parts[i], err = multiaddr.NewMultiaddr("/tcp/" + port)
				require.NoError(t, err)
			}
		}
		publisherAddr = multiaddr.Join(parts...)
		ingestCfg.HttpPublisher.ListenMultiaddr = publisherAddr.String()

	} else {
		publisherAddr = h.Addrs()[0]
	}

	e, err := engine.New(ingestCfg, priv, dt, h, store, nil)
	require.NoError(t, err)
	require.NoError(t, e.Start(ctx))

	cs := supplier.NewCarSupplier(e, store, car.ZeroLengthSectionAsEOF(false))
	require.NoError(t, cardatatransfer.StartCarDataTransfer(dt, cs))

	return &testServer{
		h:             h,
		cs:            cs,
		e:             e,
		publisherAddr: publisherAddr,
	}
}

type testClient struct {
	h    host.Host
	dt   datatransfer.Manager
	lsys ipld.LinkSystem
}

func newTestClient(t *testing.T) *testClient {
	store := dssync.MutexWrap(datastore.NewMapDatastore())
	blockStore := blockstore.NewBlockstore(store)
	lsys := storeutil.LinkSystemForBlockstore(blockStore)
	h, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/0.0.0.0/tcp/0"))
	require.NoError(t, err)
	t.Cleanup(func() {
		h.Close()
	})

	dt := testutil.SetupDataTransferOnHost(t, h, store, lsys)

	return &testClient{
		h:    h,
		dt:   dt,
		lsys: lsys,
	}
}

func findOpenPort(t *testing.T) string {
	l, err := net.Listen("tcp", "0.0.0.0:0")
	require.NoError(t, err)
	defer l.Close()
	parts := strings.Split(l.Addr().String(), ":")
	return parts[len(parts)-1]
}
