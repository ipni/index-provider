package provider_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	datatransfer "github.com/filecoin-project/go-data-transfer"
	"github.com/filecoin-project/indexer-reference-provider/config"
	"github.com/filecoin-project/indexer-reference-provider/engine"
	"github.com/filecoin-project/indexer-reference-provider/internal/cardatatransfer"
	"github.com/filecoin-project/indexer-reference-provider/internal/libp2pserver"
	"github.com/filecoin-project/indexer-reference-provider/internal/suppliers"
	"github.com/filecoin-project/indexer-reference-provider/metadata"
	p2pserver "github.com/filecoin-project/indexer-reference-provider/server/provider/libp2p"
	"github.com/filecoin-project/indexer-reference-provider/testutil"
	stiapi "github.com/filecoin-project/storetheindex/api/v0"
	p2pclient "github.com/filecoin-project/storetheindex/providerclient/libp2p"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/ipfs/go-graphsync/storeutil"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	"github.com/ipld/go-car/v2"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	selectorparse "github.com/ipld/go-ipld-prime/traversal/selector/parse"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/test"
	"github.com/stretchr/testify/require"
)

func setupServer(ctx context.Context, t *testing.T) (*libp2pserver.Server, host.Host, *suppliers.CarSupplier, *engine.Engine) {
	h, err := libp2p.New(ctx, libp2p.ListenAddrStrings("/ip4/0.0.0.0/tcp/0"))
	require.NoError(t, err)
	priv, _, err := test.RandTestKeyPair(crypto.Ed25519, 256)
	require.NoError(t, err)
	store := dssync.MutexWrap(datastore.NewMapDatastore())

	dt := testutil.SetupDataTransferOnHost(t, h, store, cidlink.DefaultLinkSystem())
	ingestCfg := config.Ingest{
		PubSubTopic: "test/topic",
	}
	e, err := engine.New(ctx, ingestCfg, priv, dt, h, store, nil)
	require.NoError(t, err)
	cs := suppliers.NewCarSupplier(e, store, car.ZeroLengthSectionAsEOF(false))
	err = cardatatransfer.StartCarDataTransfer(dt, cs)
	require.NoError(t, err)
	s := p2pserver.New(ctx, h, e)

	return s, h, cs, e
}

func setupClient(ctx context.Context, p peer.ID, t *testing.T) (datatransfer.Manager, *p2pclient.Client) {
	store := dssync.MutexWrap(datastore.NewMapDatastore())
	blockStore := blockstore.NewBlockstore(store)
	lsys := storeutil.LinkSystemForBlockstore(blockStore)
	h, err := libp2p.New(ctx, libp2p.ListenAddrStrings("/ip4/0.0.0.0/tcp/0"))
	require.NoError(t, err)

	dt := testutil.SetupDataTransferOnHost(t, h, store, lsys)

	c, err := p2pclient.New(h, p)
	require.NoError(t, err)
	return dt, c
}

func TestRetrievalRoundTrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Initialize everything
	s, sh, cs, _ := setupServer(ctx, t)
	clientDt, c := setupClient(ctx, s.ID(), t)
	err := c.ConnectAddrs(ctx, sh.Addrs()...)
	if err != nil {
		t.Fatal(err)
	}

	carBs := testutil.OpenSampleCar(t, "sample-v1-2.car")
	roots, err := carBs.Roots()
	require.NoError(t, err)
	require.Len(t, roots, 1)
	carBs.Close()

	contextID := []byte("applesauce")
	md, err := cardatatransfer.MetadataFromContextID(contextID)
	require.NoError(t, err)
	adv, err := cs.Put(ctx, contextID, filepath.Join(testutil.ThisDir(t), "./testdata/sample-v1-2.car"), md)
	require.NoError(t, err)

	// Get first advertisement
	r, err := c.GetAdv(ctx, adv)
	require.NoError(t, err)

	var receivedMd stiapi.Metadata
	err = receivedMd.UnmarshalBinary(r.Ad.Metadata.Bytes())
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
	resultChan := make(chan bool, 1)
	clientDt.SubscribeToEvents(func(event datatransfer.Event, channelState datatransfer.ChannelState) {
		switch channelState.Status() {
		case datatransfer.Failed, datatransfer.Cancelled:
			resultChan <- false
		case datatransfer.Completed:
			resultChan <- true
		}
	})
	err = clientDt.RegisterVoucherResultType(&cardatatransfer.DealResponse{})
	require.NoError(t, err)
	err = clientDt.RegisterVoucherType(&cardatatransfer.DealProposal{}, nil)
	require.NoError(t, err)
	_, err = clientDt.OpenPullDataChannel(ctx, sh.ID(), proposal, roots[0], selectorparse.CommonSelector_ExploreAllRecursively)
	require.NoError(t, err)

	select {
	case <-ctx.Done():
		require.FailNow(t, "context closed")
	case result := <-resultChan:
		require.True(t, result)
	}
}
