package p2pserver_test

import (
	"context"
	"testing"

	"github.com/filecoin-project/indexer-reference-provider/core/engine"
	"github.com/filecoin-project/indexer-reference-provider/internal/libp2pserver"
	"github.com/filecoin-project/indexer-reference-provider/internal/utils"
	p2pserver "github.com/filecoin-project/indexer-reference-provider/server/provider/libp2p"
	p2pclient "github.com/filecoin-project/storetheindex/providerclient/libp2p"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/ipld/go-ipld-prime"
	"github.com/libp2p/go-libp2p"
	crypto "github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/test"
	"github.com/stretchr/testify/require"
)

func mkEngine(t *testing.T, h host.Host, testTopic string) (*engine.Engine, error) {
	priv, _, err := test.RandTestKeyPair(crypto.Ed25519, 256)
	require.NoError(t, err)
	store := dssync.MutexWrap(datastore.NewMapDatastore())

	mhs, _ := utils.RandomMultihashes(10)
	e, err := engine.New(context.Background(), priv, h, store, testTopic, nil)
	e.RegisterCidCallback(utils.ToCallback(mhs))
	return e, err
}

func setupServer(ctx context.Context, t *testing.T) (*libp2pserver.Server, host.Host, *engine.Engine) {
	h, err := libp2p.New(context.Background(), libp2p.ListenAddrStrings("/ip4/0.0.0.0/tcp/0"))
	require.NoError(t, err)
	e, err := mkEngine(t, h, "test/topic")
	require.NoError(t, err)
	s := p2pserver.New(ctx, h, e)
	return s, h, e
}

func setupClient(ctx context.Context, p peer.ID, t *testing.T) *p2pclient.Client {
	c, err := p2pclient.New(nil, p)
	require.NoError(t, err)
	return c
}

func TestAdvertisements(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize everything
	s, sh, e := setupServer(ctx, t)
	c := setupClient(ctx, s.ID(), t)
	err := c.ConnectAddrs(ctx, sh.Addrs()...)
	if err != nil {
		t.Fatal(err)
	}

	// Publish some new advertisements.
	cids, _ := utils.RandomCids(3)
	c1, err := e.NotifyPut(ctx, cids[0].Bytes(), []byte("some metadata"))
	require.NoError(t, err)
	c2, err := e.NotifyPut(ctx, cids[1].Bytes(), []byte("some metadata"))
	require.NoError(t, err)

	// Get first advertisement
	r, err := c.GetAdv(ctx, c1)
	require.NoError(t, err)
	ad, err := e.GetAdv(ctx, c1)
	require.NoError(t, err)
	require.True(t, ipld.DeepEqual(r.Ad, ad))

	// Get latest advertisement
	r, err = c.GetLatestAdv(ctx)
	require.NoError(t, err)
	id, ad, err := e.GetLatestAdv(ctx)
	require.NoError(t, err)
	require.Equal(t, r.ID, id)
	require.Equal(t, r.ID, c2)
	require.True(t, ipld.DeepEqual(r.Ad, ad))

	// Get non-existing advertisement by id
	r, err = c.GetAdv(ctx, cids[2])
	require.Nil(t, r)
	require.Error(t, err)
	require.Equal(t, "datastore: key not found", err.Error())
}
