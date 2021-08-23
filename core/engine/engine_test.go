package engine

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/filecoin-project/go-indexer-core"
	schema "github.com/filecoin-project/storetheindex/api/v0/ingest/schema"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/ipld/go-ipld-prime"
	_ "github.com/ipld/go-ipld-prime/codec/dagjson"
	"github.com/libp2p/go-libp2p"
	crypto "github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/host"
	peer "github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/test"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

const testTopic = "indexer/test"

var prefix = cid.Prefix{
	Version:  1,
	Codec:    cid.Raw,
	MhType:   multihash.SHA2_256,
	MhLength: -1, // default length
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

func RandomCids(n int) ([]cid.Cid, error) {
	var prng = rand.New(rand.NewSource(time.Now().UnixNano()))

	res := make([]cid.Cid, n)
	for i := 0; i < n; i++ {
		b := make([]byte, 10*n)
		prng.Read(b)
		c, err := prefix.Sum(b)
		if err != nil {
			return nil, err
		}
		res[i] = c
	}
	return res, nil
}

func genRandomIndexAndAdv(t *testing.T, e *Engine) (schema.Index, schema.Link_Index, schema.Advertisement, schema.Link_Advertisement) {
	cids, _ := RandomCids(10)
	p, _ := peer.Decode("12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA")
	val := indexer.MakeValue(p, 0, cids[0].Bytes())
	index, indexLnk, err := schema.NewSingleEntryIndex(e.lsys, cids, nil, val.Metadata, nil)
	if err != nil {
		t.Fatal(err)
	}
	adv, advLnk, err := schema.NewAdvertisementWithLink(e.lsys, e.privKey, nil, indexLnk, p.String(), true)
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
	// Check that we can fetch the advertisement
	fetchAdv2, err := e.GetLatestAdv(ctx)
	require.NoError(t, err)
	fAdv2 := schema.Advertisement(fetchAdv2)
	require.Equal(t, ipld.DeepEqual(fAdv2, adv2), true, "fetched advertisement is not equal to published one")

}
