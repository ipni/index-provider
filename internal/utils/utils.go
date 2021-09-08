package utils

import (
	"bytes"
	"io"
	"math/rand"
	"testing"
	"time"

	"github.com/filecoin-project/go-indexer-core"
	schema "github.com/filecoin-project/storetheindex/api/v0/ingest/schema"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	crypto "github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/test"
	"github.com/stretchr/testify/require"
)

var prefix = schema.Linkproto.Prefix

func RandomCids(n int) ([]cid.Cid, error) {
	prng := rand.New(rand.NewSource(time.Now().UnixNano()))

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

func GenRandomIndexAndAdv(t *testing.T, lsys ipld.LinkSystem) (schema.Advertisement, schema.Link_Advertisement) {
	priv, _, err := test.RandTestKeyPair(crypto.Ed25519, 256)
	require.NoError(t, err)
	cids, _ := RandomCids(10)
	p, _ := peer.Decode("12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA")
	val := indexer.MakeValue(p, 0, cids[0].Bytes())
	cidsLnk, err := schema.NewListOfCids(lsys, cids)
	require.NoError(t, err)
	adv, advLnk, err := schema.NewAdvertisementWithLink(lsys, priv, nil, cidsLnk, val.Metadata, false, p.String())
	require.NoError(t, err)
	return adv, advLnk
}

func MkLinkSystem(ds datastore.Batching) ipld.LinkSystem {
	lsys := cidlink.DefaultLinkSystem()
	lsys.StorageReadOpener = func(lctx ipld.LinkContext, lnk ipld.Link) (io.Reader, error) {
		c := lnk.(cidlink.Link).Cid
		val, err := ds.Get(datastore.NewKey(c.String()))
		if err != nil {
			return nil, err
		}
		return bytes.NewBuffer(val), nil
	}
	lsys.StorageWriteOpener = func(_ ipld.LinkContext) (io.Writer, ipld.BlockWriteCommitter, error) {
		buf := bytes.NewBuffer(nil)
		return buf, func(lnk ipld.Link) error {
			c := lnk.(cidlink.Link).Cid
			return ds.Put(datastore.NewKey(c.String()), buf.Bytes())
		}, nil
	}
	return lsys
}
