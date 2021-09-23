package utils

import (
	"bytes"
	"io"
	"math/rand"
	"testing"
	"time"

	"github.com/filecoin-project/go-indexer-core"
	"github.com/filecoin-project/indexer-reference-provider/core"
	schema "github.com/filecoin-project/storetheindex/api/v0/ingest/schema"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	crypto "github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/test"
	mh "github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

var prefix = schema.Linkproto.Prefix

// ToCallback simply returns the list of multihashes for
// testing purposes. A more complex callback could read
// from the CID index and return the list of multihashes.
func ToCallback(mhs []mh.Multihash) core.CidCallback {
	return func(k core.LookupKey) (<-chan mh.Multihash, <-chan error) {
		chmhs := make(chan mh.Multihash, 1)
		err := make(chan error, 1)
		go func() {
			defer close(chmhs)
			defer close(err)
			for _, c := range mhs {
				chmhs <- c
			}
		}()
		return chmhs, err
	}
}

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

func RandomMultihashes(n int) ([]mh.Multihash, error) {
	var prng = rand.New(rand.NewSource(time.Now().UnixNano()))

	mhashes := make([]mh.Multihash, n)
	for i := 0; i < n; i++ {
		b := make([]byte, 10*n)
		prng.Read(b)
		c, err := prefix.Sum(b)
		if err != nil {
			return nil, err
		}
		mhashes[i] = c.Hash()
	}
	return mhashes, nil
}

func GenRandomIndexAndAdv(t *testing.T, lsys ipld.LinkSystem) (schema.Advertisement, schema.Link_Advertisement) {
	priv, _, err := test.RandTestKeyPair(crypto.Ed25519, 256)
	require.NoError(t, err)
	mhs, _ := RandomMultihashes(10)
	p, _ := peer.Decode("12D3KooWKRyzVWW6ChFjQjK4miCty85Niy48tpPV95XdKu1BcvMA")
	val := indexer.MakeValue(p, 0, mhs[0])
	cidsLnk, err := schema.NewListOfMhs(lsys, mhs)
	require.NoError(t, err)
	addrs := []string{"/ip4/0.0.0.0/tcp/3103"}
	adv, advLnk, err := schema.NewAdvertisementWithLink(lsys, priv, nil, cidsLnk, val.Metadata, false, p.String(), addrs)
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
