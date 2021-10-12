package utils

import (
	"bytes"
	"context"
	"io"
	"math/rand"
	"testing"
	"time"

	"github.com/filecoin-project/indexer-reference-provider"
	stiapi "github.com/filecoin-project/storetheindex/api/v0"
	"github.com/filecoin-project/storetheindex/api/v0/ingest/schema"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/test"
	mh "github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

var prefix = schema.Linkproto.Prefix

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

// ToCallback simply returns the list of multihashes for
// testing purposes. A more complex callback could read
// from the CID index and return the list of multihashes.
func ToCallback(mhs []mh.Multihash) provider.Callback {
	return func(_ context.Context, _ provider.LookupKey) (provider.MultihashIterator, error) {
		return &sliceMhIterator{mhs: mhs}, nil
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
	prng := rand.New(rand.NewSource(time.Now().UnixNano()))

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
	ctxID := mhs[0]
	metadata := stiapi.Metadata{
		ProtocolID: 0x300000,
		Data:       []byte("test-metadata"),
	}
	cidsLnk, err := schema.NewListOfMhs(lsys, mhs)
	require.NoError(t, err)
	addrs := []string{"/ip4/0.0.0.0/tcp/3103"}
	adv, advLnk, err := schema.NewAdvertisementWithLink(lsys, priv, nil, cidsLnk, ctxID, metadata, false, p.String(), addrs)
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
