package internal

import (
	"bytes"
	"context"
	"io"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/multicodec"
	"github.com/ipni/go-libipni/ingest/schema"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multihash"

	// Import so these codecs get registered.
	_ "github.com/ipld/go-ipld-prime/codec/dagcbor"
	_ "github.com/ipld/go-ipld-prime/codec/dagjson"
)

type (
	ProviderClientStore struct {
		datastore.Batching
		ipld.LinkSystem
	}

	// TODO: replace advertisement type with schema.Advertisement now that we have propper structs in
	//       sti.

	Advertisement struct {
		ID               cid.Cid
		PreviousID       cid.Cid
		ProviderID       peer.ID
		ContextID        []byte
		Metadata         []byte
		Addresses        []string
		Signature        []byte
		Entries          *EntriesIterator
		IsRemove         bool
		ExtendedProvider *schema.ExtendedProvider
	}
)

func (a *Advertisement) HasEntries() bool {
	return a.Entries.IsPresent()
}

func newProviderClientStore() *ProviderClientStore {
	store := dssync.MutexWrap(datastore.NewMapDatastore())
	lsys := cidlink.DefaultLinkSystem()
	lsys.StorageReadOpener = func(lctx ipld.LinkContext, lnk ipld.Link) (io.Reader, error) {
		c := lnk.(cidlink.Link).Cid
		val, err := store.Get(lctx.Ctx, datastore.NewKey(c.String()))
		if err != nil {
			return nil, err
		}
		return bytes.NewBuffer(val), nil
	}
	lsys.StorageWriteOpener = func(lctx ipld.LinkContext) (io.Writer, ipld.BlockWriteCommitter, error) {
		buf := bytes.NewBuffer(nil)
		return buf, func(lnk ipld.Link) error {
			c := lnk.(cidlink.Link).Cid
			return store.Put(lctx.Ctx, datastore.NewKey(c.String()), buf.Bytes())
		}, nil
	}
	return &ProviderClientStore{
		Batching:   store,
		LinkSystem: lsys,
	}
}

func (s *ProviderClientStore) getNextChunkLink(ctx context.Context, target cid.Cid) (cid.Cid, error) {
	n, err := s.LinkSystem.Load(linking.LinkContext{Ctx: ctx}, cidlink.Link{Cid: target}, schema.EntryChunkPrototype)
	if err != nil {
		return cid.Undef, err
	}

	chunk, err := schema.UnwrapEntryChunk(n)
	if err != nil {
		return cid.Undef, err
	}
	if chunk.Next == nil {
		return cid.Undef, nil
	}
	return chunk.Next.(cidlink.Link).Cid, nil
}

func (s *ProviderClientStore) getEntriesChunk(ctx context.Context, target cid.Cid) (cid.Cid, []multihash.Multihash, error) {
	n, err := s.LinkSystem.Load(linking.LinkContext{Ctx: ctx}, cidlink.Link{Cid: target}, schema.EntryChunkPrototype)
	if err != nil {
		return cid.Undef, nil, err
	}

	chunk, err := schema.UnwrapEntryChunk(n)
	if err != nil {
		return cid.Undef, nil, err
	}
	var next cid.Cid
	if chunk.Next == nil {
		next = cid.Undef
	} else {
		next = chunk.Next.(cidlink.Link).Cid
	}

	return next, chunk.Entries, nil
}

func (s *ProviderClientStore) getAdvertisement(ctx context.Context, id cid.Cid) (*Advertisement, error) {
	val, err := s.Batching.Get(ctx, datastore.NewKey(id.String()))
	if err != nil {
		return nil, err
	}

	nb := schema.AdvertisementPrototype.NewBuilder()
	decoder, err := multicodec.LookupDecoder(id.Prefix().Codec)
	if err != nil {
		return nil, err
	}

	err = decoder(nb, bytes.NewBuffer(val))
	if err != nil {
		return nil, err
	}
	node := nb.Build()

	ad, err := schema.UnwrapAdvertisement(node)
	if err != nil {
		return nil, err
	}

	dprovid, err := peer.Decode(ad.Provider)
	if err != nil {
		return nil, err
	}

	var prevCid cid.Cid
	if ad.PreviousID != nil {
		prevCid = ad.PreviousID.(cidlink.Link).Cid
	}

	entriesCid := ad.Entries.(cidlink.Link).Cid

	a := &Advertisement{
		ID:         id,
		ProviderID: dprovid,
		ContextID:  ad.ContextID,
		Metadata:   ad.Metadata,
		Addresses:  ad.Addresses,
		PreviousID: prevCid,
		Signature:  ad.Signature,
		Entries: &EntriesIterator{
			root:  entriesCid,
			next:  entriesCid,
			ctx:   ctx,
			store: s,
		},
		IsRemove:         ad.IsRm,
		ExtendedProvider: ad.ExtendedProvider,
	}
	return a, nil
}

// TODO: add advertisement signature verification
