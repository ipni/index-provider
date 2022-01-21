package internal

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/filecoin-project/storetheindex/api/v0/ingest/schema"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/multicodec"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/multiformats/go-multihash"
)

type (
	ProviderClientStore struct {
		datastore.Batching
		ipld.LinkSystem
	}

	Advertisement struct {
		ID         cid.Cid
		PreviousID cid.Cid
		ProviderID peer.ID
		ContextID  []byte
		Metadata   []byte
		Addresses  []string
		Signature  []byte
		Entries    *EntriesIterator
		IsRemove   bool
	}
)

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

func (s *ProviderClientStore) getEntriesChunk(ctx context.Context, target cid.Cid) (cid.Cid, []multihash.Multihash, error) {
	val, err := s.Batching.Get(ctx, datastore.NewKey(target.String()))
	if err != nil {
		return cid.Undef, nil, err
	}
	nb := schema.Type.EntryChunk.NewBuilder()
	decoder, err := multicodec.LookupDecoder(target.Prefix().Codec)
	if err != nil {
		return cid.Undef, nil, err
	}

	err = decoder(nb, bytes.NewBuffer(val))
	if err != nil {
		return cid.Undef, nil, err
	}

	node := nb.Build().(schema.EntryChunk)
	var next cid.Cid
	if node.FieldNext().IsAbsent() || node.FieldNext().IsNull() {
		next = cid.Undef
	} else {
		lnk, err := node.FieldNext().AsNode().AsLink()
		if err != nil {
			return cid.Undef, nil, err
		}
		next = lnk.(cidlink.Link).Cid
	}

	node.FieldEntries()
	cit := node.FieldEntries().ListIterator()
	var mhs []multihash.Multihash
	for !cit.Done() {
		_, cnode, _ := cit.Next()
		h, err := cnode.AsBytes()
		if err != nil {
			return cid.Undef, nil, fmt.Errorf("cannot decode an entry from the ingestion list: %s", err)
		}
		_, m, err := multihash.MHFromBytes(h)
		if err != nil {
			return cid.Undef, nil, err
		}
		mhs = append(mhs, m)
	}
	return next, mhs, nil
}

func (s *ProviderClientStore) getAdvertisement(ctx context.Context, id cid.Cid) (*Advertisement, error) {
	val, err := s.Batching.Get(ctx, datastore.NewKey(id.String()))
	if err != nil {
		return nil, err
	}

	nb := schema.Type.Advertisement.NewBuilder()
	decoder, err := multicodec.LookupDecoder(id.Prefix().Codec)
	if err != nil {
		return nil, err
	}

	err = decoder(nb, bytes.NewBuffer(val))
	if err != nil {
		return nil, err
	}
	node := nb.Build().(schema.Advertisement)

	provid, err := node.FieldProvider().AsString()
	if err != nil {
		return nil, err
	}

	contextID, err := node.FieldContextID().AsBytes()
	if err != nil {
		return nil, err
	}

	meta, err := node.FieldMetadata().AsBytes()
	if err != nil {
		return nil, err
	}

	adds, err := schema.IpldToGoStrings(node.FieldAddresses())
	if err != nil {
		return nil, err
	}
	var prevCid cid.Cid
	if node.FieldPreviousID().Exists() {
		lnk, err := node.FieldPreviousID().Must().AsLink()
		if err != nil {
			return nil, err
		} else {
			prevCid = lnk.(cidlink.Link).Cid
		}
	}

	sign, err := node.FieldSignature().AsBytes()
	if err != nil {
		return nil, err
	}

	elink, err := node.FieldEntries().AsLink()
	if err != nil {
		return nil, err
	}
	entriesCid := elink.(cidlink.Link).Cid

	isRm, err := node.FieldIsRm().AsBool()
	if err != nil {
		return nil, err
	}

	dprovid, err := peer.Decode(provid)
	if err != nil {
		return nil, err
	}

	a := &Advertisement{
		ID:         id,
		ProviderID: dprovid,
		ContextID:  contextID,
		Metadata:   meta,
		Addresses:  adds,
		PreviousID: prevCid,
		Signature:  sign,
		Entries: &EntriesIterator{
			root:  entriesCid,
			next:  entriesCid,
			ctx:   ctx,
			store: s,
		},
		IsRemove: isRm,
	}
	return a, nil
}

// TODO: add advertisement signature verification
