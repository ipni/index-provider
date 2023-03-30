package mirror

import (
	"context"
	"errors"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	hamt "github.com/ipld/go-ipld-adl-hamt"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/node/bindnode"
	"github.com/ipld/go-ipld-prime/schema"
	stischema "github.com/ipni/go-libipni/ingest/schema"
	provider "github.com/ipni/index-provider"
)

var (
	latestMirroredAdCidKey = datastore.NewKey("latest-mirrored-ad-cid")
	latestOriginalAdCidKey = datastore.NewKey("latest-original-ad-cid")
)

func (m *Mirror) getLatestOriginalAdCid(ctx context.Context) (cid.Cid, error) {
	v, err := m.ds.Get(ctx, latestOriginalAdCidKey)
	if err != nil {
		if errors.Is(err, datastore.ErrNotFound) {
			return cid.Undef, nil
		}
		return cid.Undef, err
	}
	_, c, err := cid.CidFromBytes(v)
	if err != nil {
		return cid.Undef, err
	}
	return c, nil
}

func (m *Mirror) setLatestOriginalAdCid(ctx context.Context, c cid.Cid) error {
	return m.ds.Put(ctx, latestOriginalAdCidKey, c.Bytes())
}

func (m *Mirror) getLatestMirroredAdCid(ctx context.Context) (cid.Cid, error) {
	v, err := m.ds.Get(ctx, latestMirroredAdCidKey)
	if err != nil {
		if errors.Is(err, datastore.ErrNotFound) {
			return cid.Undef, nil
		}
		return cid.Undef, err
	}
	_, c, err := cid.CidFromBytes(v)
	if err != nil {
		return cid.Undef, err
	}
	return c, nil
}

func (m *Mirror) setLatestMirroredAdCid(ctx context.Context, c cid.Cid) error {
	return m.ds.Put(ctx, latestMirroredAdCidKey, c.Bytes())
}

func (m *Mirror) loadAd(ctx context.Context, c cid.Cid) (*stischema.Advertisement, error) {
	an, err := m.ls.Load(ipld.LinkContext{Ctx: ctx}, cidlink.Link{Cid: c}, stischema.AdvertisementPrototype)
	if err != nil {
		return nil, err
	}
	return stischema.UnwrapAdvertisement(an)
}

func (m *Mirror) loadEntries(ctx context.Context, l ipld.Link) (provider.MultihashIterator, error) {
	// TODO figure out how to avoid loading the link twice. Right now it is not possible to unwrap
	//      a node that is already loaded with Any prototype.
	lctx := ipld.LinkContext{Ctx: ctx}
	n, err := m.ls.Load(lctx, l, basicnode.Prototype.Any)
	if err != nil {
		return nil, err
	}
	switch {
	case isEntryChunkNode(n):
		return provider.EntryChunkMultihashIterator(l, m.ls)
	case isHamtNode(n):
		n, err := m.ls.Load(lctx, l, hamt.HashMapRootPrototype)
		if err != nil {
			return nil, err
		}
		root := bindnode.Unwrap(n).(*hamt.HashMapRoot)
		return provider.HamtMultihashIterator(root, m.ls), nil
	default:
		return nil, errors.New("unknown entries type")
	}
}

func (m *Mirror) getEntriesPrototype(ctx context.Context, l ipld.Link) (schema.TypedPrototype, error) {
	n, err := m.ls.Load(ipld.LinkContext{Ctx: ctx}, l, basicnode.Prototype.Any)
	if err != nil {
		return nil, err
	}
	switch {
	case isEntryChunkNode(n):
		return stischema.EntryChunkPrototype, nil
	case isHamtNode(n):
		return hamt.HashMapRootPrototype, nil
	default:
		return nil, errors.New("unknown entries type")
	}
}

func isHamtNode(n ipld.Node) bool {
	_, err := n.LookupByString("hamt")
	return err == nil
}

func isEntryChunkNode(n ipld.Node) bool {
	_, err := n.LookupByString("Entries")
	return err == nil
}

func (m *Mirror) setMirroredEntriesLink(ctx context.Context, mirrored, original ipld.Link) error {
	k := mirroredLinkDatastoreKey(original)
	return m.ds.Put(ctx, k, mirrored.(cidlink.Link).Cid.Bytes())
}

func (m *Mirror) getOriginalEntriesLinkFromMirror(ctx context.Context, original ipld.Link) (ipld.Link, error) {
	k := mirroredLinkDatastoreKey(original)
	v, err := m.ds.Get(ctx, k)
	if err != nil {
		return nil, err
	}
	_, c, err := cid.CidFromBytes(v)
	if err != nil {
		return nil, err
	}
	return cidlink.Link{Cid: c}, nil
}

func mirroredLinkDatastoreKey(original ipld.Link) datastore.Key {
	return datastore.KeyWithNamespaces([]string{"mirrored-entries-link", original.String()})
}
