package engine

import (
	"bytes"
	"io"

	schema "github.com/filecoin-project/storetheindex/api/v0/ingest/schema"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
)

const (
	advPrefix      = "adv/"
	indexPrefix    = "index/"
	latestAdvKey   = "sync/adv"
	latestIndexKey = "sync/index"
)

/*
// LinkSystem for the reference provider
// It checks the linkContext to add a prefix before persisting the DAG
// into the datastore.
// NOTE: LinkContext propagation does not currently work with Graphsync,
// so we can use this approach until we figure out what may be happening.
func mkLinkSystem(ds datastore.Batching) ipld.LinkSystem {
	lsys := cidlink.DefaultLinkSystem()
	lsys.StorageReadOpener = func(lctx ipld.LinkContext, lnk ipld.Link) (io.Reader, error) {
		val, err := ds.Get(datastore.NewKey(storageKey(lctx, lnk)))
		if err != nil {
			return nil, err
		}
		return bytes.NewBuffer(val), nil
	}
	lsys.StorageWriteOpener = func(lctx ipld.LinkContext) (io.Writer, ipld.BlockWriteCommitter, error) {
		buf := bytes.NewBuffer(nil)
		return buf, func(lnk ipld.Link) error {
			return ds.Put(datastore.NewKey(storageKey(lctx, lnk)), buf.Bytes())
		}, nil
	}
	return lsys
}

// storageKey uses the corresponding prefix in the datastore
// according to the schema type the link belongs to.
// see api/v0/ingest/utils.go of filecoin-project/storetheindex
// to understand how the storer for the schema works.
func storageKey(lctx ipld.LinkContext, lnk ipld.Link) string {
	c := lnk.(cidlink.Link).Cid
	if lctx.Ctx == nil {
		return c.String()
	}
	val := lctx.Ctx.Value(schema.IsIndexKey)
	if bool(val.(schema.LinkContextValue)) {
		return indexPrefix + c.String()
	}
	return advPrefix + c.String()

}

*/

// Standard linksystem. No prefix are added for persistence.
func mkStdLinkSystem(ds datastore.Batching) ipld.LinkSystem {
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

func (e *Engine) putLatestAdv(advID []byte) error {
	// NOTE: Keep latest sync also in-memory or just in the datastore?
	return e.ds.Put(datastore.NewKey(latestAdvKey), advID)
}

func (e *Engine) putLatestIndex(c cid.Cid) error {
	// NOTE: Keep latest sync also in-memory or just in the datastore?
	return e.ds.Put(datastore.NewKey(latestAdvKey), c.Bytes())
}

func (e *Engine) getLatest(isIndex bool) (cid.Cid, error) {
	key := latestIndexKey
	if !isIndex {
		key = latestAdvKey
	}
	b, err := e.ds.Get(datastore.NewKey(key))
	if err != nil {
		if err == datastore.ErrNotFound {
			return cid.Undef, nil
		}
		return cid.Undef, err
	}
	_, c, err := cid.CidFromBytes(b)
	return c, err
}

func (e *Engine) getLatestIndexLink() (schema.Link_Index, error) {
	c, err := e.getLatest(true)
	if err != nil {
		return nil, err
	}
	return schema.LinkIndexFromCid(c), nil
}

/*
func (e *Engine) getLatestAdvLink() (schema.Link_Advertisement, error) {
	c, err := e.getLatest(false)
	if err != nil {
		return nil, err
	}
	return schema.LinkAdvFromCid(c), nil
}
*/
