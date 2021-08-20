package engine

import (
	"bytes"
	"io"

	"github.com/filecoin-project/indexer-reference-provider/core"
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

// LinkSystem for the reference provider
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

func storageKey(lctx ipld.LinkContext, lnk ipld.Link) string {
	c := lnk.(cidlink.Link).Cid
	if lctx.Ctx.Value(schema.IsIndexKey).(bool) {
		return indexPrefix + c.String()
	}
	return advPrefix + c.String()

}

func (e *Engine) putLatestAdv(advID []byte) error {
	// NOTE: Keep latest sync also in-memory or just in the datastore?
	return e.ds.Put(datastore.NewKey(latestAdvKey), advID)
}

func (e *Engine) putLatestIndex(c cid.Cid) error {
	// NOTE: Keep latest sync also in-memory or just in the datastore?
	return e.ds.Put(datastore.NewKey(latestAdvKey), c.Bytes())
}

func (e *Engine) getLatestAdv() ([]byte, error) {
	b, err := e.ds.Get(datastore.NewKey(latestAdvKey))
	if err != nil {
		if err == datastore.ErrNotFound {
			return nil, nil
		}
		return nil, err
	}
	return b, nil
}

func (e *Engine) getLatestIndex() (cid.Cid, error) {
	b, err := e.ds.Get(datastore.NewKey(latestAdvKey))
	if err != nil {
		if err == datastore.ErrNotFound {
			return cid.Undef, nil
		}
		return cid.Undef, err
	}
	_, c, err := cid.CidFromBytes(b)
	return c, err
}

func (e *Engine) getLatestIndexLink() (core.IndexLink, error) {
	c, err := e.getLatestIndex()
	if err != nil {
		return nil, err
	}
	return schema.LinkIndexFromCid(c), nil
}
