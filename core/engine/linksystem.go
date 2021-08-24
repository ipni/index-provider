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
	latestAdvKey   = "sync/adv"
	latestIndexKey = "sync/index"
)

// Creates the main engine linksystem.
func mkLinkSystem(ds datastore.Datastore) ipld.LinkSystem {
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
	return e.ds.Put(datastore.NewKey(latestAdvKey), advID)
}

func (e *Engine) putLatestIndex(c cid.Cid) error {
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
