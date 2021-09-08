package engine

import (
	"bytes"
	"io"

	schema "github.com/filecoin-project/storetheindex/api/v0/ingest/schema"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagjson"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
)

const (
	latestAdvKey   = "sync/adv"
	latestIndexKey = "sync/index"
)

// Creates the main engine linksystem.
func (e *Engine) mkLinkSystem(ds datastore.Datastore) ipld.LinkSystem {
	lsys := cidlink.DefaultLinkSystem()
	lsys.StorageReadOpener = func(lctx ipld.LinkContext, lnk ipld.Link) (io.Reader, error) {
		c := lnk.(cidlink.Link).Cid

		// Get the node from datastore.
		val, err := ds.Get(datastore.NewKey(c.String()))
		if err != nil {
			return nil, err
		}

		// Decode the node to check its type.
		n, err := decodeIPLDNode(bytes.NewBuffer(val))
		if err != nil {
			return nil, err
		}
		// If not an advertisement run the callback
		if !isAdvertisement(n) {
			// If the callback has been set.
			if e.cb != nil {
				cids, err := e.cb(c)
				if err != nil {
					return nil, err
				}
				// Wrap cids into a List_String
				lstr := schema.NewListStringFromCids(cids)
				buf := bytes.NewBuffer(nil)
				err = dagjson.Encode(lstr, buf)
				if err != nil {
					return nil, err
				}
				return buf, nil
			}
			// If no callback set fallback to trying to return
			// what was found in the datastore (next line).
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

// decodeIPLDNode from a reaed
// This is used to get the ipld.Node from a set of raw bytes.
func decodeIPLDNode(r io.Reader) (ipld.Node, error) {
	nb := basicnode.Prototype.Any.NewBuilder()
	err := dagjson.Decode(nb, r)
	if err != nil {
		return nil, err
	}
	return nb.Build(), nil
}

// Checks if an IPLD node is an advertisement or
// an index.
// (We may need additional checks if we extend
// the schema with new types that are traversable)
func isAdvertisement(n ipld.Node) bool {
	indexID, _ := n.LookupByString("Signature")
	return indexID != nil
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

/*
func (e *Engine) getLatestAdvLink() (schema.Link_Advertisement, error) {
	c, err := e.getLatest(false)
	if err != nil {
		return nil, err
	}
	return schema.LinkAdvFromCid(c), nil
}
*/
