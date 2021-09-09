package engine

import (
	"bytes"
	"io"

	"github.com/filecoin-project/indexer-reference-provider/core"
	schema "github.com/filecoin-project/storetheindex/api/v0/ingest/schema"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagjson"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
)

const (
	latestAdvKey      = "sync/adv/"
	keyToCidMapPrefix = "map/keyCid/"
	cidToKeyMapPrefix = "map/cidKey/"
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
				// Get the relationship between cid received
				// to dealID so the callback knows how to
				// regenerate the list of CIDs.
				key, err := e.getCidKeyMap(c)
				if err != nil {
					return nil, err
				}
				// NOTE: For removals we may not have the
				// list of CIDs, let's see what selector we end up
				// using, but we may need additional validation here
				// in order not to follow the link.
				cids, err := e.cb(key)
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

func (e *Engine) putKeyCidMap(key core.LookupKey, c cid.Cid) error {
	// We need to store the map Key-Cid to know what CidLink to put
	// in advertisement when we notify a removal.
	err := e.ds.Put(datastore.NewKey(keyToCidMapPrefix+string(key)), c.Bytes())
	if err != nil {
		return err
	}
	// And the other way around when graphsync ios making a request,
	// so the callback in the linksystem knows to what key we are referring.
	return e.ds.Put(datastore.NewKey(cidToKeyMapPrefix+c.String()), key)
}

func (e *Engine) deleteKeyCidMap(key core.LookupKey) error {
	return e.ds.Delete(datastore.NewKey(keyToCidMapPrefix + string(key)))
}

func (e *Engine) deleteCidKeyMap(c cid.Cid) error {
	return e.ds.Delete(datastore.NewKey(cidToKeyMapPrefix + c.String()))
}

func (e *Engine) getCidKeyMap(c cid.Cid) (core.LookupKey, error) {
	return e.ds.Get(datastore.NewKey(cidToKeyMapPrefix + c.String()))
}

func (e *Engine) getKeyCidMap(key core.LookupKey) (cid.Cid, error) {
	b, err := e.ds.Get(datastore.NewKey(keyToCidMapPrefix + string(key)))
	if err != nil {
		if err == datastore.ErrNotFound {
			return cid.Undef, nil
		}
		return cid.Undef, err
	}
	_, d, err := cid.CidFromBytes(b)
	return d, err
}

func (e *Engine) getLatestAdv() (cid.Cid, error) {
	key := latestAdvKey
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
