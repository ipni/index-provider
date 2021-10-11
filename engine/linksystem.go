package engine

import (
	"bytes"
	"io"

	"github.com/filecoin-project/indexer-reference-provider"
	"github.com/filecoin-project/storetheindex/api/v0/ingest/schema"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagjson"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	mh "github.com/multiformats/go-multihash"
)

const (
	latestAdvKey          = "sync/adv/"
	keyToCidMapPrefix     = "map/keyCid/"
	cidToKeyMapPrefix     = "map/cidKey/"
	linksEntryCachePrefix = "cache/links/"
)

// Creates the main engine linksystem.
func (e *Engine) mkLinkSystem() ipld.LinkSystem {
	lsys := cidlink.DefaultLinkSystem()
	lsys.StorageReadOpener = func(lctx ipld.LinkContext, lnk ipld.Link) (io.Reader, error) {
		c := lnk.(cidlink.Link).Cid
		log.Debugf("Triggered ReadOpener from engine's linksystem with cid (%s)", c)

		// Get the node from main datastore. If it is in the
		// main datastore it means it is an advertisement.
		val, err := e.ds.Get(datastore.NewKey(c.String()))
		if err != nil && err != datastore.ErrNotFound {
			log.Errorf("Error getting object from datastore in linksystem: %s", err)
			return nil, err
		}

		// If data was retrieved from the datastore, this may be an advertisement.
		if len(val) != 0 {
			// Decode the node to check its type to see if it is an Advertisement.
			n, err := decodeIPLDNode(bytes.NewBuffer(val))
			if err != nil {
				log.Errorf("Could not decode IPLD node for potential advertisement: %s", err)
				return nil, err
			}
			// If this was an advertisement, then return it.
			if isAdvertisement(n) {
				log.Infow("Retrieved advertisement from datastore", "cid", c, "size", len(val))
				return bytes.NewBuffer(val), nil
			}
			log.Infow("Retrieved non-advertisement object from datastore", "cid", c, "size", len(val))
		}

		// Not an advertisement, so this means we are receiving ingestion data.

		// If no callback registered return error
		if e.cb == nil {
			log.Error("No callback has been registered in engine")
			return nil, ErrNoCallback
		}

		log.Debugw("Checking cache for data", "cid", c)

		// Check if the key is already cached.
		b, err := e.getCacheEntry(c)
		if err != nil {
			log.Errorf("Error fetching cached list for Cid (%s): %s", c, err)
			return nil, err
		}

		// If we don't have the link, generate the linked list in cache so it's
		// ready to be served for this (and future) ingestions.
		//
		// TODO: This process may take a lot of time, we should do it
		// asynchronously to parallelize it. We could implement a cache manager
		// that keeps the state of what has been generated, what has been
		// requested but not available and requires reading from a CAR, and
		// what is ready for ingestion. This manager will also have to handle
		// garbage collecting the cache.
		//
		// The reason for caching this?  When we build the ingestion linked
		// lists and we are serving back the structure to an indexer, we will
		// be receiving requests for a chunkEntry, as we can't read a specific
		// subset of CIDs from the CAR index, we need some intermediate storage
		// to map link of the chunk in the linked list with the list of CIDs it
		// corresponds to.
		if b == nil {
			log.Infow("Entry for CID is not cached, generating chunks", "cid", c)
			// If the link is not found, it means that the root link of the list has
			// not been generated and we need to get the relationship between the cid
			// received and the lookupKey so the callback knows how to
			// regenerate the list of CIDs.
			key, err := e.getCidKeyMap(c)
			if err != nil {
				log.Errorf("Error fetching relationship between CID and lookup key: %s", err)
				return nil, err
			}

			// TODO: For removals we may not have the list of CIDs, let's see
			// what selector we end up using, but we may need additional
			// validation here in order not to follow the link. If we do
			// step-by-step syncs, this would mean that when the subscribers
			// sees an advertisement of remove type, it doesn't follow the
			// Entries link, if just gets the cid, and uses its local map cid
			// to lookupKey to trigger the removal of all entries for that
			// lookupKey in its index.
			mhIter, err := e.cb(lctx.Ctx, key)
			if err != nil {
				return nil, err
			}

			// Store the linked list entries in cache as we generate them.  We
			// use the cache linksystem that stores entries in an in-memory
			// datastore.
			_, err = generateChunks(e.cachelsys, mhIter, maxIngestChunk)
			if err != nil {
				log.Errorf("Error generating linked list from callback: %s", err)
				return nil, err
			}
		} else {
			log.Infow("Found cache entry for CID", "cid", c)
		}

		// Return the linked list node.
		val, err = e.getCacheEntry(c)
		if err != nil {
			log.Errorf("Error fetching cached list for CID (%s): %s", c, err)
			return nil, err
		}

		// If no value was populated it means that nothing was found
		// in the multiple datastores.
		if len(val) == 0 {
			log.Errorf("No object has been found in linksystem for CID (%s)", c)
			return nil, datastore.ErrNotFound
		}

		return bytes.NewBuffer(val), nil
	}
	lsys.StorageWriteOpener = func(_ ipld.LinkContext) (io.Writer, ipld.BlockWriteCommitter, error) {
		buf := bytes.NewBuffer(nil)
		return buf, func(lnk ipld.Link) error {
			c := lnk.(cidlink.Link).Cid
			return e.ds.Put(datastore.NewKey(c.String()), buf.Bytes())
		}, nil
	}
	return lsys
}

// Generate chunks of the linked list.
//
// This function takes a linksystem for persistence along with the channels
// from a callback, and generates the linked list structure. It also supports
// configuring the number of entries per chunk in the list.
func generateChunks(lsys ipld.LinkSystem, mhIter provider.MultihashIterator, maxChunkSize int) (ipld.Link, error) {
	mhs := make([]mh.Multihash, 0, maxChunkSize)
	var chunkLnk ipld.Link
	var totalMhCount, chunkCount int
	for {
		next, err := mhIter.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		mhs = append(mhs, next)
		totalMhCount++

		if len(mhs) >= maxChunkSize {
			chunkLnk, _, err = schema.NewLinkedListOfMhs(lsys, mhs, chunkLnk)
			if err != nil {
				return nil, err
			}
			chunkCount++
			mhs = make([]mh.Multihash, 0, maxChunkSize)
		}
	}

	// Chunk remaining multihashes.
	if len(mhs) != 0 {
		var err error
		chunkLnk, _, err = schema.NewLinkedListOfMhs(lsys, mhs, chunkLnk)
		if err != nil {
			return nil, err
		}
		chunkCount++
	}

	log.Infow("Generated linked chunks of multihashes", "totalMhCount", totalMhCount, "chunkCount", chunkCount)
	return chunkLnk, nil
}

// cacheLinkSystem persist IPLD objects in an in-memory datastore.
func (e *Engine) cacheLinkSystem() ipld.LinkSystem {
	lsys := cidlink.DefaultLinkSystem()
	lsys.StorageReadOpener = func(lctx ipld.LinkContext, lnk ipld.Link) (io.Reader, error) {
		c := lnk.(cidlink.Link).Cid
		val, err := e.cache.Get(datastore.NewKey(linksEntryCachePrefix + c.String()))
		if err != nil {
			return nil, err
		}
		return bytes.NewBuffer(val), nil
	}
	lsys.StorageWriteOpener = func(lctx ipld.LinkContext) (io.Writer, ipld.BlockWriteCommitter, error) {
		buf := bytes.NewBuffer(nil)
		return buf, func(lnk ipld.Link) error {
			c := lnk.(cidlink.Link).Cid
			return e.cache.Put(datastore.NewKey(linksEntryCachePrefix+c.String()), buf.Bytes())
		}, nil
	}
	return lsys
}

// vanillaLinkSystem plainly loads and stores from engine datastore.
//
// This is used to plainly load and store links without the complex
// logic of the main linksystem. This is mainly used to retrieve
// stored advertisements through the link from the main blockstore.
func (e *Engine) vanillaLinkSystem() ipld.LinkSystem {
	lsys := cidlink.DefaultLinkSystem()
	lsys.StorageReadOpener = func(lctx ipld.LinkContext, lnk ipld.Link) (io.Reader, error) {
		c := lnk.(cidlink.Link).Cid
		val, err := e.ds.Get(datastore.NewKey(c.String()))
		if err != nil {
			return nil, err
		}
		return bytes.NewBuffer(val), nil
	}
	lsys.StorageWriteOpener = func(lctx ipld.LinkContext) (io.Writer, ipld.BlockWriteCommitter, error) {
		buf := bytes.NewBuffer(nil)
		return buf, func(lnk ipld.Link) error {
			c := lnk.(cidlink.Link).Cid
			return e.cache.Put(datastore.NewKey(c.String()), buf.Bytes())
		}, nil
	}
	return lsys
}

// Linksystem used to generate links from a list of cids without
// persisting anything in the process.
func noStoreLinkSystem() ipld.LinkSystem {
	lsys := cidlink.DefaultLinkSystem()
	lsys.StorageWriteOpener = func(lctx ipld.LinkContext) (io.Writer, ipld.BlockWriteCommitter, error) {
		buf := bytes.NewBuffer(nil)
		return buf, func(lnk ipld.Link) error {
			return nil
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

func (e *Engine) putKeyCidMap(key provider.LookupKey, c cid.Cid) error {
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

func (e *Engine) deleteKeyCidMap(key provider.LookupKey) error {
	return e.ds.Delete(datastore.NewKey(keyToCidMapPrefix + string(key)))
}

func (e *Engine) deleteCidKeyMap(c cid.Cid) error {
	return e.ds.Delete(datastore.NewKey(cidToKeyMapPrefix + c.String()))
}

func (e *Engine) getCidKeyMap(c cid.Cid) (provider.LookupKey, error) {
	return e.ds.Get(datastore.NewKey(cidToKeyMapPrefix + c.String()))
}

func (e *Engine) getKeyCidMap(key provider.LookupKey) (cid.Cid, error) {
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

// get an entry from cache.
func (e *Engine) getCacheEntry(c cid.Cid) ([]byte, error) {
	b, err := e.cache.Get(datastore.NewKey(linksEntryCachePrefix + c.String()))
	if err != nil {
		if err == datastore.ErrNotFound {
			return nil, nil
		}
		return nil, err
	}
	return b, err
}

func (e *Engine) getLatestAdv() (cid.Cid, error) {
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
