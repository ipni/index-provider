package suppliers

import (
	"errors"
	"io"

	"github.com/ipfs/go-cid"
)

// ErrNotFound signals that CidIteratorSupplier has no iterator corresponding to the given key.
var ErrNotFound = errors.New("no CID iterator found for given key")

type (
	// CidIteratorSupplier supplies iterators by key.
	CidIteratorSupplier interface {
		// Supply supplies a CID iterator for a given key.
		// If no such iterator is found, this function returns ErrNotFound error.
		Supply(key cid.Cid) (CidIterator, error)
	}

	// CidIterator provides an iterator over a list of CIDs.
	// Once the iteration over CIDs is complete, the user should close this iterator by calling CidIterator.Close.
	// See CidIterator.Next
	CidIterator interface {
		io.Closer
		// Next returns the next CID supplied by this iterator.
		// If no more CIDs are left to return, this function returns io.EOF error.
		Next() (cid.Cid, error)
	}
)
