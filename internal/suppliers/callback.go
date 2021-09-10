package suppliers

import (
	"io"

	"github.com/filecoin-project/indexer-reference-provider/core"
	"github.com/ipfs/go-cid"
)

// ToCidCallback converts the given cidIter to core.CidCallback.
func ToCidCallback(cidIterSup CidIteratorSupplier) core.CidCallback {
	return func(key core.LookupKey) (chan cid.Cid, chan error) {
		/*
			ci, err := cidIterSup.Supply(key)
			if err != nil {
				return nil, err
			}
			return drain(ci)
		*/
		return nil, nil
	}
}

func drain(ci CidIterator) ([]cid.Cid, error) {
	cidList := make([]cid.Cid, 0)
	for {
		c, err := ci.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		cidList = append(cidList, c)
	}
	return cidList, nil
}
