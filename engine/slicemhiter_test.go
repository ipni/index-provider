package engine_test

import (
	"io"

	provider "github.com/filecoin-project/index-provider"
	mh "github.com/multiformats/go-multihash"
)

var _ provider.MultihashIterator = (*sliceMhIterator)(nil)

type sliceMhIterator struct {
	mhs    []mh.Multihash
	offset int
}

func (s *sliceMhIterator) Next() (mh.Multihash, error) {
	if s.offset < len(s.mhs) {
		next := s.mhs[s.offset]
		s.offset++
		return next, nil
	}
	return nil, io.EOF
}
