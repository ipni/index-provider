package suppliers

import (
	"bytes"
	"io"

	"github.com/ipfs/go-cid"
)

var _ io.ReadCloser = (*CidIteratorReadCloser)(nil)

type CidIteratorReadCloser struct {
	toBytes    func(cid cid.Cid) ([]byte, error)
	iter       CidIterator
	reachedEnd bool
	buf        bytes.Buffer
}

func NewCidIteratorReadCloser(iter CidIterator, marshaller func(cid cid.Cid) ([]byte, error)) *CidIteratorReadCloser {
	return &CidIteratorReadCloser{
		toBytes: marshaller,
		iter:    iter,
	}
}

func (c *CidIteratorReadCloser) Read(p []byte) (n int, err error) {
	// While there is not enough buffered bytes attempt to populate the buffer.
	for !c.reachedEnd && c.buf.Len() < len(p) {
		next, err := c.iter.Next()
		if err != nil {
			if err != io.EOF {
				return 0, err
			}
			c.reachedEnd = true
			break
		}
		b, err := c.toBytes(next)
		if err != nil {
			return 0, err
		}
		if _, err = c.buf.Write(b); err != nil {
			return 0, err
		}
	}
	return c.buf.Read(p)
}

func (c *CidIteratorReadCloser) Close() error {
	return c.iter.Close()
}
