package http_mh_iter

import (
	"io"

	"github.com/filecoin-project/index-provider/server/utils"
	"github.com/multiformats/go-multihash"
)

var (
	_ io.ReaderFrom = (*nextPageRequest)(nil)
	_ io.ReaderFrom = (*nextPageResponse)(nil)
	_ io.WriterTo   = (*nextPageRequest)(nil)
	_ io.WriterTo   = (*nextPageResponse)(nil)
)

type nextPageRequest struct {
	AdvReqID  string
	ContextID []byte
	Start     uint64
	Count     uint64
}

type nextPageResponse struct {
	Mhs []multihash.Multihash
}

func (req *nextPageRequest) WriteTo(w io.Writer) (n int64, err error) {
	return utils.MarshalToJson(w, req)
}

func (req *nextPageRequest) ReadFrom(r io.Reader) (n int64, err error) {
	return utils.UnmarshalAsJson(r, req)
}

func (res *nextPageResponse) WriteTo(w io.Writer) (n int64, err error) {
	return utils.MarshalToJson(w, res)
}

func (res *nextPageResponse) ReadFrom(r io.Reader) (n int64, err error) {
	return utils.UnmarshalAsJson(r, res)
}
