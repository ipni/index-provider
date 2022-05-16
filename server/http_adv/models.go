package http_adv

import (
	"io"

	"github.com/filecoin-project/index-provider/metadata"
	"github.com/filecoin-project/index-provider/server/utils"
	"github.com/ipfs/go-cid"
)

var (
	_ io.ReaderFrom = (*AnnounceAdvReq)(nil)
	_ io.ReaderFrom = (*AnnounceAdvRes)(nil)
	_ io.WriterTo   = (*AnnounceAdvReq)(nil)
	_ io.WriterTo   = (*AnnounceAdvRes)(nil)
)

type (
	// AnnounceAdvReq represents a request for publishing a raw advertisement.
	AnnounceAdvReq struct {
		ReqID string // the index multihash server may use to verify, based on advertisement request
		Token string // the index multihash server used to verify request

		Url       string // url to request multihashes
		ContextID []byte // advertisement context id
		Total     uint64 // total count of multihashes in the advertisement

		MetaData metadata.Metadata

		IsDel bool
	}

	// AnnounceAdvRes  represents the response to an AnnounceAdvReq.
	AnnounceAdvRes struct {
		AdvId cid.Cid `json:"adv_id"`
	}
)

func (a *AnnounceAdvRes) WriteTo(w io.Writer) (n int64, err error) {
	return utils.MarshalToJson(w, a)
}

func (a *AnnounceAdvReq) WriteTo(w io.Writer) (n int64, err error) {
	return utils.MarshalToJson(w, a)
}

func (a *AnnounceAdvRes) ReadFrom(r io.Reader) (n int64, err error) {
	return utils.UnmarshalAsJson(r, a)
}

func (a *AnnounceAdvReq) ReadFrom(r io.Reader) (n int64, err error) {
	return utils.UnmarshalAsJson(r, a)
}
