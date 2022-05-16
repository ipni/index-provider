package http_mh_iter

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"github.com/filecoin-project/index-provider/server/utils"
	"io"
	"net/http"

	provider "github.com/filecoin-project/index-provider"
	"github.com/multiformats/go-multihash"
)

var _ provider.MultihashIterator = (*HttpMhIterator)(nil)

type HttpMhIterOption struct {
	ReqID, Url, Token string
	PageSize          uint64
}

type HttpMhIterator struct {
	HttpMhIterOption

	contextID []byte
	total     uint64

	mhs []multihash.Multihash
	pos uint64

	invalid bool
}

func NewHttpMhIterator(opts *HttpMhIterOption, contextID []byte, total uint64) *HttpMhIterator {
	if opts.PageSize == 0 {
		opts.PageSize = defMulhashIterPageSize
	}
	hmi := &HttpMhIterator{
		HttpMhIterOption: *opts,
		total:            total,
		contextID:        contextID,
		mhs:              make([]multihash.Multihash, 0, total),
	}
	return hmi
}

func (it *HttpMhIterator) Next() (multihash.Multihash, error) {
	if it.pos >= uint64(len(it.mhs)) {
		if it.pos >= it.total {
			return nil, io.EOF
		}
		if err := it.nextPage(); err != nil {
			return nil, fmt.Errorf("load next page failed:%w", err)
		}
	}
	mh := it.mhs[it.pos]
	it.pos++
	return mh, nil
}

func (it *HttpMhIterator) nextPage() error {
	if it.total == uint64(len(it.mhs)) {
		return io.EOF
	}

	if it.invalid {
		return fmt.Errorf("invalid http multihash iterator")
	}

	var req = &nextPageRequest{
		AdvReqID:  it.ReqID,
		ContextID: it.contextID,
		Start:     uint64(len(it.mhs)),
		Count:     it.PageSize,
	}

	b64CtxID := base64.StdEncoding.EncodeToString(it.contextID)

	var body = bytes.NewBuffer(nil)
	if _, err := req.WriteTo(body); err != nil {
		return fmt.Errorf("req to http.Body failed:%w", err)
	}
	httpReq, err := http.NewRequest(http.MethodPost, it.Url+nextPagePath, body)
	if err != nil {
		return fmt.Errorf("new http request failed:%w", err)
	}

	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", it.Token)
	httpReq.Header.Set("ReqID", it.ReqID)

	resp, err := (&http.Client{}).Do(httpReq)
	if err != nil {
		return fmt.Errorf("post next page request failed:%w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return utils.HttpStatusCodeErr(resp)
	}

	var res nextPageResponse
	if _, err := res.ReadFrom(resp.Body); err != nil {
		return fmt.Errorf("parse nextPageResponse failed:%w", err)
	}

	var count = uint64(len(res.Mhs))
	if count != req.Count {
		if count > req.Count || count+req.Start != it.total {
			it.invalid = true
			return fmt.Errorf("requested multihash count not equal returns(%d != %d)", req.Count, count)
		}
	}
	it.mhs = append(it.mhs, res.Mhs...)

	log.Infof("multihash iterator load contextID:%s, from:%d, to:%d, success", b64CtxID, req.Start, req.Count)

	return nil
}
