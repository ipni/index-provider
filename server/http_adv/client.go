package http_adv

import (
	"bytes"
	"context"
	"github.com/filecoin-project/index-provider/server/utils"
	"net/http"

	"github.com/filecoin-project/index-provider/metadata"
	"github.com/ipfs/go-cid"
)

type Client struct {
	url, token         string
	mhitUrl, mhitToken string
}

func NewHttpAdvClient(url, token, mhitUrl, mhitToken string) (*Client, error) {
	return &Client{url: url, token: token, mhitUrl: mhitUrl, mhitToken: mhitToken}, nil
}

func (c *Client) AnnounceAdv(ctx context.Context, ctxID []byte, reqID string, total uint64,
	md metadata.Metadata, isDel bool) (cid.Cid, error) {
	var req = &AnnounceAdvReq{
		ReqID:     reqID,
		Token:     c.mhitToken,
		Url:       c.mhitUrl,
		ContextID: ctxID,
		Total:     total,
		MetaData:  md,
		IsDel:     isDel,
	}

	body := bytes.NewBuffer(nil)
	if _, err := req.WriteTo(body); err != nil {
		return cid.Undef, err
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url+announceAdvPath, body)
	if err != nil {
		return cid.Undef, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", c.token)
	resp, err := (&http.Client{}).Do(httpReq)
	if err != nil {
		return cid.Undef, err
	}

	if resp.StatusCode != http.StatusOK {
		return cid.Undef, utils.HttpStatusCodeErr(resp)
	}

	var advRes = &AnnounceAdvRes{}
	if _, err := advRes.ReadFrom(resp.Body); err != nil {
		return cid.Undef, err
	}

	return advRes.AdvId, nil
}
