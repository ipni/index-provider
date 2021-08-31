package p2pclient

import (
	"context"
	"fmt"

	v0 "github.com/filecoin-project/indexer-reference-provider/api/v0"
	"github.com/filecoin-project/indexer-reference-provider/api/v0/models"
	pb "github.com/filecoin-project/indexer-reference-provider/api/v0/pb"
	"github.com/filecoin-project/indexer-reference-provider/internal/libp2pclient"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
)

type Provider struct {
	p2pc *libp2pclient.Client
}

func NewProvider(ctx context.Context, h host.Host, options ...libp2pclient.ClientOption) (*Provider, error) {
	client, err := libp2pclient.NewClient(ctx, h, v0.ProviderProtocolID, options...)
	if err != nil {
		return nil, err
	}
	return &Provider{
		p2pc: client,
	}, nil
}

func (cl *Provider) GetLatestAdv(ctx context.Context, p peer.ID) (*models.AdResponse, error) {
	req := &pb.ProviderMessage{
		Type: pb.ProviderMessage_GET_LATEST,
	}

	data, err := cl.sendRecv(ctx, p, req, pb.ProviderMessage_AD_RESPONSE)
	if err != nil {
		return nil, err
	}

	return models.UnmarshalResp(data)
}

func (cl *Provider) GetAdv(ctx context.Context, p peer.ID, id cid.Cid) (*models.AdResponse, error) {
	data, err := models.MarshalReq(&models.AdRequest{ID: id})
	if err != nil {
		return nil, err
	}
	req := &pb.ProviderMessage{
		Type: pb.ProviderMessage_GET_AD,
		Data: data,
	}

	data, err = cl.sendRecv(ctx, p, req, pb.ProviderMessage_AD_RESPONSE)
	if err != nil {
		return nil, err
	}

	return models.UnmarshalResp(data)
}

func (cl *Provider) sendRecv(ctx context.Context, p peer.ID, req *pb.ProviderMessage, expectRspType pb.ProviderMessage_MessageType) ([]byte, error) {
	resp := new(pb.ProviderMessage)
	err := cl.p2pc.SendRequest(ctx, p, req, func(data []byte) error {
		return resp.Unmarshal(data)
	})
	if err != nil {
		return nil, fmt.Errorf("failed to send request to indexer: %s", err)
	}
	if resp.GetType() != expectRspType {
		if resp.GetType() == pb.ProviderMessage_ERROR_RESPONSE {
			return nil, v0.DecodeError(resp.GetData())
		}
		return nil, fmt.Errorf("response type is not %s", expectRspType.String())
	}
	return resp.GetData(), nil
}
