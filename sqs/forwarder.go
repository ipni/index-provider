package sqs

import (
	"context"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	stiapi "github.com/filecoin-project/storetheindex/api/v0"
	"github.com/filecoin-project/storetheindex/api/v0/ingest/schema"
	"github.com/filecoin-project/index-provider/engine"
	logging "github.com/ipfs/go-log/v2"	
	"github.com/ipld/go-ipld-prime"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/multiformats/go-multibase"
	"github.com/multiformats/go-multihash"
)

type SQSForwarder struct {
	Context context.Context
	PeerID string
	Engine *engine.Engine
	Client *sqs.Client
	PrivKey crypto.PrivKey
	Link ipld.LinkSystem
	Multiaddrs []string
	QueueUrl string
	ErrorChannel chan error

	lastAdvertisement schema.Link_Advertisement
}

var log = logging.Logger("sqs")

func NewSQSForwarder(ctx context.Context, engine *engine.Engine, peerID string) (*SQSForwarder, error) {
	cfg, err := config.LoadDefaultConfig(ctx)

	if err != nil {
		return nil, err
	}
	
	client := sqs.NewFromConfig(cfg)
	privKey, link := engine.GetAdvertisementPublishingParameters()

	forwarder := &SQSForwarder{ctx, peerID, engine, client, privKey, link, strings.Split(os.Getenv("BITSWAP_PEER_MULTIADDR"), ","), os.Getenv("SQS_PUBLISHING_QUEUE_URL"), make(chan error), nil}
	log.Infof("starting SQS forwarding from queue %s", forwarder.QueueUrl)

	return forwarder, nil
}

func (f *SQSForwarder) Start() {
	go func(){
		for {
			// Receive messages from SQS
			input := &sqs.ReceiveMessageInput{
				MessageAttributeNames: []string{
					string(types.QueueAttributeNameAll),
				},
				QueueUrl: aws.String(f.QueueUrl),
				MaxNumberOfMessages: 10,
				VisibilityTimeout: 0,
			}

			response, err := f.Client.ReceiveMessage(f.Context, input)

			if err != nil {
				f.ErrorChannel <- err
				return
			}

			totalMessages := len(response.Messages)
			if totalMessages > 0 {
				multihashes := make([]string, len(response.Messages)) 

				for i, message := range(response.Messages) {
					multihashes[i] = *message.Body 
				}

				// Advertise all multihashes
				err = f.Publish(multihashes)

				if err != nil {					
					f.ErrorChannel <- err
					return
				}
			} else {
				// When no messages are received, wait 15 seconds before retrying			
				time.Sleep(15 * time.Second)
			}
		}
	}()
}

func (f *SQSForwarder) Publish(multihashes []string) (error) {
	mhs := make([]multihash.Multihash, len(multihashes))

	for i, rmhs := range(multihashes) {
		// We cant use multihash.FromB58String here since the JS implementation uses base58btc
		_, bytes, err := multibase.Decode(rmhs)

		if err != nil {
			return err
		}

		_, mHash, err := multihash.MHFromBytes(bytes)

		if err != nil {
			return err
		}

		mhs[i] = mHash
	}

	cidsLnk, err := schema.NewListOfMhs(f.Link, mhs)

	if err != nil {
		return err
	}

	adv, _, err := schema.NewAdvertisementWithLink(
		f.Link, f.PrivKey, f.lastAdvertisement, cidsLnk, mhs[0], stiapi.Metadata{ProtocolID: 0x300000, Data: []byte{}}, false, f.PeerID, f.Multiaddrs,
	)

	if err != nil {
		return err
	}

	cid, err := f.Engine.Publish(f.Context, adv)
	
	if err != nil {
		return err
	}

	f.lastAdvertisement, err = schema.AdvertisementLink(f.Link, adv)

	if err != nil {
		return err
	}

	for _, rmhs := range(multihashes) {
		log.Infof("Published multihash %s via CID %s", rmhs, cid)
	}

	return nil
}

func (f *SQSForwarder) Shutdown() error {
	log.Infof("stopping SQS forwarding")

	return nil
}