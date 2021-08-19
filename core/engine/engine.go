package engine

import (
	"bytes"
	"context"
	"io"

	"github.com/filecoin-project/go-indexer-core"
	"github.com/filecoin-project/indexer-reference-provider/core"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-ipld-prime"
	"github.com/libp2p/go-libp2p-core/host"
	peer "github.com/libp2p/go-libp2p-core/peer"
	legs "github.com/willscott/go-legs"

	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
)

var log = logging.Logger("reference-provider")

var _ core.Interface = &Engine{}

// Engine is an implementation of the core reference provider interface.
type Engine struct {
	// Host running the provider process.
	host host.Host
	// Datastore used for persistence of different assets
	// (advertisements, indexed data, etc.).
	ds datastore.Batching
	// Local index with all provided data.
	// NOTE: We may want to wrap indexer.Interface into datastore interface
	// to be able to use it as a datastore right away.
	index indexer.Interface
	// go-legs subscriber for IPLD-aware ingestion
	ipldIngest legs.LegPublisher
	// pubsubtopic where the provider will push advertisements
	pubSubTopic string
}

// New creates a reference provider engine with the corresponding config.
func New(ctx context.Context, host host.Host, ds datastore.Batching, index indexer.Interface, pubSubTopic string) (*Engine, error) {
	log.Debugw("Starting new reference provider engine")
	lsys := mkLinkSystem(ds)
	lp, err := legs.NewPublisher(ctx, ds, host, pubSubTopic, lsys)
	if err != nil {
		return nil, err
	}
	return &Engine{
		host:        host,
		ds:          ds,
		index:       index,
		ipldIngest:  lp,
		pubSubTopic: pubSubTopic,
	}, nil
}

// TODO: This will probably change once we start implementing the actual ingestion protocol.
func mkLinkSystem(ds datastore.Batching) ipld.LinkSystem {
	lsys := cidlink.DefaultLinkSystem()
	lsys.StorageReadOpener = func(lctx ipld.LinkContext, lnk ipld.Link) (io.Reader, error) {
		c := lnk.(cidlink.Link).Cid
		val, err := ds.Get(datastore.NewKey(c.String()))
		if err != nil {
			return nil, err
		}
		return bytes.NewBuffer(val), nil
	}
	lsys.StorageWriteOpener = func(_ ipld.LinkContext) (io.Writer, ipld.BlockWriteCommitter, error) {
		buf := bytes.NewBuffer(nil)
		return buf, func(lnk ipld.Link) error {
			c := lnk.(cidlink.Link).Cid
			return ds.Put(datastore.NewKey(c.String()), buf.Bytes())
		}, nil
	}
	return lsys
}

func (e *Engine) PublishLocal(ctx context.Context, ad core.Advertisement) error {
	panic("not implemented")
}

func (e *Engine) Publish(ctx context.Context, ad core.Advertisement) error {
	panic("not implemented")
}

func (e *Engine) PushAdv(ctx context.Context, indexer peer.ID, ad core.Advertisement) error {
	panic("not implemented")
}

func (e *Engine) Push(ctx context.Context, indexer peer.ID, cid cid.Cid, val indexer.Value) {
	panic("not implemented")
}

func (e *Engine) Put(cids []cid.Cid, val indexer.Value) (bool, error) {
	panic("not implemented")
}

func (e *Engine) GetAdv(id cid.Cid) (core.Advertisement, error) {
	panic("not implemented")
}

func (e *Engine) GetLatestAdv() (core.Advertisement, error) {
	panic("not implemented")
}
