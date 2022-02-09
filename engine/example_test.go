package engine_test

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"time"

	datatransfer "github.com/filecoin-project/go-data-transfer"
	dtimpl "github.com/filecoin-project/go-data-transfer/impl"
	dtnetwork "github.com/filecoin-project/go-data-transfer/network"
	gstransport "github.com/filecoin-project/go-data-transfer/transport/graphsync"
	provider "github.com/filecoin-project/index-provider"
	"github.com/filecoin-project/index-provider/config"
	"github.com/filecoin-project/index-provider/engine"
	"github.com/filecoin-project/index-provider/metadata"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	gsimpl "github.com/ipfs/go-graphsync/impl"
	gsnet "github.com/ipfs/go-graphsync/network"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/multiformats/go-multihash"
)

// Example_advertiseHelloWorld shows an example of instantiating an engine.Engine and publishing
// and advertisement for a sample content.
//
// Note that the advertisement published uses metadata.BitswapMetadata. This is for demonstrative
// purposes only. The example does not set up the retrieval side for the content.
func Example_advertiseHelloWorld() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Get the multihash of content to advertise
	content := "Hello World!"
	sayHelloCtxID := "Say hello"
	fmt.Printf("Preparing to advertise content: '%s'\n", string(content))
	mh, err := multihash.Sum([]byte(content), multihash.SHA2_256, -1)
	if err != nil {
		panic(err)
	}
	fmt.Printf("✓ Generated content multihash: %s\n", mh.B58String())

	// Generate a random peer identity
	priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		panic(err)
	}
	fmt.Println("✓ Generated random peer identity.")

	// Create a new libp2p host
	h, err := libp2p.New(libp2p.Identity(priv))
	if err != nil {
		panic(err)
	}
	// Only print the first three characters to keep golang example output happy.
	fmt.Printf("✓ Instantiated new libp2p host with peer ID: %s...\n", h.ID().String()[:3])

	store := dssync.MutexWrap(datastore.NewMapDatastore())
	defer store.Close()

	// Set up datatrasfer and graphsync
	dt, err := startDatatrasfer(h, ctx, store)
	if err != nil {
		panic(err)
	}
	fmt.Println("✓ Datatransfer manager is ready.")

	// Construct a new provider engine with default configuration.
	cfg := config.NewIngest()
	engine, err := engine.New(cfg, priv, dt, h, store, nil)
	if err != nil {
		panic(err)
	}
	fmt.Println("✓ Instantiated provider engine with config:")
	fmt.Printf("   announcements topic: %s\n", cfg.PubSubTopic)

	engine.RegisterCallback(func(ctx context.Context, contextID []byte) (provider.MultihashIterator, error) {
		if string(contextID) == sayHelloCtxID {
			return &singleMhIterator{mh: mh}, nil
		}
		return nil, fmt.Errorf("no content is found for context ID: %v", contextID)
	})
	fmt.Printf("✓ Registered callback for context ID: %s\n", sayHelloCtxID)

	// Start the engine
	if err = engine.Start(context.Background()); err != nil {
		panic(err)
	}
	fmt.Println("✓ Provider engine started.")

	// Note that this example publishes an ad with bitswap metadata as an example.
	// But it does not instantiate a bitswap server to serve retrievals.
	adCid, err := engine.NotifyPut(context.Background(), []byte(sayHelloCtxID), metadata.BitswapMetadata)
	if err != nil {
		panic(err)
	}
	// Only print the first three characters to keep golang example output happy.
	fmt.Printf("✓ Published advertisement for content with CID: %s...\n", adCid.String()[:3])

	if err := engine.Shutdown(); err != nil {
		panic(err)
	}

	//Output:
	//Preparing to advertise content: 'Hello World!'
	//✓ Generated content multihash: QmWvQxTqbG2Z9HPJgG57jjwR154cKhbtJenbyYTWkjgF3e
	//✓ Generated random peer identity.
	//✓ Instantiated new libp2p host with peer ID: 12D...
	//✓ Datatransfer manager is ready.
	//✓ Instantiated provider engine with config:
	//    announcements topic: /indexer/ingest/mainnet
	//✓ Registered callback for context ID: Say hello
	//✓ Provider engine started.
	//✓ Published advertisement for content with CID: bag...
}

func startDatatrasfer(h host.Host, ctx context.Context, ds datastore.Batching) (datatransfer.Manager, error) {
	gn := gsnet.NewFromLibp2pHost(h)
	dtNet := dtnetwork.NewFromLibp2pHost(h)
	gs := gsimpl.New(ctx, gn, cidlink.DefaultLinkSystem())
	tp := gstransport.NewTransport(h.ID(), gs)
	dt, err := dtimpl.NewDataTransfer(ds, dtNet, tp)
	if err != nil {
		return nil, err
	}
	ready := make(chan error, 1)
	dt.OnReady(func(err error) {
		ready <- err
	})
	if err := dt.Start(ctx); err != nil {
		return nil, err
	}

	timer := time.NewTimer(2 * time.Second)
	select {
	case readyErr := <-ready:
		if readyErr != nil {
			return nil, readyErr
		}
	case <-timer.C:
		return nil, errors.New("timed out waiting for datatrasfer to be ready")
	}
	return dt, nil
}

type singleMhIterator struct {
	offset int
	mh     multihash.Multihash
}

func (s *singleMhIterator) Next() (multihash.Multihash, error) {
	if s.offset == 0 {
		s.offset++
		return s.mh, nil
	}
	return nil, io.EOF
}
