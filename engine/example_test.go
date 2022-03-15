package engine_test

import (
	"context"
	"fmt"
	"io"

	provider "github.com/filecoin-project/index-provider"
	"github.com/filecoin-project/index-provider/engine"
	"github.com/filecoin-project/index-provider/metadata"
	"github.com/libp2p/go-libp2p"
	"github.com/multiformats/go-multihash"
)

// Example_advertiseHelloWorld shows an example of instantiating an engine.Engine and publishing
// and advertisement for a sample content.
//
// Note that the advertisement published uses metadata.BitswapMetadata. This is for demonstrative
// purposes only. The example does not set up the retrieval side for the content.
func Example_advertiseHelloWorld() {
	// Get the multihash of content to advertise
	content := "Hello World!"
	sayHelloCtxID := "Say hello"
	fmt.Printf("Preparing to advertise content: '%s'\n", string(content))
	mh, err := multihash.Sum([]byte(content), multihash.SHA2_256, -1)
	if err != nil {
		panic(err)
	}
	fmt.Printf("✓ Generated content multihash: %s\n", mh.B58String())

	// Create a new libp2p host
	h, err := libp2p.New()
	if err != nil {
		panic(err)
	}
	// Only print the first three characters to keep golang example output happy.
	fmt.Printf("✓ Instantiated new libp2p host with peer ID: %s...\n", h.ID().String()[:2])

	// Construct a new provider engine with given libp2p host that announces advertisements over
	// gossipsub and datatrasfer/graphsync.
	engine, err := engine.New(engine.WithHost(h), engine.WithPublisherKind(engine.DataTransferPublisher))
	if err != nil {
		panic(err)
	}
	fmt.Println("✓ Instantiated provider engine")
	defer engine.Shutdown()

	engine.RegisterMultihashLister(func(ctx context.Context, contextID []byte) (provider.MultihashIterator, error) {
		if string(contextID) == sayHelloCtxID {
			return &singleMhIterator{mh: mh}, nil
		}
		return nil, fmt.Errorf("no content is found for context ID: %v", contextID)
	})
	fmt.Printf("✓ Registered lister for context ID: %s\n", sayHelloCtxID)

	// Start the engine
	if err = engine.Start(context.Background()); err != nil {
		panic(err)
	}
	fmt.Println("✓ Provider engine started.")

	// Multiple transports can be included in metadata.
	md := metadata.New(metadata.Bitswap{})

	// Note that this example publishes an ad with bitswap metadata as an example.
	// But it does not instantiate a bitswap server to serve retrievals.
	adCid, err := engine.NotifyPut(context.Background(), []byte(sayHelloCtxID), md)
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
	//✓ Instantiated new libp2p host with peer ID: Qm...
	//✓ Instantiated provider engine
	//✓ Registered lister for context ID: Say hello
	//✓ Provider engine started.
	//✓ Published advertisement for content with CID: bag...
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
