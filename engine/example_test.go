package engine_test

import (
	"context"
	crand "crypto/rand"
	"fmt"
	"io"

	"github.com/ipni/go-libipni/metadata"
	provider "github.com/ipni/index-provider"
	"github.com/ipni/index-provider/engine"
	"github.com/ipni/index-provider/engine/xproviders"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
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
	fmt.Printf("✓ Instantiated new libp2p host with peer ID: %s...\n", h.ID().String()[:4])

	// Construct a new provider engine with given libp2p host that announces advertisements over
	// gossipsub.
	engine, err := engine.New(engine.WithHost(h), engine.WithPublisherKind(engine.Libp2pPublisher))
	if err != nil {
		panic(err)
	}
	fmt.Println("✓ Instantiated provider engine")
	defer engine.Shutdown()

	engine.RegisterMultihashLister(func(ctx context.Context, provider peer.ID, contextID []byte) (provider.MultihashIterator, error) {
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
	md := metadata.Default.New(metadata.Bitswap{})

	// Note that this example publishes an ad with bitswap metadata as an example.
	// But it does not instantiate a bitswap server to serve retrievals.
	adCid, err := engine.NotifyPut(context.Background(), nil, []byte(sayHelloCtxID), md)
	if err != nil {
		panic(err)
	}
	// Only print the first three characters to keep golang example output happy.
	fmt.Printf("✓ Published advertisement for content with CID: %s...\n", adCid.String()[:3])

	// Create an advertisement with ExtendedProviders
	providerID := h.ID()
	privKey := h.Peerstore().PrivKey(providerID)
	addrs := h.Addrs()
	mdBytes, err := md.MarshalBinary()
	if err != nil {
		panic(err)
	}

	// Generate random keys and identity for a new ExtendedProvider
	xPrivKey, _, err := crypto.GenerateEd25519Key(crand.Reader)
	if err != nil {
		panic(err)
	}
	xProviderID, err := peer.IDFromPrivateKey(xPrivKey)
	if err != nil {
		panic(err)
	}
	xAddr, err := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/9000")
	if err != nil {
		panic(err)
	}

	// Build and sign ExtendedProviders advertisement
	xAd, err := xproviders.NewAdBuilder(providerID, privKey, addrs).
		WithLastAdID(adCid).
		WithExtendedProviders(xproviders.NewInfo(xProviderID, xPrivKey, mdBytes, []multiaddr.Multiaddr{xAddr})).
		WithOverride(true).
		WithContextID([]byte("sample-context-id")).
		WithMetadata(mdBytes).
		BuildAndSign()
	if err != nil {
		panic(err)
	}

	// Publish the advertisement using engine
	xAdCid, err := engine.Publish(context.Background(), *xAd)
	if err != nil {
		panic(err)
	}
	// Only print the first three characters to keep golang example output happy.
	fmt.Printf("✓ Published ExtendedProviders advertisement for content with CID: %s...\n", xAdCid.String()[:3])

	if err := engine.Shutdown(); err != nil {
		panic(err)
	}

	//Output:
	//Preparing to advertise content: 'Hello World!'
	//✓ Generated content multihash: QmWvQxTqbG2Z9HPJgG57jjwR154cKhbtJenbyYTWkjgF3e
	//✓ Instantiated new libp2p host with peer ID: 12D3...
	//✓ Instantiated provider engine
	//✓ Registered lister for context ID: Say hello
	//✓ Provider engine started.
	//✓ Published advertisement for content with CID: bag...
	//✓ Published ExtendedProviders advertisement for content with CID: bag...
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
