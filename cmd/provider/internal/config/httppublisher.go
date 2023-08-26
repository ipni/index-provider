package config

import (
	"github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
)

type HttpPublisher struct {
	// AnnounceMultiaddr is the address supplied in the announce message
	// telling indexers the address to use to retrieve advertisements. If not
	// specified, the ListenMultiaddr is used.
	AnnounceMultiaddr string
	// ListenMultiaddr is the address of the interface to listen for HTTP
	// requests for advertisements. Set this to "" to disable serving plain
	// HTTP if only libp2phttp is wanted.
	ListenMultiaddr string
	// NoLibp2p disables serving HTTP over libp2p if true. Set this to true to
	// publish over plain HTTP only.
	NoLibp2p bool
}

// NewHttpPublisher instantiates a new config with default values.
func NewHttpPublisher() HttpPublisher {
	return HttpPublisher{
		ListenMultiaddr: "/ip4/0.0.0.0/tcp/3104/http",
	}
}

func (hs *HttpPublisher) ListenNetAddr() (string, error) {
	if hs.ListenMultiaddr == "" {
		return "", nil
	}
	maddr, err := multiaddr.NewMultiaddr(hs.ListenMultiaddr)
	if err != nil {
		return "", err
	}
	httpMultiaddr, _ := multiaddr.NewMultiaddr("/http")
	maddr = maddr.Decapsulate(httpMultiaddr)

	netAddr, err := manet.ToNetAddr(maddr)
	if err != nil {
		return "", err
	}
	return netAddr.String(), nil
}
