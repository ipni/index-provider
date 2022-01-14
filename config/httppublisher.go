package config

import (
	"github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
)

type HttpPublisher struct {
	ListenMultiaddr string
}

// NewHttpPublisher instantiates a new config with default values.
func NewHttpPublisher() HttpPublisher {
	return HttpPublisher{
		ListenMultiaddr: "/ip4/0.0.0.0/tcp/3104/http",
	}
}

func (hs *HttpPublisher) ListenNetAddr() (string, error) {
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
