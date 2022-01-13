package config

import (
	"time"

	"github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
)

const (
	defaultAdminServerAddr = "/ip4/127.0.0.1/tcp/3102"
	defaultReadTimeout     = Duration(30 * time.Second)
	defaultWriteTimeout    = Duration(30 * time.Second)
)

type AdminServer struct {
	// Admin is the admin API listen address
	ListenMultiaddr string
	ReadTimeout     Duration
	WriteTimeout    Duration
}

// NewAdminServer instantiates a new AdminServer config with default values.
func NewAdminServer() AdminServer {
	return AdminServer{
		ListenMultiaddr: defaultAdminServerAddr,
		ReadTimeout:     defaultReadTimeout,
		WriteTimeout:    defaultWriteTimeout,
	}
}

func (as *AdminServer) ListenNetAddr() (string, error) {
	maddr, err := multiaddr.NewMultiaddr(as.ListenMultiaddr)
	if err != nil {
		return "", err
	}

	netAddr, err := manet.ToNetAddr(maddr)
	if err != nil {
		return "", err
	}
	return netAddr.String(), nil
}

// PopulateDefaults replaces zero-values in the config with default values.
func (c *AdminServer) PopulateDefaults() {
	if c.ListenMultiaddr == "" {
		c.ListenMultiaddr = defaultAdminServerAddr
	}
	if c.ReadTimeout == 0 {
		c.ReadTimeout = defaultReadTimeout
	}
	if c.WriteTimeout == 0 {
		c.WriteTimeout = defaultWriteTimeout
	}
}
