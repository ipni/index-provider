package adminserver

import (
	ma "github.com/multiformats/go-multiaddr"
)

// ConnectReq request
type ConnectReq struct {
	Addr ma.Multiaddr
}
