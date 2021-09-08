package config

const (
	defaultAdminAddr = "/ip4/127.0.0.1/tcp/3102"
	defaultP2PAddr   = "/ip4/0.0.0.0/tcp/3103"
)

// Addresses stores the (string) multiaddr addresses for the node.
type Addresses struct {
	// Admin is the admin API listen address
	Admin string
	// P2PMaddr is the libp2p host multiaddr
	P2PAddr string
}
