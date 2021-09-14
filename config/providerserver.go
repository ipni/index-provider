package config

const (
	defaultNodeMultiaddr = "/ip4/0.0.0.0/tcp/3103"
)

type ProviderServer struct {
	// ListenMultiaddr captures the (string) multiaddr addresses for the node.
	ListenMultiaddr string
}
