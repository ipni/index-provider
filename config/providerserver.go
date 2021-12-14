package config

const (
	defaultNodeMultiaddr = "/ip4/0.0.0.0/tcp/3103"
)

type ProviderServer struct {
	// ListenMultiaddr is the multiaddr string for the node's listen address
	ListenMultiaddr string
	// RetrievalMultiaddrs are the addresses to advertise for data retrieval
	RetrievalMultiaddrs []string
	EnableHttpPublisher bool
	HttpAddr            string
}
