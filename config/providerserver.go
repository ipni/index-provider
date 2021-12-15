package config

type ProviderServer struct {
	// ListenMultiaddr is the multiaddr string for the node's listen address
	ListenMultiaddr string
	// RetrievalMultiaddrs are the addresses to advertise for data retrieval.
	// Defaults to the provider's libp2p host listen addresses.
	RetrievalMultiaddrs []string
}

// NewProviderServer instantiates a new ProviderServer config with default values.
func NewProviderServer() ProviderServer {
	return ProviderServer{
		ListenMultiaddr: "/ip4/0.0.0.0/tcp/3103",
	}
}
