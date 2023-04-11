package config

import (
	"time"

	"github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
)

const (
	defaultDelegatedRoutingReadTimeout  = Duration(10 * time.Minute)
	defaultDelegatedRoutingWriteTimeout = Duration(10 * time.Minute)
	defaultDelegatedRoutingCidTtl       = Duration(24 * time.Hour)
	defaultAdFlushFrequency             = Duration(10 * time.Minute)
	defaultDelegatedRoutingChunkSize    = 1_000
	defaultDelegatedRoutingSnapshotSize = 10_000
	defaultPageSize                     = 5_000
)

// DelegatedRouting tracks the configuration of delegated routing server. If specified, index provider will expose a delegated routing server that will
// allow an IPFS node to advertise their CIDs through the delegated routing protocol.
type DelegatedRouting struct {
	ListenMultiaddr string
	ReadTimeout     Duration
	WriteTimeout    Duration
	// CidTtl is a lifetime of a cid after which it is considered expired
	CidTtl Duration
	// AdFlushFrequency defines a frequency of a flush operation that is going to be performed on the current chunk. In other words a non empty
	// current chunk will be converted to an advertisement and published if it's older than this value. Set to 0 to disable.
	AdFlushFrequency Duration
	// ChunkSize is size of a chunk before it gets advertised to an indexer.
	// In other words it's a number of CIDs per advertisement
	ChunkSize int
	// SnapshotSize is the maximum number of records in the Provide payload after which it is considered a snapshot.
	// Snapshots don't have individual timestamps recorded into the datastore. Instead, timestamps are recorded as a binary blob after processing is done.
	SnapshotSize int
	// ProviderID is a Peer ID of the IPFS node that the delegated routing server is expecting advertisements from
	ProviderID string
	// DsPageSize is a size of the database page that is going to be used on delegated routing server initialisation.
	DsPageSize int
	// Addrs is a list of multiaddresses of the IPFS node that the delegated routing server is expecting advertisements from
	Addrs []string
}

// NewDelegatedRouting instantiates a new delegated routing config with default values.
func NewDelegatedRouting() DelegatedRouting {
	return DelegatedRouting{
		// we would like this functionality to be off by default
		ProviderID:       "",
		ListenMultiaddr:  "",
		ReadTimeout:      defaultDelegatedRoutingReadTimeout,
		WriteTimeout:     defaultDelegatedRoutingWriteTimeout,
		CidTtl:           defaultDelegatedRoutingCidTtl,
		ChunkSize:        defaultDelegatedRoutingChunkSize,
		SnapshotSize:     defaultDelegatedRoutingSnapshotSize,
		AdFlushFrequency: defaultAdFlushFrequency,
		DsPageSize:       defaultPageSize,
	}
}

// PopulateDefaults replaces zero-values in the config with default values.
func (c *DelegatedRouting) PopulateDefaults() {
	if c.ReadTimeout == 0 {
		c.ReadTimeout = defaultDelegatedRoutingReadTimeout
	}
	if c.WriteTimeout == 0 {
		c.WriteTimeout = defaultDelegatedRoutingWriteTimeout
	}
	if c.CidTtl == 0 {
		c.CidTtl = defaultDelegatedRoutingCidTtl
	}
	if c.AdFlushFrequency == 0 {
		c.AdFlushFrequency = defaultAdFlushFrequency
	}
	if c.ChunkSize == 0 {
		c.ChunkSize = defaultDelegatedRoutingChunkSize
	}
	if c.SnapshotSize == 0 {
		c.SnapshotSize = defaultDelegatedRoutingSnapshotSize
	}
	if c.DsPageSize == 0 {
		c.DsPageSize = defaultPageSize
	}
}

func (as *DelegatedRouting) ListenNetAddr() (string, error) {
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
