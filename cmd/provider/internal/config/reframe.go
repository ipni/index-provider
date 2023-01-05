package config

import (
	"time"

	"github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
)

const (
	defaultReframeReadTimeout  = Duration(10 * time.Minute)
	defaultReframeWriteTimeout = Duration(10 * time.Minute)
	defaultReframeCidTtl       = Duration(24 * time.Hour)
	defaultReframeChunkSize    = 1_000
	defaultReframeSnapshotSize = 10_000
	defaultPageSize            = 5_000
)

// Reframe tracks the configuration of reframe serber. If specified, index provider will expose a reframe server that will
// allow an IPFS node to advertise their CIDs through the delegated routing protocol.
type Reframe struct {
	ListenMultiaddr string
	ReadTimeout     Duration
	WriteTimeout    Duration
	// CidTtl is a lifetime of a cid after which it is considered expired
	CidTtl Duration
	// ChunkSize is size of a chunk before it gets advertised to an indexer.
	// In other words it's a number of CIDs per advertisement
	ChunkSize int
	// SnapshotSize is the maximum number of records in the Provide payload after which it is considered a snapshot.
	// Snapshots don't have individual timestamps recorded into the datastore. Instead, timestamps are recorded as a binary blob after processing is done.
	SnapshotSize int
	// ProviderID is a Peer ID of the IPFS node that the reframe server is expecting advertisements from
	ProviderID string
	// DsPageSize is a size of the database page that is going to be used on reframe server initialisation.
	DsPageSize int
	// Addrs is a list of multiaddresses of the IPFS node that the reframe server is expecting advertisements from
	Addrs []string
}

// NewReframe instantiates a new Reframe config with default values.
func NewReframe() Reframe {
	return Reframe{
		// we would like this functionality to be off by default
		ProviderID:      "",
		ListenMultiaddr: "",
		ReadTimeout:     defaultReframeReadTimeout,
		WriteTimeout:    defaultReframeWriteTimeout,
		CidTtl:          defaultReframeCidTtl,
		ChunkSize:       defaultReframeChunkSize,
		SnapshotSize:    defaultReframeSnapshotSize,
		DsPageSize:      defaultPageSize,
	}
}

// PopulateDefaults replaces zero-values in the config with default values.
func (c *Reframe) PopulateDefaults() {
	if c.ReadTimeout == 0 {
		c.ReadTimeout = defaultReframeReadTimeout
	}
	if c.WriteTimeout == 0 {
		c.WriteTimeout = defaultReframeWriteTimeout
	}
	if c.CidTtl == 0 {
		c.CidTtl = defaultReframeCidTtl
	}
	if c.ChunkSize == 0 {
		c.ChunkSize = defaultReframeChunkSize
	}
	if c.SnapshotSize == 0 {
		c.SnapshotSize = defaultReframeSnapshotSize
	}
	if c.DsPageSize == 0 {
		c.DsPageSize = defaultPageSize
	}
}

func (as *Reframe) ListenNetAddr() (string, error) {
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
