package config

const (
	// Keep 1024 chunks in cache; keeps 256MiB if chunks are 0.25MiB.
	defaultLinkCacheSize = 1024
	// Multihashes are 128 bytes so 16384 results in 0.25MiB chunk when full.
	defaultLinkedChunkSize = 16384
	defaultPubSubTopic     = "/indexer/ingest/mainnet"
)

type PublisherKind string

const (
	HttpPublisherKind       PublisherKind = "http"
	Libp2pPublisherKind     PublisherKind = "libp2p"
	Libp2pHttpPublisherKind PublisherKind = "libp2phttp"
)

// Ingest configures settings related to the ingestion protocol.
type Ingest struct {
	// LinkCacheSize is the maximum number of links that cash can store before
	// LRU eviction.  If a single linked list has more links than the cache can
	// hold, the cache is resized to be able to hold all links.
	LinkCacheSize int
	// LinkedChunkSize is the number of multihashes in each chunk of in the
	// advertised entries linked list.  If multihashes are 128 bytes, then
	// setting LinkedChunkSize = 16384 will result in blocks of about 2Mb when
	// full.
	LinkedChunkSize int
	// PubSubTopic used to advertise ingestion announcements.
	PubSubTopic string
	// PurgeLinkCache tells whether to purge the link cache on daemon startup.
	PurgeLinkCache bool

	// HttpPublisher configures the dagsync ipnisync publisher.
	HttpPublisher HttpPublisher

	// PublisherKind specifies which dagsync.Publisher implementation to use.
	// When set to "http", the publisher serves plain HTTP. When set to
	// "libp2p" the publisher serves HTTP over libp2p. When set to
	// "libp2phttp", the publisher serves both plain HTTP and HTTP over libp2p.
	//
	// Plain HTTP is disabled if HttpPublisher.ListenMultiaddr is set to "".
	PublisherKind PublisherKind

	// SyncPolicy configures which indexers are allowed to sync advertisements
	// with this provider over a data transfer session.
	SyncPolicy Policy
}

// NewIngest instantiates a new Ingest configuration with default values.
func NewIngest() Ingest {
	return Ingest{
		LinkCacheSize:   defaultLinkCacheSize,
		LinkedChunkSize: defaultLinkedChunkSize,
		PubSubTopic:     defaultPubSubTopic,
		HttpPublisher:   NewHttpPublisher(),
		PublisherKind:   HttpPublisherKind,
		SyncPolicy:      NewPolicy(),
	}
}

// PopulateDefaults replaces zero-values in the config with default values.
func (c *Ingest) PopulateDefaults() {
	if c.LinkCacheSize == 0 {
		c.LinkCacheSize = defaultLinkCacheSize
	}
	if c.LinkedChunkSize == 0 {
		c.LinkedChunkSize = defaultLinkedChunkSize
	}
	if c.PubSubTopic == "" {
		c.PubSubTopic = defaultPubSubTopic
	}
}
