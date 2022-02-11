package config

import "time"

const (
	// Keep 1024 chunks in cache; keeps 256MiB if chunks are 0.25MiB.
	defaultLinkCacheSize = 1024
	// Multihashes are 128 bytes so 16384 results in 0.25MiB chunk when full.
	defaultLinkedChunkSize   = 16384
	defaultPubSubTopic       = "/indexer/ingest/mainnet"
	defaultStartPublishDelay = Duration(time.Minute)
)

type PublisherKind string

const (
	DTSyncPublisherKind PublisherKind = "dtsync"
	HttpPublisherKind   PublisherKind = "http"
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

	// HttpPublisher configures the go-legs httpsync publisher.
	HttpPublisher HttpPublisher

	// PublisherKind specifies which legs.Publisher implementation to use.
	PublisherKind PublisherKind

	// StartPublishDelay specifies how long to wait, after startup, to
	// re-publish the latest advertisement notification.  This delay gives time
	// to connect to other nodes and establish gossip mesh before sending
	// pubsub advertisement notification message.
	StartPublishDelay Duration
}

// NewIngest instantiates a new Ingest configuration with default values.
func NewIngest() Ingest {
	return Ingest{
		LinkCacheSize:     defaultLinkCacheSize,
		LinkedChunkSize:   defaultLinkedChunkSize,
		PubSubTopic:       defaultPubSubTopic,
		HttpPublisher:     NewHttpPublisher(),
		PublisherKind:     DTSyncPublisherKind,
		StartPublishDelay: defaultStartPublishDelay,
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
