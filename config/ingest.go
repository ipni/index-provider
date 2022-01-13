package config

const (
	defaultLinkCacheSize   = 1024
	defaultLinkedChunkSize = 16384
	defaultPubSubTopic     = "indexer/ingest"
)

// Ingest tracks the configuration related to the ingestion protocol
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
}

// NewIngest instantiates a new Ingest configuration with default values.
func NewIngest() Ingest {
	return Ingest{
		LinkCacheSize:   defaultLinkCacheSize,
		LinkedChunkSize: defaultLinkedChunkSize,
		PubSubTopic:     defaultPubSubTopic,
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
