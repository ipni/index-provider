package config

const (
	defaultLinkCacheSize   = 1024
	defaultLinkedChunkSize = 100
	defaultPubSubTopic     = "indexer/ingest"
)

// Ingest tracks the configuration related to the ingestion protocol
type Ingest struct {
	// LinkCacheSize is the maximum number of links that cash can store before LRU eviction.  If a
	// single linked list has more links than the cache can hold, the cache is
	// resized to be able to hold all links.
	LinkCacheSize int
	// LinkedChunkSize is the number of hashes in each chunk of ingestion
	// linked list.
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

// OverrideUnsetToDefaults replaces zero-values in the config with default values.
func (cfg *Ingest) OverrideUnsetToDefaults() {
	if cfg.LinkCacheSize == 0 {
		cfg.LinkCacheSize = defaultLinkCacheSize
	}
	if cfg.LinkedChunkSize == 0 {
		cfg.LinkedChunkSize = defaultLinkedChunkSize
	}
	if cfg.PubSubTopic == "" {
		cfg.PubSubTopic = defaultPubSubTopic
	}
}
