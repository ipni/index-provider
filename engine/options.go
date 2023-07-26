package engine

import (
	"fmt"
	"net/url"

	datatransfer "github.com/filecoin-project/go-data-transfer/v2"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	_ "github.com/ipni/go-libipni/maurl"
	"github.com/ipni/index-provider/engine/chunker"
	"github.com/ipni/index-provider/engine/policy"
	"github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multicodec"
)

const (
	// NoPublisher indicates that no announcements are made to the network and all advertisements
	// are only stored locally.
	NoPublisher PublisherKind = ""

	// DataTransferPublisher makes announcements over a gossipsub topic and exposes a
	// datatransfer/graphsync server that allows peers in the network to sync advertisements.
	DataTransferPublisher PublisherKind = "dtsync"

	// HttpPublisher exposes a HTTP server that announces published advertisements and allows peers
	// in the network to sync them over raw HTTP transport.
	HttpPublisher PublisherKind = "http"
)

type (
	// PublisherKind represents the kind of publisher to use in order to announce a new
	// advertisement to the network.
	// See: WithPublisherKind, NoPublisher, DataTransferPublisher, HttpPublisher.
	PublisherKind string

	// Option sets a configuration parameter for the provider engine.
	Option func(*options) error

	options struct {
		ds datastore.Batching
		h  host.Host

		// announceURLs is the list of indexer URLs to send direct HTTP
		// announce messages to.
		announceURLs []*url.URL

		// key is always initialized from the host peerstore.
		// Setting an explicit identity must not be exposed unless it is tightly coupled with the
		// host identity. Otherwise, the signature of advertisement will not match the libp2p host
		// ID.
		key crypto.PrivKey

		// It is important to not to change this parameter when running against
		// existing datastores. The reason for that is to maintain backward
		// compatibility. Older records from previous library versions aren't
		// indexed by provider ID as there could have been only one provider in
		// the previous versions. Provider host and retrieval addresses can be
		// overidden from the NotifyPut and Notify Remove method, otherwise the
		// default configured provider will be assumed.
		provider peer.AddrInfo

		pubKind PublisherKind
		pubDT   datatransfer.Manager
		// pubHttpAnnounceAddrs are the addresses that are put into announce
		// messages to tell the indexer the addresses where advertisement are
		// published.
		pubHttpAnnounceAddrs []multiaddr.Multiaddr
		pubHttpListenAddr    string
		pubHttpWithoutServer bool
		pubHttpHandlerPath   string
		pubTopicName         string
		pubTopic             *pubsub.Topic
		pubExtraGossipData   []byte

		entCacheCap int
		purgeCache  bool
		chunker     chunker.NewChunkerFunc

		syncPolicy *policy.Policy
	}
)

func newOptions(o ...Option) (*options, error) {
	opts := &options{
		pubKind:           NoPublisher,
		pubHttpListenAddr: "0.0.0.0:3104",
		pubTopicName:      "/indexer/ingest/mainnet",
		// Keep 1024 ad entry DAG in cache; note, the size on disk depends on DAG format and
		// multihash code.
		entCacheCap: 1024,
		// By default use chained Entry Chunk as the format of advertisement entries, with maximum
		// 16384 multihashes per chunk.
		chunker:    chunker.NewChainChunkerFunc(16384),
		purgeCache: false,
	}

	for _, apply := range o {
		if err := apply(opts); err != nil {
			return nil, err
		}
	}

	if opts.syncPolicy == nil {
		var err error
		opts.syncPolicy, err = policy.New(true, nil)
		if err != nil {
			return nil, err
		}
	}

	if opts.ds == nil {
		opts.ds = dssync.MutexWrap(datastore.NewMapDatastore())
	}

	if (opts.key == nil || len(opts.provider.Addrs) == 0 || opts.provider.ID == "") && opts.h == nil {
		// need a host
		h, err := libp2p.New()
		if err != nil {
			return nil, err
		}
		log.Infow("Libp2p host is not configured, but required; created a new host.", "id", h.ID())
		opts.h = h
	}

	if opts.key == nil {
		// Initialize private key from libp2p host
		opts.key = opts.h.Peerstore().PrivKey(opts.h.ID())
	}
	// Defensively check that host's self private key is indeed set.
	if opts.key == nil {
		return nil, fmt.Errorf("cannot find private key in self peerstore; libp2p host is misconfigured")
	}

	if len(opts.provider.Addrs) == 0 {
		opts.provider.Addrs = opts.h.Addrs()
		log.Infow("Retrieval address not configured; using host listen addresses instead.", "retrievalAddrs", opts.provider.Addrs)
	}
	if opts.provider.ID == "" {
		opts.provider.ID = opts.h.ID()
		log.Infow("Retrieval ID not configured; using host ID instead.", "retrievalID", opts.provider.ID)
	}

	return opts, nil
}

func (o *options) retrievalAddrsAsString() []string {
	var ras []string
	for _, ra := range o.provider.Addrs {
		ras = append(ras, ra.String())
	}
	return ras
}

// WithPurgeCacheOnStart sets whether to clear any cached entries chunks when the provider engine
// starts.
// If unset, cache is rehydrated from previously cached entries stored in datastore if present.
// See: WithDatastore.
func WithPurgeCacheOnStart(p bool) Option {
	return func(o *options) error {
		o.purgeCache = p
		return nil
	}
}

// WithChainedEntries sets format of advertisement entries to chained Entry Chunk with the
// given chunkSize as the maximum number of multihashes per chunk.
//
// If unset, advertisement entries are formatted as chained Entry Chunk with default maximum of
// 16384 multihashes per chunk.
//
// To use HAMT as the advertisement entries format, see: WithHamtEntries.
// For caching configuration: WithEntriesCacheCapacity, chunker.CachedEntriesChunker
func WithChainedEntries(chunkSize int) Option {
	return func(o *options) error {
		o.chunker = chunker.NewChainChunkerFunc(chunkSize)
		return nil
	}
}

// WithHamtEntries sets format of advertisement entries to HAMT with the given hash algorithm,
// bit-width and bucket size.
//
// If unset, advertisement entries are formatted as chained Entry Chunk with default maximum of
// 16384 multihashes per chunk.
//
// Only multicodec.Identity, multicodec.Sha2_256 and multicodec.Murmur3X64_64 are supported as hash
// algorithm.
// The bit-width and bucket size must be at least 3 and 1 respectively.
// For more information on HAMT data structure, see:
//   - https://ipld.io/specs/advanced-data-layouts/hamt/spec
//   - https://github.com/ipld/go-ipld-adl-hamt
//
// For caching configuration: WithEntriesCacheCapacity, chunker.CachedEntriesChunker
func WithHamtEntries(hashAlg multicodec.Code, bitWidth, bucketSize int) Option {
	return func(o *options) error {
		o.chunker = chunker.NewHamtChunkerFunc(hashAlg, bitWidth, bucketSize)
		return nil
	}
}

// WithEntriesCacheCapacity sets the maximum number of advertisement entries DAG to cache. The
// cached DAG may be in chained Entry Chunk or HAMT format. See WithChainedEntries and
// WithHamtEntries to select the ad entries DAG format.
//
// If unset, the default capacity of 1024 is used. This means at most 1024 DAGs will be cached.
//
// The cache is evicted using LRU policy. Note that the capacity dictates the number of complete
// chains that are cached, not individual entry chunks. This means, the maximum storage used by the
// cache is a factor of capacity, chunk size and the length of multihashes in each chunk.
//
// As an example, for 128-bit long multihashes the cache with default capacity of 1024, and default
// chunk size of 16384 can grow up to 256MiB when full.
func WithEntriesCacheCapacity(s int) Option {
	return func(o *options) error {
		o.entCacheCap = s
		return nil
	}
}

// WithPublisherKind sets the kind of publisher used to announce new advertisements.
// If unset, advertisements are only stored locally and no announcements are made.
// See: PublisherKind.
func WithPublisherKind(k PublisherKind) Option {
	return func(o *options) error {
		o.pubKind = k
		return nil
	}
}

// WithHttpPublisherListenAddr sets the net listen address for the HTTP publisher.
// If unset, the default net listen address of '0.0.0.0:3104' is used.
//
// Note that this option only takes effect if the PublisherKind is set to HttpPublisher.
// See: WithPublisherKind.
func WithHttpPublisherListenAddr(addr string) Option {
	return func(o *options) error {
		o.pubHttpListenAddr = addr
		return nil
	}
}

// WithHttpPublisherWithoutServer sets the HTTP publisher to not start a server.
// Setting up the handler is left to the user.
func WithHttpPublisherWithoutServer() Option {
	return func(o *options) error {
		o.pubHttpWithoutServer = true
		return nil
	}
}

// WithHttpPublisherHandlerPath should only be used with
// WithHttpPublisherWithoutServer
func WithHttpPublisherHandlerPath(handlerPath string) Option {
	return func(o *options) error {
		o.pubHttpHandlerPath = handlerPath
		return nil
	}
}

// WithHttpPublisherAnnounceAddr sets the address to be supplied in announce
// messages to tell indexers where to retrieve advertisements.
//
// This option only takes effect if the PublisherKind is set to HttpPublisher.
func WithHttpPublisherAnnounceAddr(addr string) Option {
	return func(o *options) error {
		if addr != "" {
			maddr, err := multiaddr.NewMultiaddr(addr)
			if err != nil {
				return err
			}
			o.pubHttpAnnounceAddrs = append(o.pubHttpAnnounceAddrs, maddr)
		}
		return nil
	}
}

// WithTopicName sets toe topic name on which pubsub announcements are published.
// To override the default pubsub configuration, use WithTopic.
//
// Note that this option only takes effect if the PublisherKind is set to DataTransferPublisher.
// See: WithPublisherKind.
func WithTopicName(t string) Option {
	return func(o *options) error {
		o.pubTopicName = t
		return nil
	}
}

// WithTopic sets the pubsub topic on which new advertisements are announced.
// To use the default pubsub configuration with a specific topic name, use WithTopicName. If both
// options are specified, WithTopic takes presence.
//
// Note that this option only takes effect if the PublisherKind is set to DataTransferPublisher.
// See: WithPublisherKind.
func WithTopic(t *pubsub.Topic) Option {
	return func(o *options) error {
		o.pubTopic = t
		return nil
	}
}

// WithDataTransfer sets the instance of datatransfer.Manager to use.
// If unspecified a new instance is created automatically.
//
// Note that this option only takes effect if the PublisherKind is set to DataTransferPublisher.
// See: WithPublisherKind.
func WithDataTransfer(dt datatransfer.Manager) Option {
	return func(o *options) error {
		o.pubDT = dt
		return nil
	}
}

// WithHost specifies the host to which the provider engine belongs.
// If unspecified, a host is created automatically.
// See: libp2p.New.
func WithHost(h host.Host) Option {
	return func(o *options) error {
		o.h = h
		return nil
	}
}

// WithDatastore sets the datastore that is used by the engine to store advertisements.
// If unspecified, an ephemeral in-memory datastore is used.
// See: datastore.NewMapDatastore.
func WithDatastore(ds datastore.Batching) Option {
	return func(o *options) error {
		o.ds = ds
		return nil
	}
}

// WithRetrievalAddrs sets the addresses that specify where to get the content corresponding to an
// indexing advertisement.
// If unspecified, the libp2p host listen addresses are used.
// See: WithHost.
func WithRetrievalAddrs(addrs ...string) Option {
	return func(o *options) error {
		if len(addrs) != 0 {
			maddrs := make([]multiaddr.Multiaddr, len(addrs))
			for i, a := range addrs {
				var err error
				maddrs[i], err = multiaddr.NewMultiaddr(a)
				if err != nil {
					return fmt.Errorf("bad multiaddr %q: %w", a, err)
				}
			}
			o.provider.Addrs = maddrs
		}
		return nil
	}
}

func WithSyncPolicy(syncPolicy *policy.Policy) Option {
	return func(o *options) error {
		o.syncPolicy = syncPolicy
		return nil
	}
}

// WithProvider sets the peer and addresses for the provider to put in indexing advertisements.
// This value overrides `WithRetrievalAddrs`
func WithProvider(provider peer.AddrInfo) Option {
	return func(o *options) error {
		o.provider = provider
		return nil
	}
}

// WithExtraGossipData supplies extra data to include in the pubsub announcement.
// Note that this option only takes effect if the PublisherKind is set to DataTransferPublisher.
// See: WithPublisherKind.
func WithExtraGossipData(extraData []byte) Option {
	return func(o *options) error {
		if len(extraData) != 0 {
			// Make copy for safety.
			o.pubExtraGossipData = make([]byte, len(extraData))
			copy(o.pubExtraGossipData, extraData)
		}
		return nil
	}
}

func WithPrivateKey(key crypto.PrivKey) Option {
	return func(o *options) error {
		o.key = key
		return nil
	}
}

// WithDirectAnnounce sets indexer URLs to send direct HTTP announcements to.
func WithDirectAnnounce(announceURLs ...string) Option {
	return func(o *options) error {
		for _, urlStr := range announceURLs {
			u, err := url.Parse(urlStr)
			if err != nil {
				return err
			}
			o.announceURLs = append(o.announceURLs, u)
		}
		return nil
	}
}
