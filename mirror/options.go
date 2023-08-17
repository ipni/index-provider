package mirror

import (
	"crypto/rand"
	"errors"
	"fmt"
	"time"

	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	hamt "github.com/ipld/go-ipld-adl-hamt"
	"github.com/ipld/go-ipld-prime/schema"
	stischema "github.com/ipni/go-libipni/ingest/schema"
	"github.com/ipni/index-provider/engine/chunker"
	"github.com/libp2p/go-libp2p"
	p2pcrypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multicodec"
)

type (
	Option  func(*options) error
	options struct {
		h                           host.Host
		ds                          datastore.Batching
		syncInterval                time.Duration
		httpListenAddr              string
		initAdRecurLimit            int64
		entriesRecurLimit           int64
		chunkerFunc                 chunker.NewChunkerFunc
		chunkCacheCap               int
		chunkCachePurge             bool
		privKey                     p2pcrypto.PrivKey
		topic                       string
		skipRemapOnEntriesTypeMatch bool
		entriesRemapPrototype       schema.TypedPrototype
		alwaysReSignAds             bool
	}
)

// TODO: add options to restructure advertisements.
//       nft.storage advertisement chain is a good usecase, where remapping entries to say HAMT
//       probably won't make much difference. But combining ads to make a shorter chain will most
//       likely improve end-to-end ingestion latency.

func newOptions(o ...Option) (*options, error) {
	opts := options{
		chunkCacheCap:   1024,
		chunkCachePurge: false,
		topic:           "/indexer/ingest/mainnet",
		syncInterval:    10 * time.Minute,
	}
	for _, apply := range o {
		if err := apply(&opts); err != nil {
			return nil, err
		}
	}
	if opts.h == nil {
		var err error
		if opts.privKey == nil {
			opts.privKey, _, err = p2pcrypto.GenerateEd25519Key(rand.Reader)
			if err != nil {
				return nil, err
			}
		}
		if opts.h, err = libp2p.New(libp2p.Identity(opts.privKey)); err != nil {
			return nil, err
		}
	} else {
		peerIDFromPrivKey, err := peer.IDFromPrivateKey(opts.privKey)
		if err != nil {
			return nil, fmt.Errorf("could not get peer ID from private key: %w", err)
		}
		if opts.h.ID() != peerIDFromPrivKey {
			return nil, errors.New("host ID does not match ID from private key")
		}
	}
	if opts.ds == nil {
		opts.ds = dssync.MutexWrap(datastore.NewMapDatastore())
	}
	// TODO: Do not set this when libp2phttp available.
	if opts.httpListenAddr == "" {
		opts.httpListenAddr = "127.0.0.1:0"
	}
	return &opts, nil
}

func (o *options) remapEntriesEnabled() bool {
	// Use whether the chunker func is set or not as a flag to decide if entries should be remapped.
	return o.chunkerFunc != nil
}

// WithDatastore specifies the datastore used by the mirror to persist mirrored
// advertisements, their entries and other internal data. Defaults to an
// ephemeral in-memory datastore.
func WithDatastore(ds datastore.Batching) Option {
	return func(o *options) error {
		o.ds = ds
		return nil
	}
}

// WithHost specifies the libp2p host the mirror should be exposed on.
// If unspecified a host with default options and random identity is used.
func WithHost(h host.Host, privKey p2pcrypto.PrivKey) Option {
	return func(o *options) error {
		if h != nil && privKey == nil {
			return errors.New("if host is specified then private key must be specified")
		}
		o.h = h
		o.privKey = privKey
		return nil
	}
}

// WithEntryChunkRemapper remaps the entries from the original provider into schema.EntryChunkPrototype
// structure with the given chunk size.
// If unset, the original structure is mirrored without change.
//
// See: WithSkipRemapOnEntriesTypeMatch, WithHamtRemapper.
func WithEntryChunkRemapper(chunkSize int) Option {
	return func(o *options) error {
		o.entriesRemapPrototype = stischema.EntryChunkPrototype
		o.chunkerFunc = chunker.NewChainChunkerFunc(chunkSize)
		return nil
	}
}

// WithHamtRemapper remaps the entries from the original provider into hamt.HashMapRootPrototype
// structure with the given bit-width and bucket size.
// If unset, the original structure is mirrored without change.
//
// See: WithSkipRemapOnEntriesTypeMatch, WithEntryChunkRemapper.
func WithHamtRemapper(hashAlg multicodec.Code, bitwidth, bucketSize int) Option {
	return func(o *options) error {
		o.entriesRemapPrototype = hamt.HashMapRootPrototype
		o.chunkerFunc = chunker.NewHamtChunkerFunc(hashAlg, bitwidth, bucketSize)
		return nil
	}
}

// WithHTTPListenAddr sets the HTTP address:port for the http publisher to listen on.
func WithHTTPListenAddr(addr string) Option {
	return func(o *options) error {
		o.httpListenAddr = addr
		return nil
	}
}

// WithSkipRemapOnEntriesTypeMatch specifies weather to skip remapping entries if the original
// structure prototype matches the configured remap option.
// Note that setting this option without setting a remap option has no effect.
//
// See: WithEntryChunkRemapper, WithHamtRemapper.
func WithSkipRemapOnEntriesTypeMatch(s bool) Option {
	return func(o *options) error {
		o.skipRemapOnEntriesTypeMatch = s
		return nil
	}
}

// WithSyncInterval specifies the time interval at which the original provider is checked for new
// advertisements.
// If unset, the default time interval of 10 minutes is used.
func WithSyncInterval(interval time.Duration) Option {
	return func(o *options) error {
		o.syncInterval = interval
		return nil
	}
}

// WithInitialAdRecursionLimit specifies the recursion limit for the initial
// sync if no previous advertisements are mirrored by the mirror.
//
// There is no recursion limit if unset.
func WithInitialAdRecursionLimit(limit int64) Option {
	return func(o *options) error {
		o.initAdRecurLimit = limit
		return nil
	}
}

// WithEntriesRecursionLimit specifies the recursion limit for syncing the
// advertisement entries.
//
// There is no recursion limit if unset.
func WithEntriesRecursionLimit(limit int64) Option {
	return func(o *options) error {
		o.entriesRecurLimit = limit
		return nil
	}
}

// WithRemappedEntriesCacheCapacity sets the LRU cache capacity used to store the remapped
// advertisement entries. The capacity refers to the number of complete entries DAGs cached. The
// actual storage occupied by the cache depends on the shape of the DAGs.
// See: chunker.CachedEntriesChunker.
//
// This option has no effect if no entries remapper option is set.
// Defaults to 1024.
func WithRemappedEntriesCacheCapacity(c int) Option {
	return func(o *options) error {
		o.chunkCacheCap = c
		return nil
	}
}

// WithPurgeCachedEntries specifies whether to delete any cached entries on start-up.
// This option has no effect if no entries remapper option is set.
func WithPurgeCachedEntries(b bool) Option {
	return func(o *options) error {
		o.chunkCachePurge = b
		return nil
	}
}

// WithTopicName specifies the topi name on which the mirrored advertisements are announced.
func WithTopicName(t string) Option {
	return func(o *options) error {
		o.topic = t
		return nil
	}
}

// WithAlwaysReSignAds specifies whether every mirrored ad should be resigned by the mirror identity
// regardless of weather the advertisement content is changed as a result of mirroring or not.
// By default, advertisements are only re-signed if: 1) the link to previous advertisement is not
// changed, and 2) link to entries is not changed.
func WithAlwaysReSignAds(r bool) Option {
	return func(o *options) error {
		o.alwaysReSignAds = r
		return nil
	}
}
