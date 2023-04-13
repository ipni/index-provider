package delegatedrouting

import "time"

const (
	defaultSnapshotMaxChunkSize = 1_000_000
	defaultPageSize             = 20_000
	defaultFlushFrequency       = 10 * time.Minute
)

type Options struct {
	// SnapshotMaxChunkSize defines a size of a chunk that CID snapshot is
	// going to be split into before stored in the datastore. Needed as leveldb
	// can't handle binary payloads above a certain threshold
	SnapshotMaxChunkSize int
	// PageSize defines a maximum number of results that can be returned by a
	// query during datastore initialisation
	PageSize int
	// AdFlushFrequency defines a frequency of a flush operation that is going
	// to be performed on the current chunk. In other words a non empty current
	// chunk will be converted to an ad and published.
	AdFlushFrequency time.Duration
}

type Option func(*Options)

func WithSnapshotMaxChunkSize(i int) Option {
	return func(o *Options) {
		o.SnapshotMaxChunkSize = i
	}
}

func WithPageSize(i int) Option {
	return func(o *Options) {
		o.PageSize = i
	}
}

func WithAdFlushFrequency(d time.Duration) Option {
	return func(o *Options) {
		o.AdFlushFrequency = d
	}
}

func ApplyOptions(opt ...Option) Options {
	opts := Options{
		SnapshotMaxChunkSize: defaultSnapshotMaxChunkSize,
		PageSize:             defaultPageSize,
		AdFlushFrequency:     defaultFlushFrequency,
	}
	for _, o := range opt {
		o(&opts)
	}
	return opts
}
