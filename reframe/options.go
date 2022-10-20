package reframe

const (
	defaultSnapshotMaxChunkSize = 1_000_000
)

type Options struct {
	// SnapshotMaxChunkSize defines a size of a chunk that CID snapshot is going to be split into before stored in the datastore.
	// Needed as leveldb can't handle binary payloads above a certain threshold
	SnapshotMaxChunkSize int
}

type Option func(*Options)

func WithSnapshotMaxChunkSize(i int) Option {
	return func(o *Options) {
		o.SnapshotMaxChunkSize = i
	}
}

func ApplyOptions(opt ...Option) Options {
	opts := Options{
		SnapshotMaxChunkSize: defaultSnapshotMaxChunkSize,
	}
	for _, o := range opt {
		o(&opts)
	}
	return opts
}
