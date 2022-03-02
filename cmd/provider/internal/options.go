package internal

import (
	"time"

	"github.com/ipld/go-ipld-prime/traversal/selector"
)

type (
	Option func(*options) error

	options struct {
		entriesRecurLimit selector.RecursionLimit
		topic             string
		maxSyncRetry      uint64
		syncRetryBackoff  time.Duration
	}
)

func newOptions(o ...Option) (*options, error) {
	opts := &options{
		entriesRecurLimit: selector.RecursionLimitNone(),
		topic:             "/indexer/ingest/mainnet",
		maxSyncRetry:      10,
		syncRetryBackoff:  500 * time.Millisecond,
	}
	for _, apply := range o {
		if err := apply(opts); err != nil {
			return nil, err
		}
	}
	return opts, nil
}

// WithSyncRetryBackoff sets the length of time to wait before retrying a faild sync.
// Defaults to 500ms if unset.
func WithSyncRetryBackoff(d time.Duration) Option {
	return func(o *options) error {
		o.syncRetryBackoff = d
		return nil
	}
}

// WithMaxSyncRetry sets the maximum number of times to retry a failed sync.
// Defaults to 10 if unset.
func WithMaxSyncRetry(r uint64) Option {
	return func(o *options) error {
		o.maxSyncRetry = r
		return nil
	}
}

// WithTopicName sets the topic name on which the provider announces advertised content.
// Defaults to '/indexer/ingest/mainnet'.
func WithTopicName(topic string) Option {
	return func(o *options) error {
		o.topic = topic
		return nil
	}
}

// WithEntriesRecursionLimit sets the recursion limit when syncing advertisement entries chain.
// Defaults to no limit.
func WithEntriesRecursionLimit(limit selector.RecursionLimit) Option {
	return func(o *options) error {
		o.entriesRecurLimit = limit
		return nil
	}
}
