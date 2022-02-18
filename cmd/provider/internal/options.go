package internal

import "github.com/ipld/go-ipld-prime/traversal/selector"

type (
	Option func(*options) error

	options struct {
		entriesRecurLimit selector.RecursionLimit
		topic             string
	}
)

func newOptions(o ...Option) (*options, error) {
	opts := &options{
		entriesRecurLimit: selector.RecursionLimitNone(),
		topic:             "/indexer/ingest/mainnet",
	}
	for _, apply := range o {
		if err := apply(opts); err != nil {
			return nil, err
		}
	}
	return opts, nil
}

func WithTopic(topic string) Option {
	return func(o *options) error {
		o.topic = topic
		return nil
	}
}

func WithEntriesRecursionLimit(limit selector.RecursionLimit) Option {
	return func(o *options) error {
		o.entriesRecurLimit = limit
		return nil
	}
}
