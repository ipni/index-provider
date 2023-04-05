package server

import "time"

type (
	// Option captures a configurable parameter in admin HTTP server.
	Option func(*options) error

	options struct {
		listenAddr       string
		readTimeout      time.Duration
		writeTimeout     time.Duration
		adFlushFrequency time.Duration
	}
)

func newOptions(o ...Option) (*options, error) {
	opts := &options{
		listenAddr:   "",
		readTimeout:  30 * time.Second,
		writeTimeout: 30 * time.Second,
	}

	for _, apply := range o {
		if err := apply(opts); err != nil {
			return nil, err
		}
	}
	return opts, nil
}

// WithListenAddr sets the net address on which the delegated routing HTTP server is exposed.
func WithListenAddr(addr string) Option {
	return func(o *options) error {
		o.listenAddr = addr
		return nil
	}
}

// WithReadTimeout set s the HTTP read timeout.
// If unset, the default of 30 seconds is used.
func WithReadTimeout(t time.Duration) Option {
	return func(o *options) error {
		o.readTimeout = t
		return nil
	}
}

// WithWriteTimeout sets the HTTP write timeout.
// If unset, the default of 30 seconds is used.
func WithWriteTimeout(t time.Duration) Option {
	return func(o *options) error {
		o.writeTimeout = t
		return nil
	}
}

// WithAdFlushFrequency sets the frequency at which the current chunk is publihsed even if it's not full
func WithAdFlushFrequency(t time.Duration) Option {
	return func(o *options) error {
		o.adFlushFrequency = t
		return nil
	}
}
