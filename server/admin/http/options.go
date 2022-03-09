package adminserver

import "time"

type (
	// Option captures a configurable parameter in admin HTTP server.
	Option func(*options) error

	options struct {
		listenAddr   string
		readTimeout  time.Duration
		writeTimeout time.Duration
	}
)

func newOptions(o ...Option) (*options, error) {
	opts := &options{
		listenAddr:   "0.0.0.0:3102",
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

// WithListenAddr sets the net address on which the admin HTTP server is exposed.
// If uset, the default address of '0.0.0.0:3102' is used.
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

// WithWriteTimeout set s the HTTP write timeout.
// If unset, the default of 30 seconds is used.
func WithWriteTimeout(t time.Duration) Option {
	return func(o *options) error {
		o.writeTimeout = t
		return nil
	}
}
