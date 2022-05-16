package http_mh_iter

import (
	"time"

	"github.com/filecoin-project/index-provider/server/utils"
)

type (
	FnVerifyToken func(string, string) error

	Option func(*options) error

	options struct {
		utils.Options
		verifyToken FnVerifyToken
	}
)

func newOptions(o ...Option) (*options, error) {
	opts := &options{
		Options: utils.Options{
			ListenAddr:   "0.0.0.0:3105",
			ReadTimeout:  30 * time.Second,
			WriteTimeout: 30 * time.Second,
		},
	}

	for _, apply := range o {
		if err := apply(opts); err != nil {
			return nil, err
		}
	}
	return opts, nil
}

func WithTokenVerify(v FnVerifyToken) Option {
	return func(o *options) error {
		o.verifyToken = v
		return nil
	}
}

func WithListenAddr(addr string) Option {
	return func(o *options) error {
		o.ListenAddr = addr
		return nil
	}
}

func WithReadTimeout(t time.Duration) Option {
	return func(o *options) error {
		o.ReadTimeout = t
		return nil
	}
}

func WithWriteTimeout(t time.Duration) Option {
	return func(o *options) error {
		o.WriteTimeout = t
		return nil
	}
}
