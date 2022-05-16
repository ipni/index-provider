package http_adv

import (
	"fmt"
	"time"

	"github.com/filecoin-project/index-provider/server/utils"
)

type (
	FnVerifyToken func(string) error

	Option func(*options) error

	options struct {
		utils.Options
		httpMultiHashPageSize uint64
		VerifyToken           FnVerifyToken
	}
)

func newOptions(o ...Option) (*options, error) {
	opts := &options{
		Options: utils.Options{
			ListenAddr:   "0.0.0.0:3106",
			ReadTimeout:  30 * time.Second,
			WriteTimeout: 30 * time.Second,
		},
		httpMultiHashPageSize: 1024,
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
		o.VerifyToken = v
		return nil
	}
}

func WithMultiHashIterPageSize(l uint64) Option {
	return func(o *options) error {
		if l == 0 {
			return fmt.Errorf("page size must be lager than 0")
		}
		o.httpMultiHashPageSize = l
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
