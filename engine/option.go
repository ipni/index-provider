package engine

import "fmt"

// engineConfig contains all options for engineConfiguring Engine.
type engineConfig struct {
	extraGossipData []byte
}

type Option func(*engineConfig) error

// apply applies the given options to this engineConfig.
func (c *engineConfig) apply(opts []Option) error {
	for i, opt := range opts {
		if err := opt(c); err != nil {
			return fmt.Errorf("option %d failed: %s", i, err)
		}
	}
	return nil
}

// WithExtraGossipData supplies extra data to include in the pubsub announcement.
func WithExtraGossipData(extraData []byte) Option {
	return func(c *engineConfig) error {
		if len(extraData) != 0 {
			// Make copy for safety.
			c.extraGossipData = make([]byte, len(extraData))
			copy(c.extraGossipData, extraData)
		}
		return nil
	}
}
