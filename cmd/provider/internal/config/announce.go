package config

import (
	"fmt"
	"net/url"
)

// DirectAnnounce configures the target indexers that advertisement announce
// messages are sent directly to via HTTP.
type DirectAnnounce struct {
	// NoPubsubAnnounce disables pubsub announce when set to true. The default
	// behavior (false) is to enable sending advertisement announcements via
	// gossib pubsub.
	NoPubsubAnnounce bool
	// URLs is a list of indexer URLs to send HTTP announce messages to.
	URLs []string
}

// NewDirectAnnounce returns DirectAnnounce with values set to their defaults.
func NewDirectAnnounce() DirectAnnounce {
	return DirectAnnounce{}
}

// ParseURLs returns parsed URLs.
func (d DirectAnnounce) ParseURLs() ([]*url.URL, error) {
	if len(d.URLs) == 0 {
		return nil, nil
	}

	urls := make([]*url.URL, len(d.URLs))
	var err error
	for i, u := range d.URLs {
		urls[i], err = url.Parse(u)
		if err != nil {
			return nil, fmt.Errorf("bad DirectAnnounce url %q: %w", u, err)
		}
	}

	return urls, nil
}
