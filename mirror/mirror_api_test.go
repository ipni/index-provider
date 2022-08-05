package mirror

import (
	"github.com/ipld/go-ipld-prime/schema"
)

// GetTopicName is exposed for testing purposes only.
func (m *Mirror) GetTopicName() string {
	return m.topic
}

// RemapEntriesEnabled is exposed for testing purposes only.
func (m *Mirror) RemapEntriesEnabled() bool {
	return m.remapEntriesEnabled()

}

// EntriesRemapPrototype is exposed for testing purposes only.
func (m *Mirror) EntriesRemapPrototype() schema.TypedPrototype {
	return m.entriesRemapPrototype
}

// AlwaysReSignAds is exposed for testing purposes only.
func (m *Mirror) AlwaysReSignAds() bool {
	return m.alwaysReSignAds
}
