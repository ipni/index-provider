package delegatedrouting_test

import (
	"testing"

	drouting "github.com/ipni/index-provider/delegatedrouting"
	"github.com/stretchr/testify/require"
)

func TestOptionsDefaults(t *testing.T) {
	opts := drouting.ApplyOptions()
	require.Equal(t, 1_000_000, opts.SnapshotMaxChunkSize)
}
