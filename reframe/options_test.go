package reframe_test

import (
	"testing"

	reframeListener "github.com/filecoin-project/index-provider/reframe"
	"github.com/stretchr/testify/require"
)

func TestOptionsDefaults(t *testing.T) {
	opts := reframeListener.ApplyOptions()
	require.Equal(t, 1_000_000, opts.SnapshotMaxChunkSize)
}
