package stores_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/indexer-reference-provider/internal/cardatatransfer/stores"
	"github.com/filecoin-project/indexer-reference-provider/testutil"
)

func TestReadOnlyStoreTracker(t *testing.T) {
	ctx := context.Background()

	rdOnlyBS1 := testutil.OpenSampleCar(t, "sample-v1.car")
	rdOnlyBS2 := testutil.OpenSampleCar(t, "sample-wrapped-v2-2.car")

	len1 := testutil.GetBstoreLen(ctx, t, rdOnlyBS1)

	k1 := "k1"
	k2 := "k2"
	tracker := stores.NewReadOnlyBlockstores()

	// Get a non-existent key
	_, err := tracker.Get(k1)
	require.True(t, stores.IsNotFound(err))

	// Add a read-only blockstore
	ok, err := tracker.Track(k1, rdOnlyBS1)
	require.NoError(t, err)
	require.True(t, ok)

	// Get the blockstore using its key
	got, err := tracker.Get(k1)
	require.NoError(t, err)

	// Verify the blockstore is the same
	lenGot := testutil.GetBstoreLen(ctx, t, got)
	require.Equal(t, len1, lenGot)

	// Call GetOrOpen with a different CAR file
	ok, err = tracker.Track(k2, rdOnlyBS2)
	require.NoError(t, err)
	require.True(t, ok)

	// Verify the blockstore is different
	len2 := testutil.GetBstoreLen(ctx, t, rdOnlyBS2)
	require.NotEqual(t, len1, len2)

	// Untrack the second blockstore from the tracker
	err = tracker.Untrack(k2)
	require.NoError(t, err)

	// Verify it's been removed
	_, err = tracker.Get(k2)
	require.True(t, stores.IsNotFound(err))
}