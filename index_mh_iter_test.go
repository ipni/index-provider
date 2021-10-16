package provider

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/index"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

func TestIndexMhIterator_NextReturnsMhThenEOFOnHappyPath(t *testing.T) {
	wantMh, err := multihash.Sum([]byte("fish"), multihash.SHA3_256, -1)
	require.NoError(t, err)

	subject := CarMultihashIterator(context.Background(), &testIterableIndex{
		doForEach: func(f func(multihash.Multihash, uint64) error) error {
			err := f(wantMh, 1)
			require.NoError(t, err)
			err = f(wantMh, 2)
			require.NoError(t, err)
			return nil
		},
	})

	gotMh, err := subject.Next()
	require.Equal(t, wantMh, gotMh)
	require.NoError(t, err)

	gotMh, err = subject.Next()
	require.Equal(t, wantMh, gotMh)
	require.NoError(t, err)

	gotMh, err = subject.Next()
	require.Nil(t, gotMh)
	require.Equal(t, io.EOF, err)
}

func TestIndexMhIterator_NextReturnsErrorOnUnHappyPath(t *testing.T) {
	wantMh, err := multihash.Sum([]byte("fish"), multihash.SHA3_256, -1)
	require.NoError(t, err)
	wantErr := errors.New("lobster")

	subject := CarMultihashIterator(context.Background(), &testIterableIndex{
		doForEach: func(f func(multihash.Multihash, uint64) error) error {
			err := f(wantMh, 1)
			require.NoError(t, err)
			return wantErr
		},
	})

	gotMh, err := subject.Next()
	require.NotNil(t, gotMh)
	require.Nil(t, err)

	gotMh, err = subject.Next()
	require.Nil(t, gotMh)
	require.Equal(t, wantErr, err)
}

func TestNewIndexMhIterator_TimesOutWhenContextTimesOut(t *testing.T) {
	t.Skipf("TODO(mvdan): context currently unused")
	timedoutCtx, cancelFunc := context.WithTimeout(context.Background(), time.Nanosecond)
	t.Cleanup(cancelFunc)

	idx, err := car.GenerateIndexFromFile("testdata/sample-v1.car")
	require.NoError(t, err)
	iterIdx, ok := idx.(index.IterableIndex)
	require.True(t, ok)

	subject := CarMultihashIterator(timedoutCtx, iterIdx)

	// Assert that eventually deadline exceeded error is returned.
	// Note, we have to assert eventually, since we can't guarantee whether mh gets added to channel
	// first or ctx.Done() is selected first.
	for {
		_, err = subject.Next()
		if err != nil {
			break
		}
	}
	require.EqualError(t, err, "context deadline exceeded")
}

func TestNewCarSupplier_ReturnsExpectedMultihashes(t *testing.T) {
	idx, err := car.GenerateIndexFromFile("testdata/sample-v1.car")
	require.NoError(t, err)
	iterIdx, ok := idx.(index.IterableIndex)
	require.True(t, ok)

	var wantMhs []multihash.Multihash
	err = iterIdx.ForEach(func(m multihash.Multihash, _ uint64) error {
		wantMhs = append(wantMhs, m)
		return nil
	})
	require.NoError(t, err)

	ctx, cancelFunc := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancelFunc)
	subject := CarMultihashIterator(ctx, iterIdx)

	var gotMhs []multihash.Multihash
	for {
		gotNext, err := subject.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		gotMhs = append(gotMhs, gotNext)
	}

	// In different order; just check the lengths.
	// CarMultihashIterator.Next already checks the order is followed.
	require.Equal(t, len(wantMhs), len(gotMhs))
}

var _ index.IterableIndex = (*testIterableIndex)(nil)

type testIterableIndex struct {
	index.MultihashIndexSorted
	doForEach func(f func(multihash.Multihash, uint64) error) error
}

func (t *testIterableIndex) ForEach(f func(multihash.Multihash, uint64) error) error {
	return t.doForEach(f)
}
