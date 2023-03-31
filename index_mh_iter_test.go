package provider

import (
	"errors"
	"io"
	"testing"

	"github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/index"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

func TestIndexMhIterator_NextReturnsMhThenEOFOnHappyPath(t *testing.T) {
	wantMh, err := multihash.Sum([]byte("fish"), multihash.SHA3_256, -1)
	require.NoError(t, err)

	subject, err := CarMultihashIterator(&testIterableIndex{
		doForEach: func(f func(multihash.Multihash, uint64) error) error {
			err := f(wantMh, 1)
			require.NoError(t, err)
			err = f(wantMh, 2)
			require.NoError(t, err)
			return nil
		},
	})
	require.NoError(t, err)

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

func TestIndexMhIterator_FailsFastWhenForEachReturnsError(t *testing.T) {
	wantErr := errors.New("lobster")
	subject, err := CarMultihashIterator(&testIterableIndex{
		doForEach: func(f func(multihash.Multihash, uint64) error) error {
			return wantErr
		},
	})
	require.Equal(t, wantErr, err)
	require.Nil(t, subject)
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

	subject, err := CarMultihashIterator(iterIdx)
	require.NoError(t, err)

	var gotMhs []multihash.Multihash
	for {
		gotNext, err := subject.Next()
		if errors.Is(err, io.EOF) {
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
