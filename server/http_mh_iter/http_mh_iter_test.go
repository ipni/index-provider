package http_mh_iter

import (
	"context"
	"errors"
	"fmt"
	"github.com/filecoin-project/index-provider/server/utils"
	"github.com/filecoin-project/index-provider/testutil"
	"github.com/stretchr/testify/require"
	"io"
	"strings"
	"testing"
	"time"

	"math/rand"
)

const (
	ReqID      = "TestReqID"
	ErrToken   = "TestErrToken"
	Token      = "TestToken"
	ContextID  = "TestContextID"
	ListenAddr = "0.0.0.0:3107"
	PageSize   = 101
)

func TestMhIter(t *testing.T) {
	svr, err := NewServer(
		WithTokenVerify(func(reqID string, token string) error {
			if reqID == ReqID && token == Token {
				return nil
			}
			return fmt.Errorf("unauthurized")
		}),
		WithListenAddr(ListenAddr),
	)

	require.NoError(t, err)

	mhsCount := 1020
	mhs := testutil.RandomMultihashes(t, rand.New(rand.NewSource(334455)), mhsCount)
	svr.AddMultiHashes([]byte(ContextID), mhs)

	var svrStartErrChan = make(chan error, 1)
	go func() {
		svrStartErrChan <- svr.Start()
	}()

	opts := &HttpMhIterOption{
		ReqID:    ReqID,
		Url:      "http://" + ListenAddr,
		Token:    ErrToken,
		PageSize: PageSize,
	}
	time.Sleep(time.Millisecond * 100)

	t.Run("TestUnauthorized", func(t *testing.T) {
		httpMhIterWithErrToken := NewHttpMhIterator(opts, []byte(ContextID), uint64(mhsCount))
		err := httpMhIterWithErrToken.nextPage()
		require.Equal(t, errors.Is(err, utils.ErrUnauthorized), true)
	})

	opts.Token = Token
	httpMhIter := NewHttpMhIterator(opts, []byte(ContextID), uint64(mhsCount))

	t.Run("TestMultiHashIterator", func(t *testing.T) {
		for i := 0; i < mhsCount; i++ {
			mh, err := httpMhIter.Next()
			require.NoError(t, err)
			require.Equal(t, mhs[i], mh)
		}
	})

	t.Run("TestNextEOF", func(t *testing.T) {
		_, err := httpMhIter.Next()
		require.Equal(t, errors.Is(err, io.EOF), true)
	})

	t.Run("TestNextPageEOF", func(t *testing.T) {
		require.Equal(t, errors.Is(httpMhIter.nextPage(), io.EOF), true)
	})

	t.Run("TestRemove", func(t *testing.T) {
		require.Equal(t, svr.DelMultiHashes([]byte("ContextIDNotExists")), false)
		require.Equal(t, svr.DelMultiHashes([]byte(ContextID)), true)
		_, err := NewHttpMhIterator(opts, []byte(ContextID), uint64(mhsCount)).Next()
		require.Equal(t, strings.Contains(err.Error(), "not found"), true)

	})
	svrStartErrChan <- nil

	require.NoError(t, svr.Shutdown(context.TODO()))
	require.NoError(t, <-svrStartErrChan)
}
