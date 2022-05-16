package http_adv

import (
	"context"
	"fmt"
	"github.com/filecoin-project/index-provider/engine"
	"github.com/filecoin-project/index-provider/metadata"
	"github.com/filecoin-project/index-provider/server/http_mh_iter"
	"github.com/filecoin-project/index-provider/server/utils"
	"github.com/filecoin-project/index-provider/testutil"
	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/require"
	"math/rand"
	"strings"
	"testing"
	"time"
)

const (
	ReqID            = "ReqID"
	AdvToken         = "AdvToken"
	MhToken          = "MhToken"
	ContextID        = "ContextID"
	AdvSvrListenAddr = "0.0.0.0:3106"
	MhSvrListenAddr  = "0.0.0.0:3105"
)

func newMultihashIterSvr(t *testing.T) *http_mh_iter.Server {
	svr, err := http_mh_iter.NewServer(
		http_mh_iter.WithTokenVerify(func(reqID string, token string) error {
			if reqID == ReqID && token == MhToken {
				return nil
			}
			return fmt.Errorf("unauthurized")
		}),
		http_mh_iter.WithListenAddr(MhSvrListenAddr),
	)

	require.NoError(t, err)
	return svr
}

func newAdvSvr(t *testing.T) *Server {
	e, err := engine.New(engine.WithPublisherKind(engine.NoPublisher))
	require.NoError(t, err)
	require.NoError(t, e.Start(context.TODO()))

	advSvr, err := NewServer(e, WithListenAddr(AdvSvrListenAddr),
		WithTokenVerify(func(token string) error {
			if token != AdvToken {
				return utils.ErrUnauthorized
			}
			return nil
		}),
	)

	e.RegisterMultihashLister(advSvr.ListMultihashes)

	require.NoError(t, err)
	return advSvr
}

func TestHttpAdv(t *testing.T) {
	mhSvr := newMultihashIterSvr(t)
	advSvr := newAdvSvr(t)

	mhsCount := 1020
	mhs := testutil.RandomMultihashes(t, rand.New(rand.NewSource(334455)), mhsCount)
	mhSvr.AddMultiHashes([]byte(ContextID), mhs)

	svrErrCh := make(chan error, 2)

	go func() {
		svrErrCh <- advSvr.Start()
	}()
	go func() {
		svrErrCh <- mhSvr.Start()
	}()

	<-time.After(time.Millisecond * 100)

	ctx := context.TODO()

	advClient, err := NewHttpAdvClient(
		"http://"+AdvSvrListenAddr, AdvToken,
		"http://"+MhSvrListenAddr, MhToken)

	require.NoError(t, err)

	t.Run("TestAnnounceAdvertisementOk", func(t *testing.T) {
		advCid, err := advClient.AnnounceAdv(ctx, []byte(ContextID), ReqID, uint64(mhsCount), metadata.Metadata{}, false)
		require.NoError(t, err)
		require.NotEqual(t, cid.Undef, advCid)
		t.Logf("announce advertisement successfully, cid:%s", advCid.String())
	})

	t.Run("TestAnnounceAdvCtxNotFound", func(t *testing.T) {
		_, err := advClient.AnnounceAdv(ctx, []byte("ContextIDNotFound"), ReqID, uint64(mhsCount), metadata.Metadata{}, false)
		require.Equal(t, strings.Contains(err.Error(), "not found"), true)
	})

	svrErrCh <- nil

	require.NoError(t, advSvr.Shutdown(ctx))
	require.NoError(t, mhSvr.Shutdown(ctx))

	select {
	case e := <-svrErrCh:
		require.NoError(t, e)
	}

}
