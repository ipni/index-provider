package models

import (
	"testing"

	"github.com/filecoin-project/indexer-reference-provider/internal/utils"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/ipld/go-ipld-prime"
	"github.com/stretchr/testify/require"
)

func TestMarshal(t *testing.T) {
	// Generate some CIDs and populate indexer
	cids, _ := utils.RandomCids(2)
	ds := dssync.MutexWrap(datastore.NewMapDatastore())

	// Masrhal request and check e2e
	t.Log("e2e marshalling request")
	req := &AdRequest{ID: cids[0]}
	b, err := MarshalReq(req)
	require.NoError(t, err)

	r, err := UnmarshalReq(b)
	require.NoError(t, err)
	require.Equal(t, req, r)

	// Masrhal response and check e2e
	t.Log("e2e marshalling response")
	lsys := utils.MkLinkSystem(ds)
	adv, _ := utils.GenRandomIndexAndAdv(t, lsys)
	resp := &AdResponse{
		ID: cids[1],
		Ad: adv,
	}
	b, err = MarshalResp(resp)
	require.NoError(t, err)
	r2, err := UnmarshalResp(b)
	require.NoError(t, err)
	require.True(t, ipld.DeepEqual(adv, r2.Ad))
}
