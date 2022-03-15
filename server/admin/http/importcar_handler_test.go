package adminserver

import (
	"bytes"
	"encoding/json"
	"errors"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"testing"

	provider "github.com/filecoin-project/index-provider"
	"github.com/filecoin-project/index-provider/cardatatransfer"
	mock_provider "github.com/filecoin-project/index-provider/mock"
	"github.com/filecoin-project/index-provider/supplier"
	"github.com/filecoin-project/index-provider/testutil"
	"github.com/golang/mock/gomock"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/stretchr/testify/require"
)

func Test_importCarHandler(t *testing.T) {
	rng := rand.New(rand.NewSource(1413))
	wantKey := []byte("lobster")
	wantMetadata, err := cardatatransfer.MetadataFromContextID(wantKey)
	require.NoError(t, err)
	wantRawMetadata, err := wantMetadata.MarshalBinary()
	require.NoError(t, err)

	icReq := &ImportCarReq{
		Path:     "fish",
		Key:      wantKey,
		Metadata: wantRawMetadata,
	}

	jsonReq, err := json.Marshal(icReq)
	require.NoError(t, err)

	req, err := http.NewRequest(http.MethodPost, "/admin/import/car", bytes.NewReader(jsonReq))
	require.NoError(t, err)

	mc := gomock.NewController(t)
	mockEng := mock_provider.NewMockInterface(mc)
	mockEng.EXPECT().RegisterMultihashLister(gomock.Any())
	ds := dssync.MutexWrap(datastore.NewMapDatastore())
	cs := supplier.NewCarSupplier(mockEng, ds)

	subject := importCarHandler{cs}

	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(subject.handle)
	randCids, err := testutil.RandomCids(rng, 1)
	require.NoError(t, err)
	wantCid := randCids[0]

	mockEng.
		EXPECT().
		NotifyPut(gomock.Any(), gomock.Eq(wantKey), gomock.Eq(wantMetadata)).
		Return(wantCid, nil)

	handler.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)

	respBytes, err := ioutil.ReadAll(rr.Body)
	require.NoError(t, err)

	var resp ImportCarRes
	err = json.Unmarshal(respBytes, &resp)
	require.NoError(t, err)
	require.Equal(t, wantKey, resp.Key)
	require.Equal(t, wantCid, resp.AdvId)
}

func Test_importCarHandlerFail(t *testing.T) {
	wantKey := []byte("lobster")
	wantMetadata, err := cardatatransfer.MetadataFromContextID(wantKey)
	require.NoError(t, err)
	wantRawMetadata, err := wantMetadata.MarshalBinary()
	require.NoError(t, err)

	icReq := &ImportCarReq{
		Path:     "fish",
		Key:      wantKey,
		Metadata: wantRawMetadata,
	}

	jsonReq, err := json.Marshal(icReq)
	require.NoError(t, err)

	req, err := http.NewRequest(http.MethodPost, "/admin/import/car", bytes.NewReader(jsonReq))
	require.NoError(t, err)

	mc := gomock.NewController(t)
	mockEng := mock_provider.NewMockInterface(mc)
	mockEng.EXPECT().RegisterMultihashLister(gomock.Any())
	ds := dssync.MutexWrap(datastore.NewMapDatastore())
	cs := supplier.NewCarSupplier(mockEng, ds)

	subject := importCarHandler{cs}

	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(subject.handle)

	mockEng.
		EXPECT().
		NotifyPut(gomock.Any(), gomock.Eq(wantKey), gomock.Eq(wantMetadata)).
		Return(cid.Undef, errors.New("fish"))

	handler.ServeHTTP(rr, req)

	require.Equal(t, http.StatusInternalServerError, rr.Code)

	respBytes, err := ioutil.ReadAll(rr.Body)
	require.NoError(t, err)
	require.Equal(t, "failed to import CAR: fish\n", string(respBytes))
}

func Test_importCarAlreadyAdvertised(t *testing.T) {
	wantKey := []byte("lobster")
	wantMetadata, err := cardatatransfer.MetadataFromContextID(wantKey)
	require.NoError(t, err)
	wantRawMetadata, err := wantMetadata.MarshalBinary()
	require.NoError(t, err)

	icReq := &ImportCarReq{
		Path:     "fish",
		Key:      wantKey,
		Metadata: wantRawMetadata,
	}

	jsonReq, err := json.Marshal(icReq)
	require.NoError(t, err)

	req, err := http.NewRequest(http.MethodPost, "/admin/import/car", bytes.NewReader(jsonReq))
	require.NoError(t, err)

	mc := gomock.NewController(t)
	mockEng := mock_provider.NewMockInterface(mc)
	mockEng.EXPECT().RegisterMultihashLister(gomock.Any())
	ds := dssync.MutexWrap(datastore.NewMapDatastore())
	cs := supplier.NewCarSupplier(mockEng, ds)

	subject := importCarHandler{cs}

	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(subject.handle)

	mockEng.
		EXPECT().
		NotifyPut(gomock.Any(), gomock.Eq(wantKey), gomock.Eq(wantMetadata)).
		Return(cid.Undef, provider.ErrAlreadyAdvertised)

	handler.ServeHTTP(rr, req)

	require.Equal(t, http.StatusConflict, rr.Code)

	respBytes, err := ioutil.ReadAll(rr.Body)
	require.NoError(t, err)
	require.Equal(t, "CAR already advertised\n", string(respBytes))
}
