package adminserver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	"github.com/ipfs/go-test/random"
	"github.com/ipni/go-libipni/metadata"
	provider "github.com/ipni/index-provider"
	"github.com/ipni/index-provider/cardatatransfer"
	mock_provider "github.com/ipni/index-provider/mock"
	"github.com/ipni/index-provider/supplier"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
)

func Test_importCarHandler(t *testing.T) {
	wantKey := []byte("lobster")
	wantTp, err := cardatatransfer.TransportFromContextID(wantKey)
	require.NoError(t, err)

	wantMetadata := metadata.Default.New(wantTp)
	require.NoError(t, err)

	mdBytes, err := wantMetadata.MarshalBinary()
	require.NoError(t, err)

	icReq := &ImportCarReq{
		Path:     "fish",
		Key:      wantKey,
		Metadata: mdBytes,
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

	subject := carHandler{cs}

	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(subject.handleImport)
	randCids := random.Cids(1)
	require.NoError(t, err)
	wantCid := randCids[0]

	mockEng.
		EXPECT().
		NotifyPut(gomock.Any(), gomock.Nil(), gomock.Eq(wantKey), gomock.Eq(wantMetadata)).
		Return(wantCid, nil)

	handler.ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)

	respBytes, err := io.ReadAll(rr.Body)
	require.NoError(t, err)

	var resp ImportCarRes
	err = json.Unmarshal(respBytes, &resp)
	require.NoError(t, err)
	require.Equal(t, wantKey, resp.Key)
	require.Equal(t, wantCid, resp.AdvId)
}

func Test_importCarHandlerFail(t *testing.T) {
	wantKey := []byte("lobster")
	wantTp, err := cardatatransfer.TransportFromContextID(wantKey)
	require.NoError(t, err)
	wantMetadata := metadata.Default.New(wantTp)
	mdBytes, err := wantMetadata.MarshalBinary()
	require.NoError(t, err)
	icReq := &ImportCarReq{
		Path:     "fish",
		Key:      wantKey,
		Metadata: mdBytes,
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

	subject := carHandler{cs}

	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(subject.handleImport)

	mockEng.
		EXPECT().
		NotifyPut(gomock.Any(), gomock.Nil(), gomock.Eq(wantKey), gomock.Eq(wantMetadata)).
		Return(cid.Undef, errors.New("fish"))

	handler.ServeHTTP(rr, req)

	require.Equal(t, http.StatusInternalServerError, rr.Code)

	respBytes, err := io.ReadAll(rr.Body)
	require.NoError(t, err)
	require.Equal(t, "failed to import CAR: fish\n", string(respBytes))
}

func Test_importCarAlreadyAdvertised(t *testing.T) {
	wantKey := []byte("lobster")
	wantTp, err := cardatatransfer.TransportFromContextID(wantKey)
	require.NoError(t, err)
	wantMetadata := metadata.Default.New(wantTp)
	mdBytes, err := wantMetadata.MarshalBinary()
	require.NoError(t, err)
	icReq := &ImportCarReq{
		Path:     "fish",
		Key:      wantKey,
		Metadata: mdBytes,
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

	subject := carHandler{cs}

	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(subject.handleImport)

	mockEng.
		EXPECT().
		NotifyPut(gomock.Any(), gomock.Nil(), gomock.Eq(wantKey), gomock.Eq(wantMetadata)).
		Return(cid.Undef, provider.ErrAlreadyAdvertised)

	handler.ServeHTTP(rr, req)

	require.Equal(t, http.StatusConflict, rr.Code)

	respBytes, err := io.ReadAll(rr.Body)
	require.NoError(t, err)
	require.Equal(t, "CAR already advertised\n", string(respBytes))
}

func Test_removeCarHandler(t *testing.T) {
	wantKey := []byte("lobster")
	req := requireRemoveCarHttpRequestFromKey(t, wantKey)

	mc := gomock.NewController(t)
	mockEng := mock_provider.NewMockInterface(mc)
	mockEng.EXPECT().RegisterMultihashLister(gomock.Any())
	ds := dssync.MutexWrap(datastore.NewMapDatastore())
	cs := supplier.NewCarSupplier(mockEng, ds)

	subject := carHandler{cs}

	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(subject.handleRemove)
	wantCid := random.Cids(1)[0]
	requireMockPut(t, mockEng, nil, wantKey, cs)

	mockEng.
		EXPECT().
		NotifyRemove(gomock.Any(), gomock.Eq(peer.ID("")), gomock.Eq(wantKey)).
		Return(wantCid, nil)

	handler.ServeHTTP(rr, req)

	respBytes, err := io.ReadAll(rr.Body)
	require.NoError(t, err)

	require.Equal(t, http.StatusOK, rr.Code, string(respBytes))

	var resp RemoveCarRes
	err = json.Unmarshal(respBytes, &resp)
	require.NoError(t, err)
	require.Equal(t, wantCid, resp.AdvId)
}

func Test_removeCarHandlerFail(t *testing.T) {
	wantKey := []byte("lobster")
	req := requireRemoveCarHttpRequestFromKey(t, wantKey)

	mc := gomock.NewController(t)
	mockEng := mock_provider.NewMockInterface(mc)
	mockEng.EXPECT().RegisterMultihashLister(gomock.Any())
	ds := dssync.MutexWrap(datastore.NewMapDatastore())
	cs := supplier.NewCarSupplier(mockEng, ds)

	subject := carHandler{cs}

	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(subject.handleRemove)
	requireMockPut(t, mockEng, nil, wantKey, cs)

	mockEng.
		EXPECT().
		NotifyRemove(gomock.Any(), gomock.Eq(peer.ID("")), gomock.Eq(wantKey)).
		Return(cid.Undef, errors.New("fish"))

	handler.ServeHTTP(rr, req)

	require.Equal(t, http.StatusInternalServerError, rr.Code)

	respBytes, err := io.ReadAll(rr.Body)
	require.NoError(t, err)
	require.Equal(t, "error removing car: fish\n", string(respBytes))
}

func Test_removeCarHandler_NonExistingCarIsNotFound(t *testing.T) {
	wantKey := []byte("lobster")
	req := requireRemoveCarHttpRequestFromKey(t, wantKey)

	mc := gomock.NewController(t)
	mockEng := mock_provider.NewMockInterface(mc)
	mockEng.EXPECT().RegisterMultihashLister(gomock.Any())
	ds := dssync.MutexWrap(datastore.NewMapDatastore())
	cs := supplier.NewCarSupplier(mockEng, ds)

	subject := carHandler{cs}

	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(subject.handleRemove)
	handler.ServeHTTP(rr, req)

	require.Equal(t, http.StatusNotFound, rr.Code)
}

func Test_removeCarHandler_UnspecifiedKeyIsBadRequest(t *testing.T) {
	req := requireRemoveCarHttpRequestFromKey(t, []byte{})

	mc := gomock.NewController(t)
	mockEng := mock_provider.NewMockInterface(mc)
	mockEng.EXPECT().RegisterMultihashLister(gomock.Any())
	ds := dssync.MutexWrap(datastore.NewMapDatastore())
	cs := supplier.NewCarSupplier(mockEng, ds)

	subject := carHandler{cs}

	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(subject.handleRemove)
	handler.ServeHTTP(rr, req)

	require.Equal(t, http.StatusBadRequest, rr.Code)
}

func Test_removeCarHandler_InvalidJsonIsBadRequest(t *testing.T) {
	req := requireRemoveCarHttpRequest(t, bytes.NewReader([]byte(`{"fish": that was not JSON`)))

	mc := gomock.NewController(t)
	mockEng := mock_provider.NewMockInterface(mc)
	mockEng.EXPECT().RegisterMultihashLister(gomock.Any())
	ds := dssync.MutexWrap(datastore.NewMapDatastore())
	cs := supplier.NewCarSupplier(mockEng, ds)

	subject := carHandler{cs}

	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(subject.handleRemove)
	handler.ServeHTTP(rr, req)

	require.Equal(t, http.StatusBadRequest, rr.Code)
}

func requireRemoveCarHttpRequestFromKey(t *testing.T, key []byte) *http.Request {
	jsonReq, err := json.Marshal(&RemoveCarReq{Key: key})
	require.NoError(t, err)

	req := requireRemoveCarHttpRequest(t, bytes.NewReader(jsonReq))
	return req
}

func requireRemoveCarHttpRequest(t *testing.T, body io.Reader) *http.Request {
	req, err := http.NewRequest(http.MethodPost, "/admin/remove/car", body)
	require.NoError(t, err)
	return req
}

func requireMockPut(t *testing.T, mockEng *mock_provider.MockInterface, provider *peer.AddrInfo, key []byte, cs *supplier.CarSupplier) {
	wantTp, err := cardatatransfer.TransportFromContextID(key)
	require.NoError(t, err)
	wantCid := random.Cids(1)[0]
	wantMetadata := metadata.Default.New(wantTp)

	mockEng.
		EXPECT().
		NotifyPut(gomock.Any(), gomock.Eq(provider), gomock.Eq(key), wantMetadata).
		Return(wantCid, nil)
	_, err = cs.Put(context.Background(), key, "/fish/in/da/sea", wantMetadata)
	require.NoError(t, err)
}

func Test_ListCarHandler(t *testing.T) {
	wantPath := "fish"
	wantKey := []byte("lobster")
	wantTp, err := cardatatransfer.TransportFromContextID(wantKey)
	require.NoError(t, err)

	wantMetadata := metadata.Default.New(wantTp)
	require.NoError(t, err)

	mc := gomock.NewController(t)
	mockEng := mock_provider.NewMockInterface(mc)
	mockEng.EXPECT().RegisterMultihashLister(gomock.Any())
	ds := dssync.MutexWrap(datastore.NewMapDatastore())
	cs := supplier.NewCarSupplier(mockEng, ds)

	subject := carHandler{cs}

	req, err := http.NewRequest(http.MethodGet, "/admin/list/car", nil)
	require.NoError(t, err)

	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(subject.handleList)

	handler.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	respBytes, err := io.ReadAll(rr.Body)
	require.NoError(t, err)

	var respBeforePut ListCarRes
	err = json.Unmarshal(respBytes, &respBeforePut)
	require.NoError(t, err)
	require.Len(t, respBeforePut.Paths, 0)

	randCids := random.Cids(1)
	require.NoError(t, err)
	wantCid := randCids[0]

	mockEng.
		EXPECT().
		NotifyPut(gomock.Any(), gomock.Nil(), gomock.Eq(wantKey), gomock.Eq(wantMetadata)).
		Return(wantCid, nil)

	gotCid, err := cs.Put(context.Background(), wantKey, wantPath, wantMetadata)
	require.NoError(t, err)
	require.Equal(t, wantCid, gotCid)

	handler.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	respBytes, err = io.ReadAll(rr.Body)
	require.NoError(t, err)

	var respAfterPut ListCarRes
	err = json.Unmarshal(respBytes, &respAfterPut)
	require.NoError(t, err)
	require.Len(t, respAfterPut.Paths, 1)
	require.Equal(t, wantPath, respAfterPut.Paths[0])
}
