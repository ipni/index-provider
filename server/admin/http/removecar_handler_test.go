package adminserver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/filecoin-project/index-provider/metadata"
	"io"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"testing"

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

func Test_removeCarHandler(t *testing.T) {
	rng := rand.New(rand.NewSource(1413))
	wantKey := []byte("lobster")
	req := requireRemoveCarHttpRequestFromKey(t, wantKey)

	mc := gomock.NewController(t)
	mockEng := mock_provider.NewMockInterface(mc)
	mockEng.EXPECT().RegisterMultihashLister(gomock.Any())
	ds := dssync.MutexWrap(datastore.NewMapDatastore())
	cs := supplier.NewCarSupplier(mockEng, ds)

	subject := removeCarHandler{cs}

	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(subject.handle)
	wantCid := testutil.RandomCids(t, rng, 1)[0]
	requireMockPut(t, mockEng, wantKey, cs, rng)

	mockEng.
		EXPECT().
		NotifyRemove(gomock.Any(), gomock.Eq(wantKey)).
		Return(wantCid, nil)

	handler.ServeHTTP(rr, req)

	respBytes, err := ioutil.ReadAll(rr.Body)
	require.NoError(t, err)

	require.Equal(t, http.StatusOK, rr.Code, string(respBytes))

	var resp RemoveCarRes
	err = json.Unmarshal(respBytes, &resp)
	require.NoError(t, err)
	require.Equal(t, wantCid, resp.AdvId)
}

func Test_removeCarHandlerFail(t *testing.T) {
	rng := rand.New(rand.NewSource(1413))
	wantKey := []byte("lobster")
	req := requireRemoveCarHttpRequestFromKey(t, wantKey)

	mc := gomock.NewController(t)
	mockEng := mock_provider.NewMockInterface(mc)
	mockEng.EXPECT().RegisterMultihashLister(gomock.Any())
	ds := dssync.MutexWrap(datastore.NewMapDatastore())
	cs := supplier.NewCarSupplier(mockEng, ds)

	subject := removeCarHandler{cs}

	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(subject.handle)
	requireMockPut(t, mockEng, wantKey, cs, rng)

	mockEng.
		EXPECT().
		NotifyRemove(gomock.Any(), gomock.Eq(wantKey)).
		Return(cid.Undef, errors.New("fish"))

	handler.ServeHTTP(rr, req)

	require.Equal(t, http.StatusInternalServerError, rr.Code)

	respBytes, err := ioutil.ReadAll(rr.Body)
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

	subject := removeCarHandler{cs}

	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(subject.handle)
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

	subject := removeCarHandler{cs}

	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(subject.handle)
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

	subject := removeCarHandler{cs}

	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(subject.handle)
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

func requireMockPut(t *testing.T, mockEng *mock_provider.MockInterface, key []byte, cs *supplier.CarSupplier, rng *rand.Rand) {
	wantTp, err := cardatatransfer.TransportFromContextID(key)
	require.NoError(t, err)
	wantCid := testutil.RandomCids(t, rng, 1)[0]
	wantMetadata := metadata.New(wantTp)
	mockEng.
		EXPECT().
		NotifyPut(gomock.Any(), gomock.Eq(key), wantMetadata).
		Return(wantCid, nil)
	_, err = cs.Put(context.Background(), key, "/fish/in/da/sea", wantMetadata)
	require.NoError(t, err)
}
