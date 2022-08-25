package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_doHttpPostReq_PostsExpectedRequest(t *testing.T) {
	wantBody := `{"This":"fish","That":1413,"AndTheOther":true}`
	req := struct {
		This        string
		That        int
		AndTheOther bool
	}{
		"fish", 1413, true,
	}
	handler := http.HandlerFunc(func(_ http.ResponseWriter, httpReq *http.Request) {
		require.Equal(t, "application/json", httpReq.Header["Content-Type"][0])
		reqBody, err := io.ReadAll(httpReq.Body)
		require.NoError(t, err)
		require.Equal(t, wantBody, string(reqBody))
	})
	server := httptest.NewServer(handler)
	defer server.Close()
	_, err := doHttpPostReq(context.Background(), server.URL, req)
	require.NoError(t, err)
}

func Test_doHttpPostReq_FailsToPostWhenServerIsNotAvailable(t *testing.T) {
	_, err := doHttpPostReq(context.Background(), "http://localhost:47891/saw-me-nothin-boss", nil)
	require.Error(t, err)
}

func Test_errFromHttpResp(t *testing.T) {
	r := httptest.NewRecorder()
	_, err := r.WriteString("fish")
	r.Code = http.StatusBadRequest
	r.Flush()
	require.NoError(t, err)
	require.Equal(t, "Bad Request: fish", errFromHttpResp(r.Result()).Error())
}
