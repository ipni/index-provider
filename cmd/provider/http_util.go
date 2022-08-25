package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// doHttpPostReq marshals the req to JSON and sends a POST request with content type
// application/json to the given path.
//
// This function is intended for internal use in CLI to interact with the admin server.
func doHttpPostReq(ctx context.Context, path string, req interface{}) (resp *http.Response, err error) {
	reqBody, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	bodyReader := bytes.NewReader(reqBody)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, path, bodyReader)
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	cl := &http.Client{}
	return cl.Do(httpReq)
}

// errFromHttpResp constructs an error from a HTTP response.
// The error message consists of the textual value of HTTP status, followed by a whitespace,
// followed and the fully read response body.
//
// This function is intended to generate consistent textual output in CLI for a response received
// from the admin server.
func errFromHttpResp(resp *http.Response) error {
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed  to read response: %w", err)
	}
	statusText := http.StatusText(resp.StatusCode)
	return fmt.Errorf("%s: %s", statusText, respBody)
}
