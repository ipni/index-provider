package utils

import (
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
)

var ErrUnauthorized = errors.New(http.StatusText(http.StatusUnauthorized))

func HttpStatusCodeErr(resp *http.Response) error {
	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed  to read response: %w", err)
	}
	if resp.StatusCode == http.StatusUnauthorized {
		return fmt.Errorf("%s:%w", respBody, ErrUnauthorized)
	}
	statusText := http.StatusText(resp.StatusCode)
	return fmt.Errorf("%s: %s", statusText, respBody)
}
