package utils

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
)

func Respond(w http.ResponseWriter, statusCode int, body io.WriterTo) error {
	w.WriteHeader(statusCode)
	// Attempt to serialize body as JSON
	if _, err := body.WriteTo(w); err != nil {
		return err
	}
	return nil
}

func UnmarshalAsJson(r io.Reader, dst interface{}) (int64, error) {
	body, err := ioutil.ReadAll(r)
	if err != nil {
		return 0, err
	}
	return int64(len(body)), json.Unmarshal(body, dst)
}

func MarshalToJson(w io.Writer, src interface{}) (int64, error) {
	body, err := json.Marshal(src)
	if err != nil {
		return 0, err
	}
	written, err := w.Write(body)
	return int64(written), err
}
