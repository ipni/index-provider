package adminserver

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
)

var (
	_ io.ReaderFrom = (*ErrorRes)(nil)
	_ io.ReaderFrom = (*ImportCarReq)(nil)
	_ io.ReaderFrom = (*ImportCarRes)(nil)
	_ io.ReaderFrom = (*ConnectReq)(nil)
	_ io.ReaderFrom = (*ConnectRes)(nil)

	_ io.WriterTo = (*ErrorRes)(nil)
	_ io.WriterTo = (*ImportCarReq)(nil)
	_ io.WriterTo = (*ImportCarRes)(nil)
	_ io.WriterTo = (*ConnectReq)(nil)
	_ io.WriterTo = (*ConnectRes)(nil)
)

func (er *ErrorRes) WriteTo(w io.Writer) (int64, error) {
	return marshalToJson(w, er)
}

func (er *ErrorRes) ReadFrom(r io.Reader) (int64, error) {
	return unmarshalAsJson(r, er)
}

func (er *ImportCarReq) WriteTo(w io.Writer) (int64, error) {
	return marshalToJson(w, er)
}

func (er *ImportCarReq) ReadFrom(r io.Reader) (int64, error) {
	return unmarshalAsJson(r, er)
}

func (er *ImportCarRes) WriteTo(w io.Writer) (int64, error) {
	return marshalToJson(w, er)
}

func (er *ImportCarRes) ReadFrom(r io.Reader) (int64, error) {
	return unmarshalAsJson(r, er)
}

func (er *ConnectReq) WriteTo(w io.Writer) (int64, error) {
	return marshalToJson(w, er)
}

func (er *ConnectReq) ReadFrom(r io.Reader) (int64, error) {
	return unmarshalAsJson(r, er)
}

func (er *ConnectRes) WriteTo(w io.Writer) (int64, error) {
	return marshalToJson(w, er)
}

func (er *ConnectRes) ReadFrom(r io.Reader) (int64, error) {
	return unmarshalAsJson(r, er)
}

func respond(w http.ResponseWriter, statusCode int, body io.WriterTo) {
	// Attempt to serialize body as JSON
	if _, err := body.WriteTo(w); err != nil {
		log.Errorw("faild to write response ", "err", err)
		w.WriteHeader(http.StatusInternalServerError)

		// Attemt to write fallback error response as body.
		if _, err := w.Write(errResponseFailedToMarshalAsJson); err != nil {
			// Nothing we can do; log and move on.
			log.Errorw("faild to write fallback error response", "err", err)
		}
		return
	}

	// Write requested status code now that body is successfully written.
	w.WriteHeader(statusCode)
}

func unmarshalAsJson(r io.Reader, dst interface{}) (int64, error) {
	body, err := ioutil.ReadAll(r)
	if err != nil {
		return 0, err
	}
	return int64(len(body)), json.Unmarshal(body, dst)
}

func marshalToJson(w io.Writer, src interface{}) (int64, error) {
	body, err := json.Marshal(src)
	if err != nil {
		return 0, err
	}
	written, err := w.Write(body)
	return int64(written), err
}
