package adminserver

import (
	"encoding/json"
	"github.com/fxamacker/cbor/v2"
	"io"
	"io/ioutil"
	"net/http"
)

var (
	_ io.ReaderFrom = (*ImportCarReq)(nil)
	_ io.ReaderFrom = (*ImportCarRes)(nil)
	_ io.ReaderFrom = (*RemoveCarReq)(nil)
	_ io.ReaderFrom = (*RemoveCarRes)(nil)
	_ io.ReaderFrom = (*ConnectReq)(nil)
	_ io.ReaderFrom = (*ConnectRes)(nil)

	_ io.ReaderFrom = (*AnnounceAdvReq)(nil)
	_ io.ReaderFrom = (*AnnounceAdvRes)(nil)

	_ io.WriterTo = (*ImportCarReq)(nil)
	_ io.WriterTo = (*ImportCarRes)(nil)
	_ io.WriterTo = (*RemoveCarReq)(nil)
	_ io.WriterTo = (*RemoveCarRes)(nil)
	_ io.WriterTo = (*ConnectReq)(nil)
	_ io.WriterTo = (*ConnectRes)(nil)
	_ io.WriterTo = (*AnnounceAdvReq)(nil)
	_ io.WriterTo = (*AnnounceAdvRes)(nil)
)

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

func (er *RemoveCarReq) WriteTo(w io.Writer) (int64, error) {
	return marshalToJson(w, er)
}

func (er *RemoveCarReq) ReadFrom(r io.Reader) (int64, error) {
	return unmarshalAsJson(r, er)
}

func (er *RemoveCarRes) WriteTo(w io.Writer) (int64, error) {
	return marshalToJson(w, er)
}

func (er *RemoveCarRes) ReadFrom(r io.Reader) (int64, error) {
	return unmarshalAsJson(r, er)
}

func (er *ListCarRes) WriteTo(w io.Writer) (int64, error) {
	return marshalToJson(w, er)
}

func (er *ListCarRes) ReadFrom(r io.Reader) (int64, error) {
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

func (er *AnnounceAdvRes) WriteTo(w io.Writer) (n int64, err error) {
	return marshalToJson(w, er)
}

func (er *AnnounceAdvRes) ReadFrom(r io.Reader) (n int64, err error) {
	return unmarshalAsJson(r, er)
}

func (a *AnnounceAdvReq) ReadFrom(r io.Reader) (n int64, err error) {
	return unmarshalAsCbor(r, a)
}

func (a *AnnounceAdvReq) WriteTo(w io.Writer) (n int64, err error) {
	return marshalToCbor(w, a)
}

func respond(w http.ResponseWriter, statusCode int, body io.WriterTo) {
	w.WriteHeader(statusCode)
	// Attempt to serialize body as JSON
	if _, err := body.WriteTo(w); err != nil {
		log.Errorw("faild to write response ", "err", err)
		return
	}
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

func unmarshalAsCbor(r io.Reader, dst interface{}) (int64, error) {
	body, err := ioutil.ReadAll(r)
	if err != nil {
		return 0, err
	}
	return int64(len(body)), cbor.Unmarshal(body, dst)
}

func marshalToCbor(w io.Writer, src interface{}) (int64, error) {
	body, err := cbor.Marshal(src)
	if err != nil {
		return 0, err
	}
	written, err := w.Write(body)
	return int64(written), err
}
