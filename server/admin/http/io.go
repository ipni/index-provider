package adminserver

import (
	"io"

	"github.com/filecoin-project/index-provider/server/utils"
)

var (
	_ io.ReaderFrom = (*ImportCarReq)(nil)
	_ io.ReaderFrom = (*ImportCarRes)(nil)
	_ io.ReaderFrom = (*RemoveCarReq)(nil)
	_ io.ReaderFrom = (*RemoveCarRes)(nil)
	_ io.ReaderFrom = (*ConnectReq)(nil)
	_ io.ReaderFrom = (*ConnectRes)(nil)

	_ io.WriterTo = (*ImportCarReq)(nil)
	_ io.WriterTo = (*ImportCarRes)(nil)
	_ io.WriterTo = (*RemoveCarReq)(nil)
	_ io.WriterTo = (*RemoveCarRes)(nil)
	_ io.WriterTo = (*ConnectReq)(nil)
	_ io.WriterTo = (*ConnectRes)(nil)
)

func (er *ImportCarReq) WriteTo(w io.Writer) (int64, error) {
	return utils.MarshalToJson(w, er)
}

func (er *ImportCarReq) ReadFrom(r io.Reader) (int64, error) {
	return utils.UnmarshalAsJson(r, er)
}

func (er *ImportCarRes) WriteTo(w io.Writer) (int64, error) {
	return utils.MarshalToJson(w, er)
}

func (er *ImportCarRes) ReadFrom(r io.Reader) (int64, error) {
	return utils.UnmarshalAsJson(r, er)
}

func (er *RemoveCarReq) WriteTo(w io.Writer) (int64, error) {
	return utils.MarshalToJson(w, er)
}

func (er *RemoveCarReq) ReadFrom(r io.Reader) (int64, error) {
	return utils.UnmarshalAsJson(r, er)
}

func (er *RemoveCarRes) WriteTo(w io.Writer) (int64, error) {
	return utils.MarshalToJson(w, er)
}

func (er *RemoveCarRes) ReadFrom(r io.Reader) (int64, error) {
	return utils.UnmarshalAsJson(r, er)
}

func (er *ListCarRes) WriteTo(w io.Writer) (int64, error) {
	return utils.MarshalToJson(w, er)
}

func (er *ListCarRes) ReadFrom(r io.Reader) (int64, error) {
	return utils.UnmarshalAsJson(r, er)
}

func (er *ConnectReq) WriteTo(w io.Writer) (int64, error) {
	return utils.MarshalToJson(w, er)
}

func (er *ConnectReq) ReadFrom(r io.Reader) (int64, error) {
	return utils.UnmarshalAsJson(r, er)
}

func (er *ConnectRes) WriteTo(w io.Writer) (int64, error) {
	return utils.MarshalToJson(w, er)
}

func (er *ConnectRes) ReadFrom(r io.Reader) (int64, error) {
	return utils.UnmarshalAsJson(r, er)
}
