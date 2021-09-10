package adminserver

import (
	"encoding/json"
	"fmt"
)

// errResponseFailedToMarshalAsJson captures a fallback payload set in package init
// to assure there is always a valid error response to return should all else fails.
var errResponseFailedToMarshalAsJson []byte

func init() {
	// Assure there is always a marshalled response error should all things fail.
	var err error
	errFailedToMarshal := &ErrorRes{"failed to marshal response as JSON."}
	errResponseFailedToMarshalAsJson, err = json.Marshal(errFailedToMarshal)
	if err != nil {
		panic(err)
	}
}

func newErrorResponse(format string, args ...interface{}) *ErrorRes {
	return &ErrorRes{
		Message: fmt.Sprintf(format, args...),
	}
}
