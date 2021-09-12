package adminserver

import (
	"fmt"
)

func newErrorResponse(format string, args ...interface{}) *ErrorRes {
	return &ErrorRes{
		Message: fmt.Sprintf(format, args...),
	}
}
