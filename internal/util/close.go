package util

import (
	"io"
	"reflect"
)

// CloseWithErr closes a resource and logs any error.
func CloseWithErr(closer io.Closer, name string) {
	if closer == nil {
		return
	}
	val := reflect.ValueOf(closer)
	if val.Kind() == reflect.Ptr && val.IsNil() {
		return
	}
	if err := closer.Close(); err != nil {
		if name == "" {
			Warnf("close error: %v", err)
			return
		}
		Warnf("close %s: %v", name, err)
	}
}
