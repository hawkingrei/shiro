package util

import (
	"io"
	"log"
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
			log.Printf("close error: %v", err)
			return
		}
		log.Printf("close %s: %v", name, err)
	}
}
