package uploader

import "context"

// Uploader uploads a directory and returns a location.
type Uploader interface {
	Enabled() bool
	UploadDir(ctx context.Context, dir string) (string, error)
}

// NoopUploader is a disabled uploader implementation.
type NoopUploader struct{}

func (n NoopUploader) Enabled() bool {
	return false
}

func (n NoopUploader) UploadDir(ctx context.Context, dir string) (string, error) {
	return "", nil
}
