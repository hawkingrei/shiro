package uploader

import "context"

// Uploader uploads a directory and returns a location.
type Uploader interface {
	Enabled() bool
	UploadDir(ctx context.Context, dir string) (string, error)
}

// NoopUploader is a disabled uploader implementation.
type NoopUploader struct{}

// Enabled always returns false for the noop uploader.
func (n NoopUploader) Enabled() bool {
	return false
}

// UploadDir is a no-op upload that returns an empty location.
func (n NoopUploader) UploadDir(ctx context.Context, dir string) (string, error) {
	return "", nil
}
