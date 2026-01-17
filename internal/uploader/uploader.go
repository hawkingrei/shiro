package uploader

import "context"

type Uploader interface {
	Enabled() bool
	UploadDir(ctx context.Context, dir string) (string, error)
}

type NoopUploader struct{}

func (n NoopUploader) Enabled() bool {
	return false
}

func (n NoopUploader) UploadDir(ctx context.Context, dir string) (string, error) {
	return "", nil
}
