package uploader

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	cfg "shiro/internal/config"
	"shiro/internal/util"

	"cloud.google.com/go/storage"
	"google.golang.org/api/option"
)

// GCSUploader uploads case directories to Google Cloud Storage.
type GCSUploader struct {
	cfg    cfg.GCSConfig
	client *storage.Client
}

// NewGCS constructs an uploader from GCS configuration.
func NewGCS(cfg cfg.GCSConfig) (*GCSUploader, error) {
	if !cfg.Enabled {
		return &GCSUploader{cfg: cfg}, nil
	}
	opts := []option.ClientOption{}
	if strings.TrimSpace(cfg.CredentialsFile) != "" {
		opts = append(opts, option.WithCredentialsFile(strings.TrimSpace(cfg.CredentialsFile)))
	}
	client, err := storage.NewClient(context.Background(), opts...)
	if err != nil {
		return nil, err
	}
	return &GCSUploader{cfg: cfg, client: client}, nil
}

// Enabled reports whether GCS uploads are configured.
func (u *GCSUploader) Enabled() bool {
	return u.cfg.Enabled
}

// UploadDir uploads a case directory and returns its GCS URL prefix.
func (u *GCSUploader) UploadDir(ctx context.Context, dir string) (string, error) {
	if !u.cfg.Enabled {
		return "", nil
	}
	if u.client == nil {
		return "", fmt.Errorf("gcs uploader is not initialized")
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return "", err
	}
	base := filepath.Base(dir)
	prefix := strings.Trim(u.cfg.Prefix, "/")
	if prefix != "" {
		prefix = prefix + "/"
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		path := filepath.Join(dir, entry.Name())
		if err := u.uploadFile(ctx, path, prefix+base+"/"+entry.Name()); err != nil {
			return "", err
		}
	}
	return fmt.Sprintf("gs://%s/%s%s/", u.cfg.Bucket, prefix, base), nil
}

func (u *GCSUploader) uploadFile(ctx context.Context, path, key string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer util.CloseWithErr(file, "gcs upload file")

	writer := u.client.Bucket(u.cfg.Bucket).Object(key).NewWriter(ctx)
	if _, err := io.Copy(writer, file); err != nil {
		_ = writer.Close()
		return err
	}
	return writer.Close()
}
