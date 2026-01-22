package uploader

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	cfg "shiro/internal/config"
	"shiro/internal/util"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// S3Uploader uploads case directories to S3-compatible storage.
type S3Uploader struct {
	cfg    cfg.S3Config
	client *s3.Client
}

// NewS3 constructs an uploader from S3 configuration.
func NewS3(cfg cfg.S3Config) (*S3Uploader, error) {
	if !cfg.Enabled {
		return &S3Uploader{cfg: cfg}, nil
	}
	opts := []func(*awsconfig.LoadOptions) error{}
	if cfg.Region != "" {
		opts = append(opts, awsconfig.WithRegion(cfg.Region))
	}
	if cfg.Endpoint != "" {
		resolver := aws.EndpointResolverWithOptionsFunc(func(service, _ string, _ ...any) (aws.Endpoint, error) {
			if service == s3.ServiceID {
				//nolint:staticcheck // AWS SDK v2 global endpoint resolver is deprecated, but required for custom S3 endpoints.
				return aws.Endpoint{URL: cfg.Endpoint, HostnameImmutable: true}, nil
			}
			//nolint:staticcheck // AWS SDK v2 global endpoint resolver is deprecated, but required for custom S3 endpoints.
			return aws.Endpoint{}, &aws.EndpointNotFoundError{}
		})
		//nolint:staticcheck // AWS SDK v2 global endpoint resolver is deprecated, but required for custom S3 endpoints.
		opts = append(opts, awsconfig.WithEndpointResolverWithOptions(resolver))
	}
	if cfg.AccessKeyID != "" && cfg.SecretAccessKey != "" {
		creds := credentials.NewStaticCredentialsProvider(cfg.AccessKeyID, cfg.SecretAccessKey, cfg.SessionToken)
		opts = append(opts, awsconfig.WithCredentialsProvider(creds))
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(context.Background(), opts...)
	if err != nil {
		return nil, err
	}
	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.UsePathStyle = cfg.UsePathStyle
	})
	return &S3Uploader{cfg: cfg, client: client}, nil
}

// Enabled reports whether S3 uploads are configured.
func (u *S3Uploader) Enabled() bool {
	return u.cfg.Enabled
}

// UploadDir uploads a case directory and returns its S3 URL prefix.
func (u *S3Uploader) UploadDir(ctx context.Context, dir string) (string, error) {
	if !u.cfg.Enabled {
		return "", nil
	}
	if u.client == nil {
		return "", fmt.Errorf("s3 uploader is not initialized")
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
	return fmt.Sprintf("s3://%s/%s%s/", u.cfg.Bucket, prefix, base), nil
}

func (u *S3Uploader) uploadFile(ctx context.Context, path, key string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer util.CloseWithErr(file, "s3 upload file")

	info, err := file.Stat()
	if err != nil {
		return err
	}
	_, err = u.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(u.cfg.Bucket),
		Key:           aws.String(key),
		Body:          file,
		ContentLength: aws.Int64(info.Size()),
	})
	return err
}
