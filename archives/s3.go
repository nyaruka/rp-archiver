package archives

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go/middleware"
	"github.com/aws/smithy-go/transport/http"
	"github.com/nyaruka/gocommon/aws/s3x"
	"github.com/nyaruka/rp-archiver/runtime"
)

const s3BucketURL = "https://%s.s3.amazonaws.com/%s"

// any file over this needs to be uploaded in chunks
const maxSingleUploadBytes = 5e9 // 5GB

// size of chunk to use when doing multi-part uploads
const chunkSizeBytes = 1e9 // 1GB

// NewS3Client creates a new s3 service from the passed in config, testing it as necessary
func NewS3Client(cfg *runtime.Config) (*s3x.Service, error) {
	svc, err := s3x.NewService(cfg.AWSAccessKeyID, cfg.AWSSecretAccessKey, cfg.AWSRegion, cfg.S3Endpoint, cfg.S3Minio)
	if err != nil {
		return nil, err
	}

	// test out our S3 credentials
	if err := svc.Test(context.TODO(), cfg.S3Bucket); err != nil {
		slog.Error("s3 bucket not reachable", "error", err)
		return nil, err
	}

	return svc, nil
}

// UploadToS3 writes the passed in archive
func UploadToS3(ctx context.Context, s3Client *s3x.Service, bucket string, path string, archive *Archive) error {
	f, err := os.Open(archive.ArchiveFile)
	if err != nil {
		return err
	}
	defer f.Close()

	url := fmt.Sprintf(s3BucketURL, bucket, path)

	// s3 wants a base64 encoded hash instead of our hex encoded
	hashBytes, _ := hex.DecodeString(archive.Hash)
	md5 := base64.StdEncoding.EncodeToString(hashBytes)

	// if this fits into a single part, upload that way
	if archive.Size <= maxSingleUploadBytes {
		params := &s3.PutObjectInput{
			Bucket:          aws.String(bucket),
			Body:            f,
			Key:             aws.String(path),
			ContentType:     aws.String("application/json"),
			ContentEncoding: aws.String("gzip"),
			ACL:             types.ObjectCannedACLPrivate,
			ContentMD5:      aws.String(md5),
			Metadata:        map[string]string{"md5chksum": md5},
		}
		_, err = s3Client.Client.PutObject(ctx, params)
		if err != nil {
			return err
		}
	} else {
		// this file is bigger than limit, use an upload manager instead, it will take care of uploading in parts
		uploader := manager.NewUploader(
			s3Client.Client,
			func(u *manager.Uploader) {
				u.PartSize = chunkSizeBytes
			},
		)
		params := &s3.PutObjectInput{
			Bucket:          aws.String(bucket),
			Key:             aws.String(path),
			Body:            f,
			ContentType:     aws.String("application/json"),
			ContentEncoding: aws.String("gzip"),
			ACL:             types.ObjectCannedACLPrivate,
		}

		_, err = uploader.Upload(ctx, params)
		if err != nil {
			return err
		}
	}

	archive.URL = url
	return nil
}

func withAcceptEncoding(e string) func(o *s3.Options) {
	return func(o *s3.Options) {
		o.APIOptions = append(o.APIOptions, []func(*middleware.Stack) error{
			http.SetHeaderValue("Accept-Encoding", e),
		}...)
	}
}

// GetS3FileInfo returns the ETAG hash for the passed in file
func GetS3FileInfo(ctx context.Context, s3Client *s3x.Service, fileURL string) (int64, string, error) {
	u, err := url.Parse(fileURL)
	if err != nil {
		return 0, "", err
	}

	bucket := strings.Split(u.Host, ".")[0]
	key := strings.TrimPrefix(u.Path, "/")

	head, err := s3Client.Client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		return 0, "", fmt.Errorf("error looking up S3 object bucket=%s key=%s: %w", bucket, key, err)
	}

	if head.ContentLength == nil || head.ETag == nil {
		return 0, "", fmt.Errorf("no size or ETag returned for S3 object")
	}

	// etag is quoted, remove them
	etag := strings.Trim(*head.ETag, `"`)

	return *head.ContentLength, etag, nil
}

// GetS3File return an io.ReadCloser for the passed in bucket and path
func GetS3File(ctx context.Context, s3Client *s3x.Service, fileURL string) (io.ReadCloser, error) {
	u, err := url.Parse(fileURL)
	if err != nil {
		return nil, err
	}

	bucket := strings.Split(u.Host, ".")[0]
	key := strings.TrimPrefix(u.Path, "/")

	output, err := s3Client.Client.GetObject(
		ctx,
		&s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)},
		withAcceptEncoding("gzip"),
	)
	if err != nil {
		return nil, fmt.Errorf("error fetching S3 object bucket=%s key=%s: %w", bucket, key, err)
	}

	return output.Body, nil
}
