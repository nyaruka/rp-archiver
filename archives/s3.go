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

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/nyaruka/gocommon/s3x"
)

const s3BucketURL = "https://%s.s3.amazonaws.com%s"

// any file over this needs to be uploaded in chunks
const maxSingleUploadBytes = 5e9 // 5GB

// size of chunk to use when doing multi-part uploads
const chunkSizeBytes = 1e9 // 1GB

// NewS3Client creates a new s3 service from the passed in config, testing it as necessary
func NewS3Client(cfg *Config) (*s3x.Service, error) {
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
			ACL:             aws.String(s3.BucketCannedACLPrivate),
			ContentMD5:      aws.String(md5),
			Metadata:        map[string]*string{"md5chksum": aws.String(md5)},
		}
		_, err = s3Client.Client.PutObjectWithContext(ctx, params)
		if err != nil {
			return err
		}
	} else {
		// this file is bigger than limit, use an upload manager instead, it will take care of uploading in parts
		uploader := s3manager.NewUploaderWithClient(
			s3Client.Client,
			func(u *s3manager.Uploader) {
				u.PartSize = chunkSizeBytes
			},
		)
		params := &s3manager.UploadInput{
			Bucket:          aws.String(bucket),
			Key:             aws.String(path),
			Body:            f,
			ContentType:     aws.String("application/json"),
			ContentEncoding: aws.String("gzip"),
			ACL:             aws.String(s3.BucketCannedACLPrivate),
		}

		_, err = uploader.UploadWithContext(ctx, params)
		if err != nil {
			return err
		}
	}

	archive.URL = url
	return nil
}

func withAcceptEncoding(e string) request.Option {
	return func(r *request.Request) {
		r.HTTPRequest.Header.Add("Accept-Encoding", e)
	}
}

// GetS3FileInfo returns the ETAG hash for the passed in file
func GetS3FileInfo(ctx context.Context, s3Client *s3x.Service, fileURL string) (int64, string, error) {
	u, err := url.Parse(fileURL)
	if err != nil {
		return 0, "", err
	}

	bucket := strings.Split(u.Host, ".")[0]
	path := u.Path

	head, err := s3Client.Client.HeadObjectWithContext(
		ctx,
		&s3.HeadObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(path),
		},
	)

	if err != nil {
		return 0, "", err
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
	path := u.Path

	output, err := s3Client.Client.GetObjectWithContext(
		ctx,
		&s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(path),
		},
		withAcceptEncoding("gzip"),
	)

	if err != nil {
		return nil, err
	}

	return output.Body, nil
}
