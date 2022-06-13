package archives

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/sirupsen/logrus"
)

var s3BucketURL = "https://%s.s3.amazonaws.com%s"

// NewS3Client creates a new s3 client from the passed in config, testing it as necessary
func NewS3Client(config *Config) (s3iface.S3API, error) {
	s3Session, err := session.NewSession(&aws.Config{
		Credentials:      credentials.NewStaticCredentials(config.AWSAccessKeyID, config.AWSSecretAccessKey, ""),
		Endpoint:         aws.String(config.S3Endpoint),
		Region:           aws.String(config.S3Region),
		DisableSSL:       aws.Bool(config.S3DisableSSL),
		S3ForcePathStyle: aws.Bool(config.S3ForcePathStyle),
	})
	if err != nil {
		return nil, err
	}
	s3Session.Handlers.Send.PushFront(func(r *request.Request) {
		logrus.WithField("headers", r.HTTPRequest.Header).WithField("service", r.ClientInfo.ServiceName).WithField("operation", r.Operation).WithField("params", r.Params).Debug("making aws request")
	})

	s3Client := s3.New(s3Session)

	// test out our S3 credentials
	err = TestS3(s3Client, config.S3Bucket)
	if err != nil {
		logrus.WithError(err).Fatal("s3 bucket not reachable")
		return nil, err
	}

	return s3Client, nil
}

// TestS3 tests whether the passed in s3 client is properly configured and the passed in bucket is accessible
func TestS3(s3Client s3iface.S3API, bucket string) error {
	params := &s3.HeadBucketInput{
		Bucket: aws.String(bucket),
	}
	_, err := s3Client.HeadBucket(params)
	if err != nil {
		return err
	}

	return nil
}

// UploadToS3 writes the passed in archive
func UploadToS3(ctx context.Context, s3Client s3iface.S3API, bucket string, path string, archive *Archive) error {
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
	if archive.Size <= 5e9 {
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
		_, err = s3Client.PutObjectWithContext(ctx, params)
		if err != nil {
			return err
		}
	} else {
		// this file is bigger than 5 gigs, use an upload manager instead, it will take care of uploading in parts
		uploader := s3manager.NewUploaderWithClient(
			s3Client,
			func(u *s3manager.Uploader) {
				u.PartSize = 1e9 // 1 gig per part
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

// GetS3FileETAG returns the ETAG hash for the passed in file
func GetS3FileETAG(ctx context.Context, s3Client s3iface.S3API, fileURL string) (string, error) {
	u, err := url.Parse(fileURL)
	if err != nil {
		return "", err
	}

	bucket := strings.Split(u.Host, ".")[0]
	path := u.Path

	output, err := s3Client.HeadObjectWithContext(
		ctx,
		&s3.HeadObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(path),
		},
	)

	if err != nil {
		return "", err
	}

	if output.ETag == nil {
		return "", fmt.Errorf("no ETAG for object")
	}

	// etag is quoted, remove them
	etag := strings.Trim(*output.ETag, `"`)
	return etag, nil
}

// GetS3File return an io.ReadCloser for the passed in bucket and path
func GetS3File(ctx context.Context, s3Client s3iface.S3API, fileURL string) (io.ReadCloser, error) {
	u, err := url.Parse(fileURL)
	if err != nil {
		return nil, err
	}

	bucket := strings.Split(u.Host, ".")[0]
	path := u.Path

	output, err := s3Client.GetObjectWithContext(
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
