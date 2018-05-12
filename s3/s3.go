package s3

import (
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
)

var s3BucketURL = "https://%s.s3.amazonaws.com%s"

// TestS3 tests whether the passed in s3 client is properly configured and the passed in bucket is accessible
func TestS3(s3Client s3iface.S3API, bucket string) error {
	params := &aws_s3.HeadBucketInput{
		Bucket: aws.String(bucket),
	}
	_, err := s3Client.HeadBucket(params)
	if err != nil {
		return err
	}

	return nil
}

// PutS3File writes the passed in file to the bucket with the passed in content type
func PutS3File(s3Client s3iface.S3API, bucket string, path string, contentType string, contentEncoding string, filename string, md5 string) (string, error) {
	f, err := os.Open(filename)
	if err != nil {
		return "", err
	}
	defer f.Close()

	params := &aws_s3.PutObjectInput{
		Bucket:          aws.String(bucket),
		Body:            f,
		Key:             aws.String(path),
		ContentType:     aws.String(contentType),
		ContentEncoding: aws.String(contentEncoding),
		ACL:             aws.String(aws_s3.BucketCannedACLPrivate),
		ContentMD5:      aws.String(md5),
		Metadata:        map[string]*string{"md5chksum": aws.String(md5)},
	}
	_, err = s3Client.PutObject(params)
	if err != nil {
		return "", err
	}

	url := fmt.Sprintf(s3BucketURL, bucket, path)
	return url, nil
}
