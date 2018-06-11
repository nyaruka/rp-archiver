# RapidPro Archiver [![Build Status](https://travis-ci.org/nyaruka/rp-archiver.svg?branch=master)](https://travis-ci.org/nyaruka/rp-archiver) [![codecov](https://codecov.io/gh/nyaruka/rp-archiver/branch/master/graph/badge.svg)](https://codecov.io/gh/nyaruka/rp-archiver) [![Go Report Card](https://goreportcard.com/badge/github.com/nyaruka/rp-archiver)](https://goreportcard.com/report/github.com/nyaruka/rp-archiver)

Simple service for archiving messages and runs to S3 from the RapidPro database.

## Usage

```
Archives RapidPro runs and msgs to S3

Usage of archiver:
  -archive-messages
    	whether we should archive messages (default true)
  -archive-runs
    	whether we should archive runs (default true)
  -aws-access-key-id string
    	the access key id to use when authenticating S3 (default "missing_aws_access_key_id")
  -aws-secret-access-key string
    	the secret access key id to use when authenticating S3 (default "missing_aws_secret_access_key")
  -db string
    	the connection string for our database (default "postgres://localhost/archiver_test?sslmode=disable")
  -debug-conf
    	print where config values are coming from
  -delete
    	whether to delete messages and runs from the db after archival (default false)
  -help
    	print usage information
  -keep-files
    	whether we should keep local archive files after upload (default false)
  -log-level string
    	the log level, one of error, warn, info, debug (default "info")
  -s3-bucket string
    	the S3 bucket we will write archives to (default "dl-archiver-test")
  -s3-disable-ssl
    	whether we disable SSL when accessing S3. Should always be set to False unless you're hosting an S3 compatible service within a secure internal network
  -s3-endpoint string
    	the S3 endpoint we will write archives to (default "https://s3.amazonaws.com")
  -s3-force-path-style
    	whether we force S3 path style. Should generally need to default to False unless you're hosting an S3 compatible service
  -s3-region string
    	the S3 region we will write archives to (default "us-east-1")
  -sentry-dsn string
    	the sentry configuration to log errors to, if any
  -temp-dir string
    	directory where temporary archive files are written (default "/tmp")
  -upload-to-s3
    	whether we should upload archive to S3 (default true)

Environment variables:
                   ARCHIVER_ARCHIVE_MESSAGES - bool
                       ARCHIVER_ARCHIVE_RUNS - bool
                  ARCHIVER_AWS_ACCESS_KEY_ID - string
              ARCHIVER_AWS_SECRET_ACCESS_KEY - string
                                 ARCHIVER_DB - string
                             ARCHIVER_DELETE - bool
                         ARCHIVER_KEEP_FILES - bool
                          ARCHIVER_LOG_LEVEL - string
                          ARCHIVER_S3_BUCKET - string
                     ARCHIVER_S3_DISABLE_SSL - bool
                        ARCHIVER_S3_ENDPOINT - string
                ARCHIVER_S3_FORCE_PATH_STYLE - bool
                          ARCHIVER_S3_REGION - string
                         ARCHIVER_SENTRY_DSN - string
                           ARCHIVER_TEMP_DIR - string
                       ARCHIVER_UPLOAD_TO_S3 - bool
```
