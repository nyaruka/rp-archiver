# RapidPro Archiver [![Build Status](https://travis-ci.org/nyaruka/rp-archiver.svg?branch=master)](https://travis-ci.org/nyaruka/rp-archiver) [![codecov](https://codecov.io/gh/nyaruka/rp-archiver/branch/master/graph/badge.svg)](https://codecov.io/gh/nyaruka/rp-archiver) [![Go Report Card](https://goreportcard.com/badge/github.com/nyaruka/rp-archiver)](https://goreportcard.com/report/github.com/nyaruka/rp-archiver)

Simple service for archiving messages, runs and sessions to S3 from the RapidPro database.

## Usage

```
Archives RapidPro flows, msgs and sessions to S3

Usage of archiver:
  -aws-access-key-id string
    	the access key id to use when authenticating S3 (default "missing_aws_access_key_id")
  -aws-secret-access-key string
    	the secret access key id to use when authenticating S3 (default "missing_aws_secret_access_key")
  -db string
    	the connection string for our database (default "postgres://localhost/temba?sslmode=disable")
  -debug-conf
    	print where config values are coming from
  -help
    	print usage information
  -log-level string
    	the log level, one of error, warn, info, debug (default "info")
  -s3-bucket string
    	the S3 bucket we will write archives to (default "dl-temba-archives")
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

Environment variables:
                  ARCHIVER_AWS_ACCESS_KEY_ID - string
              ARCHIVER_AWS_SECRET_ACCESS_KEY - string
                                 ARCHIVER_DB - string
                          ARCHIVER_LOG_LEVEL - string
                          ARCHIVER_S3_BUCKET - string
                     ARCHIVER_S3_DISABLE_SSL - bool
                        ARCHIVER_S3_ENDPOINT - string
                ARCHIVER_S3_FORCE_PATH_STYLE - bool
                          ARCHIVER_S3_REGION - string
                         ARCHIVER_SENTRY_DSN - string
```
