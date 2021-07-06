# RapidPro Archiver

[![Build Status](https://github.com/nyaruka/rp-archiver/workflows/CI/badge.svg)](https://github.com/nyaruka/rp-archiver/actions?query=workflow%3ACI) 
[![codecov](https://codecov.io/gh/nyaruka/rp-archiver/branch/main/graph/badge.svg)](https://codecov.io/gh/nyaruka/rp-archiver) 
[![Go Report Card](https://goreportcard.com/badge/github.com/nyaruka/rp-archiver)](https://goreportcard.com/report/github.com/nyaruka/rp-archiver) 

RP-Archiver is the [RapidPro](https://github.com/rapidpro/rapidpro) component responsible for the archiving of
old runs and messages. It interacts directly with the RapidPro database and writes archive files to an 
S3 compatible endpoint.

# Deploying

As Archiver is a go application, it compiles to a binary and that binary along with the config file is all
you need to run it on your server. You can find bundles for each platform in the
[releases directory](https://github.com/nyaruka/rp-archiver/releases). You should only run a single archiver
instance for a deployment.

# Configuration

Archiver uses a tiered configuration system, each option takes precendence over the ones above it:
 1. The configuration file
 2. Environment variables starting with `ARCHIVER_` 
 3. Command line parameters

We recommend running Archiver with no changes to the configuration and no parameters, using only
environment variables to configure it. You can use `% rp-archiver --help` to see a list of the
environment variables and parameters and for more details on each option.

# RapidPro Configuration

For use with RapidPro, you will want to configure these settings:

 * `ARCHIVER_DB`: URL describing how to connect to the RapidPro database (default "postgres://temba:temba@localhost/temba?sslmode=disable")
 * `ARCHIVER_TEMP_DIR`: The directory that temporary archives will be written before upload (default "/tmp")
 * `ARCHIVER_DELETE`: Whether to delete messages and runs after they are archived, we recommend setting this to true for large installations (default false)
 
For writing of archives, Archiver needs access to an S3 bucket, you can configure access to your bucket via:

 * `ARCHIVER_S3_REGION`: The region for your S3 bucket (ex: `ew-west-1`)
 * `ARCHIVER_S3_BUCKET`: The name of your S3 bucket (ex: `dl-archiver-test"`)
 * `ARCHIVER_S3_ENDPOINT`: The S3 endpoint we will write archives to (default "https://s3.amazonaws.com")
 * `ARCHIVER_AWS_ACCESS_KEY_ID`: The AWS access key id used to authenticate to AWS
 * `ARCHIVER_AWS_SECRET_ACCESS_KEY` The AWS secret access key used to authenticate to AWS

Recommended settings for error reporting:

 * `ARCHIVER_SENTRY_DSN`: The DSN to use when logging errors to Sentry

# Development

Once you've checked out the code, you can build Archiver with:

```
go build github.com/nyaruka/rp-archiver/cmd/rp-archiver
```

This will create a new executable in $GOPATH/bin called `rp-archiver`.

To run the tests you need to create the test database:

```
$ createdb archiver_test
```

To run all of the tests:

```
go test ./... -p=1
```

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
  -retention-period int
    	the number of days to keep before archiving (default 90)
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
                   ARCHIVER_RETENTION_PERIOD - int
                          ARCHIVER_S3_BUCKET - string
                     ARCHIVER_S3_DISABLE_SSL - bool
                        ARCHIVER_S3_ENDPOINT - string
                ARCHIVER_S3_FORCE_PATH_STYLE - bool
                          ARCHIVER_S3_REGION - string
                         ARCHIVER_SENTRY_DSN - string
                           ARCHIVER_TEMP_DIR - string
                       ARCHIVER_UPLOAD_TO_S3 - bool
```
