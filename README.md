# Archiver

[![Build Status](https://github.com/nyaruka/rp-archiver/workflows/CI/badge.svg)](https://github.com/nyaruka/rp-archiver/actions?query=workflow%3ACI) 
[![codecov](https://codecov.io/gh/nyaruka/rp-archiver/branch/main/graph/badge.svg)](https://codecov.io/gh/nyaruka/rp-archiver) 
[![Go Report Card](https://goreportcard.com/badge/github.com/nyaruka/rp-archiver)](https://goreportcard.com/report/github.com/nyaruka/rp-archiver) 

Service for archiving old RapidPro/TextIt runs and messages. It interacts directly with the database 
and writes archive files to an S3 compatible endpoint.

## Deploying

As it is a Go application, it compiles to a binary and that binary along with the config file is all
you need to run it on your server. You can find bundles for each platform in the
[releases directory](https://github.com/nyaruka/rp-archiver/releases). You should only run a single
instance for a deployment.

## Configuration

The service uses a tiered configuration system, each option takes precendence over the ones above it:

 1. The configuration file
 2. Environment variables starting with `ARCHIVER_` 
 3. Command line parameters

We recommend running it with no changes to the configuration and no parameters, using only
environment variables to configure it. You can use `% rp-archiver --help` to see a list of the
environment variables and parameters and for more details on each option.

For use with RapidPro/TextIt, you will need to configure these settings:

 * `ARCHIVER_DB`: URL describing how to connect to the database (default "postgres://temba:temba@localhost/temba?sslmode=disable")
 * `ARCHIVER_TEMP_DIR`: The directory that temporary archives will be written before upload (default "/tmp")
 * `ARCHIVER_DELETE`: Whether to delete messages and runs after they are archived, we recommend setting this to true for large installations (default false)
 
For writing of archives, Archiver needs access to a storage bucket on an S3 compatible service. For AWS we recommend that 
you choose SSE-S3 encryption as this is the only type that supports validation of upload ETags.

 * `ARCHIVER_S3_REGION`: The region for your S3 bucket (ex: `ew-west-1`)
 * `ARCHIVER_S3_BUCKET`: The name of your S3 bucket (ex: `dl-archiver-test"`)
 * `ARCHIVER_S3_ENDPOINT`: The S3 endpoint we will write archives to (default "https://s3.amazonaws.com")
 * `ARCHIVER_AWS_ACCESS_KEY_ID`: The AWS access key id used to authenticate to AWS
 * `ARCHIVER_AWS_SECRET_ACCESS_KEY` The AWS secret access key used to authenticate to AWS

If using a different encryption type or service that produces non-MD5 ETags:

 * `CHECK_S3_HASHES`: can be set to `FALSE` to disable checking of upload hashes.

Recommended settings for error reporting:

 * `ARCHIVER_SENTRY_DSN`: The DSN to use when logging errors to Sentry

## Development

Once you've checked out the code, you can build the service with:

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
go test -p=1 ./...
```
