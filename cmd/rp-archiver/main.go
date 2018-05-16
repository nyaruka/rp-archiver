package main

import (
	"context"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/evalphobia/logrus_sentry"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/nyaruka/ezconf"
	archiver "github.com/nyaruka/rp-archiver"
	"github.com/nyaruka/rp-archiver/s3"
	"github.com/sirupsen/logrus"
)

func main() {
	config := archiver.NewConfig()
	loader := ezconf.NewLoader(&config, "archiver", "Archives RapidPro flows, msgs and sessions to S3", []string{"archiver.toml"})
	loader.MustLoad()

	if config.DeleteAfterUpload && !config.UploadToS3 {
		logrus.Fatal("cannot delete archives and also not upload to s3")
	}

	// configure our logger
	logrus.SetOutput(os.Stdout)
	logrus.SetFormatter(&logrus.TextFormatter{})

	level, err := logrus.ParseLevel(config.LogLevel)
	if err != nil {
		logrus.Fatalf("Invalid log level '%s'", level)
	}
	logrus.SetLevel(level)

	// if we have a DSN entry, try to initialize it
	if config.SentryDSN != "" {
		hook, err := logrus_sentry.NewSentryHook(config.SentryDSN, []logrus.Level{logrus.PanicLevel, logrus.FatalLevel, logrus.ErrorLevel})
		hook.Timeout = 0
		hook.StacktraceConfiguration.Enable = true
		hook.StacktraceConfiguration.Skip = 4
		hook.StacktraceConfiguration.Context = 5
		if err != nil {
			logrus.Fatalf("Invalid sentry DSN: '%s': %s", config.SentryDSN, err)
		}
		logrus.StandardLogger().Hooks.Add(hook)
	}

	db, err := sqlx.Open("postgres", config.DB)
	if err != nil {
		logrus.Fatal(err)
	}

	// create our s3 client
	s3Session, err := session.NewSession(&aws.Config{
		Credentials:      credentials.NewStaticCredentials(config.AWSAccessKeyID, config.AWSSecretAccessKey, ""),
		Endpoint:         aws.String(config.S3Endpoint),
		Region:           aws.String(config.S3Region),
		DisableSSL:       aws.Bool(config.S3DisableSSL),
		S3ForcePathStyle: aws.Bool(config.S3ForcePathStyle),
	})
	if err != nil {
		logrus.WithError(err).Fatal("error creating s3 client")
	}
	s3Session.Handlers.Send.PushFront(func(r *request.Request) {
		logrus.WithField("headers", r.HTTPRequest.Header).WithField("service", r.ClientInfo.ServiceName).WithField("operation", r.Operation).WithField("params", r.Params).Debug("making aws request")
	})

	s3Client := aws_s3.New(s3Session)

	if config.UploadToS3 {
		// test out our S3 credentials
		err = s3.TestS3(s3Client, config.S3Bucket)
		if err != nil {
			logrus.WithError(err).Fatal("s3 bucket not reachable")
		} else {
			logrus.Info("s3 bucket ok")
		}
	}

	// ensure that we can actually write to the temp directory
	ctx := context.Background()
	err = archiver.EnsureTempArchiveDirectory(config.TempDir)
	if err != nil {
		logrus.WithError(err).Fatal("cannot write to temp directory")
	}

	// get our active orgs
	orgs, err := archiver.GetActiveOrgs(ctx, db)
	if err != nil {
		logrus.Fatal(err)
	}

	// for each org, do our export
	for _, org := range orgs {
		log := logrus.WithField("org", org.Name).WithField("org_id", org.ID)
		if config.ArchiveMessages {
			_, err = archiver.ArchiveOrg(ctx, time.Now(), config, db, s3Client, org, archiver.MessageType)
			if err != nil {
				log.WithError(err).Error()
			}
		}
		if config.ArchiveRuns {
			_, err = archiver.ArchiveOrg(ctx, time.Now(), config, db, s3Client, org, archiver.RunType)
			if err != nil {
				log.WithError(err).Error()
			}
		}
	}
}
