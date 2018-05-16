package main

import (
	"context"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/evalphobia/logrus_sentry"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/nyaruka/ezconf"
	"github.com/nyaruka/rp-archiver"
	"github.com/nyaruka/rp-archiver/s3"
	log "github.com/sirupsen/logrus"
	"os"
)

func main() {
	config := archiver.NewConfig()
	loader := ezconf.NewLoader(&config, "archiver", "Archives RapidPro flows, msgs and sessions to S3", []string{"archiver.toml"})
	loader.MustLoad()

	// configure our logger
	log.SetOutput(os.Stdout)
	log.SetFormatter(&log.TextFormatter{})

	level, err := log.ParseLevel(config.LogLevel)
	if err != nil {
		log.Fatalf("Invalid log level '%s'", level)
	}
	log.SetLevel(level)

	// if we have a DSN entry, try to initialize it
	if config.SentryDSN != "" {
		hook, err := logrus_sentry.NewSentryHook(config.SentryDSN, []log.Level{log.PanicLevel, log.FatalLevel, log.ErrorLevel})
		hook.Timeout = 0
		hook.StacktraceConfiguration.Enable = true
		hook.StacktraceConfiguration.Skip = 4
		hook.StacktraceConfiguration.Context = 5
		if err != nil {
			log.Fatalf("Invalid sentry DSN: '%s': %s", config.SentryDSN, err)
		}
		log.StandardLogger().Hooks.Add(hook)
	}

	db, err := sqlx.Open("postgres", config.DB)
	if err != nil {
		log.Fatal(err)
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
		log.WithError(err).Fatal("error creating s3 client")
	}
	s3Session.Handlers.Send.PushFront(func(r *request.Request) {
		log.WithField("headers", r.HTTPRequest.Header).WithField("service", r.ClientInfo.ServiceName).WithField("operation", r.Operation).WithField("params", r.Params).Debug("making aws request")
	})

	s3Client := aws_s3.New(s3Session)

	if config.UploadToS3 {
		// test out our S3 credentials
		err = s3.TestS3(s3Client, config.S3Bucket)
		if err != nil {
			log.WithError(err).Fatal("s3 bucket not reachable")
		} else {
			log.Info("s3 bucket ok")
		}
	}

	ctx := context.Background()
	orgs, err := archiver.GetActiveOrgs(ctx, db)
	if err != nil {
		log.Fatal(err)
	}

	// ensure that we can actually write to the temp directory
	dir_err := archiver.EnsureTempArchiveDirectory(ctx, config.TempDir)
	if dir_err != nil {
		log.Fatal(dir_err)
	}

	for _, org := range orgs {
		log := log.WithField("org", org.Name).WithField("org_id", org.ID)

		if config.ArchiveMessage {
			_, err := archiver.ExecuteArchiving(ctx, config, db, s3Client, org, archiver.MessageType)

			if err != nil {
				log.WithError(err)
				continue
			}
		}

		if config.ArchiveFlowrun {
			_, err := archiver.ExecuteArchiving(ctx, config, db, s3Client, org, archiver.FlowRunType)

			if err != nil {
				log.WithError(err)
				continue
			}
		}

	}
}
