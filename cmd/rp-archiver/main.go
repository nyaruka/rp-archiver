package main

import (
	"context"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/evalphobia/logrus_sentry"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/nyaruka/ezconf"
	archiver "github.com/nyaruka/rp-archiver"
	"github.com/nyaruka/rp-archiver/s3"
	log "github.com/sirupsen/logrus"
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

	s3Client := aws_s3.New(s3Session)

	// test out our S3 credentials
	err = s3.TestS3(s3Client, config.S3ArchiveBucket)
	if err != nil {
		log.WithError(err).Fatal("s3 bucket not reachable")
	} else {
		log.Info("s3 bucket ok")
	}

	ctx := context.Background()
	orgs, err := archiver.GetActiveOrgs(ctx, db)
	if err != nil {
		log.Fatal(err)
	}

	for _, org := range orgs {
		log := log.WithField("org", org.Name).WithField("org_id", org.ID)
		log.Info("checking for archives")

		tasks, err := archiver.GetArchiveTasks(ctx, db, org, archiver.MessageType)
		if err != nil {
			log.WithError(err).Error("error calculating message tasks")
			continue
		}

		for _, task := range tasks {
			log = log.WithField("start_date", task.StartDate).WithField("end_date", task.EndDate).WithField("archive_type", task.ArchiveType)
			log.Info("creating archive")
			err := archiver.CreateMsgArchive(ctx, db, &task)
			if err != nil {
				log.WithError(err).Error("error writing archive file")
				continue
			}
			err = archiver.UploadArchive(ctx, s3Client, config.S3ArchiveBucket, &task)
			if err != nil {
				log.WithError(err).Error("error writing archive to s3")
				continue
			}
			log.WithField("url", task.URL).Info("archive uploaded")

			err = archiver.WriteArchiveToDB(ctx, db, &task)
			if err != nil {
				log.WithError(err).Error("error writing record to db")
				continue
			}
			log.WithField("id", task.ID).Info("archive db record created")
		}
	}
}
