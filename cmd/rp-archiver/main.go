package main

import (
	"context"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/evalphobia/logrus_sentry"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/nyaruka/ezconf"
	"github.com/nyaruka/gocommon/analytics"
	"github.com/nyaruka/rp-archiver/archives"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

func main() {
	config := archives.NewDefaultConfig()
	loader := ezconf.NewLoader(&config, "archiver", "Archives RapidPro runs and msgs to S3", []string{"archiver.toml"})
	loader.MustLoad()

	if config.KeepFiles && !config.UploadToS3 {
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
			logrus.Fatalf("invalid sentry DSN: '%s': %s", config.SentryDSN, err)
		}
		logrus.StandardLogger().Hooks.Add(hook)
	}

	// our settings shouldn't contain a timezone, nothing will work right with this not being a constant UTC
	if strings.Contains(config.DB, "TimeZone") {
		logrus.WithField("db", config.DB).Fatalf("invalid db connection string, do not specify a timezone, archiver always uses UTC")
	}

	// force our DB connection to be in UTC
	if strings.Contains(config.DB, "?") {
		config.DB += "&TimeZone=UTC"
	} else {
		config.DB += "?TimeZone=UTC"
	}

	db, err := sqlx.Open("postgres", config.DB)
	if err != nil {
		logrus.Fatal(err)
	}
	db.SetMaxOpenConns(2)

	var s3Client s3iface.S3API
	if config.UploadToS3 {
		s3Client, err = archives.NewS3Client(config)
		if err != nil {
			logrus.WithError(err).Fatal("unable to initialize s3 client")
		}
	}

	wg := &sync.WaitGroup{}

	// if we have a librato token, configure it
	if config.LibratoToken != "" {
		analytics.RegisterBackend(analytics.NewLibrato(config.LibratoUsername, config.LibratoToken, config.InstanceName, time.Second, wg))
	}

	analytics.Start()

	// ensure that we can actually write to the temp directory
	err = archives.EnsureTempArchiveDirectory(config.TempDir)
	if err != nil {
		logrus.WithError(err).Fatal("cannot write to temp directory")
	}

	for {
		start := time.Now().In(time.UTC)

		// convert the start time to time.Time
		layout := "15:04"
		hour, err := time.Parse(layout, config.StartTime)
		if err != nil {
			logrus.WithError(err).Fatal("invalid start time supplied, format: HH:mm")
		}

		// try to archive all active orgs, and if it fails, wait 5 minutes and try again
		err = archiveActiveOrgs(db, config, s3Client)
		if err != nil {
			logrus.WithError(err).Error("error archiving, will retry in 5 minutes")
			time.Sleep(time.Minute * 5)
			continue
		}

		// ok, we did all our work for our orgs, quit if so configured or sleep until the next day
		if config.ExitOnCompletion {
			break
		}

		// build up our next start
		now := time.Now().In(time.UTC)
		nextDay := time.Date(now.Year(), now.Month(), now.Day(), hour.Hour(), hour.Minute(), 0, 0, time.UTC)

		// if this time is before our actual start, add a day
		if nextDay.Before(start) {
			nextDay = nextDay.AddDate(0, 0, 1)
		}

		napTime := nextDay.Sub(time.Now().In(time.UTC))

		if napTime > time.Duration(0) {
			logrus.WithField("time", napTime).WithField("next_start", nextDay).Info("sleeping until next UTC day")
			time.Sleep(napTime)
		} else {
			logrus.WithField("next_start", nextDay).Info("rebuilding immediately without sleep")
		}
	}

	analytics.Stop()
	wg.Wait()
}

func archiveActiveOrgs(db *sqlx.DB, cfg *archives.Config, s3Client s3iface.S3API) error {
	start := time.Now()

	// get our active orgs
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	orgs, err := archives.GetActiveOrgs(ctx, db, cfg)
	cancel()

	if err != nil {
		return errors.Wrap(err, "error getting active orgs")
	}

	// for each org, do our export
	for _, org := range orgs {
		// no single org should take more than 12 hours
		ctx, cancel := context.WithTimeout(context.Background(), time.Hour*12)
		log := logrus.WithField("org", org.Name).WithField("org_id", org.ID)

		if cfg.ArchiveMessages {
			_, _, err = archives.ArchiveOrg(ctx, time.Now(), cfg, db, s3Client, org, archives.MessageType)
			if err != nil {
				log.WithError(err).WithField("archive_type", archives.MessageType).Error("error archiving org messages")
			}
		}
		if cfg.ArchiveRuns {
			_, _, err = archives.ArchiveOrg(ctx, time.Now(), cfg, db, s3Client, org, archives.RunType)
			if err != nil {
				log.WithError(err).WithField("archive_type", archives.RunType).Error("error archiving org runs")
			}
		}

		cancel()
	}

	timeTaken := time.Since(start)
	logrus.WithField("time_taken", timeTaken).WithField("num_orgs", len(orgs)).Info("archiving of active orgs complete")

	analytics.Gauge("archiver.archive_elapsed", timeTaken.Seconds())
	analytics.Gauge("archiver.archive_orgs", float64(len(orgs)))

	return nil
}
