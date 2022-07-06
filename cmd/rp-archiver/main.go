package main

import (
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
	"github.com/nyaruka/gocommon/dates"
	"github.com/nyaruka/rp-archiver/archives"
	"github.com/sirupsen/logrus"
)

var (
	// https://goreleaser.com/cookbooks/using-main.version
	version = "dev"
	date    = "unknown"
)

func main() {
	config := archives.NewDefaultConfig()
	loader := ezconf.NewLoader(&config, "archiver", "Archives RapidPro runs and msgs to S3", []string{"archiver.toml"})
	loader.MustLoad()

	if config.KeepFiles && !config.UploadToS3 {
		logrus.Fatal("cannot delete archives and also not upload to s3")
	}

	level, err := logrus.ParseLevel(config.LogLevel)
	if err != nil {
		logrus.Fatalf("Invalid log level '%s'", level)
	}

	logrus.SetLevel(level)
	logrus.SetOutput(os.Stdout)
	logrus.SetFormatter(&logrus.TextFormatter{})
	logrus.WithField("version", version).WithField("released", date).Info("starting archiver")

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
	} else {
		db.SetMaxOpenConns(2)
		logrus.WithField("state", "starting").Info("db ok")
	}

	var s3Client s3iface.S3API
	if config.UploadToS3 {
		s3Client, err = archives.NewS3Client(config)
		if err != nil {
			logrus.WithError(err).Fatal("unable to initialize s3 client")
		} else {
			logrus.WithField("state", "starting").Info("s3 bucket ok")
		}
	}

	wg := &sync.WaitGroup{}

	// ensure that we can actually write to the temp directory
	err = archives.EnsureTempArchiveDirectory(config.TempDir)
	if err != nil {
		logrus.WithError(err).Fatal("cannot write to temp directory")
	} else {
		logrus.WithField("state", "starting").Info("tmp file access ok")
	}

	// parse our start time
	timeOfDay, err := dates.ParseTimeOfDay("tt:mm", config.StartTime)
	if err != nil {
		logrus.WithError(err).Fatal("invalid start time supplied, format: HH:MM")
	}

	// if we have a librato token, configure it
	if config.LibratoToken != "" {
		analytics.RegisterBackend(analytics.NewLibrato(config.LibratoUsername, config.LibratoToken, config.InstanceName, time.Second, wg))
	}

	analytics.Start()

	if config.Once {
		doArchival(db, config, s3Client)
	} else {
		for {
			nextArchival := getNextArchivalTime(timeOfDay)
			napTime := time.Until(nextArchival)

			logrus.WithField("sleep_time", napTime).WithField("next_archival", nextArchival).Info("sleeping until next archival")
			time.Sleep(napTime)

			doArchival(db, config, s3Client)
		}
	}

	analytics.Stop()
	wg.Wait()
}

func doArchival(db *sqlx.DB, cfg *archives.Config, s3Client s3iface.S3API) {
	for {
		// try to archive all active orgs, and if it fails, wait 5 minutes and try again
		err := archives.ArchiveActiveOrgs(db, cfg, s3Client)
		if err != nil {
			logrus.WithError(err).Error("error archiving, will retry in 5 minutes")
			time.Sleep(time.Minute * 5)
			continue
		} else {
			break
		}
	}
}

func getNextArchivalTime(tod dates.TimeOfDay) time.Time {
	t := dates.ExtractDate(dates.Now().In(time.UTC)).Combine(tod, time.UTC)

	// if this time is in the past, add a day
	if t.Before(dates.Now()) {
		t = t.Add(time.Hour * 24)
	}
	return t
}
