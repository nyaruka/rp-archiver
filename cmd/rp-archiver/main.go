package main

import (
	"log/slog"
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
	"github.com/nyaruka/rp-archiver/utils"
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

	// configure golang std structured logging to route to logrus
	slog.SetDefault(slog.New(utils.NewLogrusHandler(logrus.StandardLogger())))

	logger := slog.With("comp", "main")
	logger.Info("starting archiver", "version", version, "released", date)

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
		logger.Error("invalid db connection string, do not specify a timezone, archiver always uses UTC", "db", config.DB)
	}

	// force our DB connection to be in UTC
	if strings.Contains(config.DB, "?") {
		config.DB += "&TimeZone=UTC"
	} else {
		config.DB += "?TimeZone=UTC"
	}

	db, err := sqlx.Open("postgres", config.DB)
	if err != nil {
		logger.Error("error connecting to db", "error", err)
	} else {
		db.SetMaxOpenConns(2)
		logger.Info("db ok", "state", "starting")
	}

	var s3Client s3iface.S3API
	if config.UploadToS3 {
		s3Client, err = archives.NewS3Client(config)
		if err != nil {
			logger.Error("unable to initialize s3 client", "error", err)
		} else {
			logger.Info("s3 bucket ok", "state", "starting")
		}
	}

	wg := &sync.WaitGroup{}

	// ensure that we can actually write to the temp directory
	err = archives.EnsureTempArchiveDirectory(config.TempDir)
	if err != nil {
		logger.Error("cannot write to temp directory", "error", err)
	} else {
		logger.Info("tmp file access ok", "state", "starting")
	}

	// parse our start time
	timeOfDay, err := dates.ParseTimeOfDay("tt:mm", config.StartTime)
	if err != nil {
		logger.Error("invalid start time supplied, format: HH:MM", "error", err)
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

			logger.Info("sleeping until next archival", "sleep_time", napTime, "next_archival", nextArchival)
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
			slog.Error("error archiving, will retry in 5 minutes", "error", err)
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
