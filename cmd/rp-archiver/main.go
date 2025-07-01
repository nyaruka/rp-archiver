package main

import (
	"log"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/getsentry/sentry-go"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/nyaruka/ezconf"
	"github.com/nyaruka/gocommon/aws/cwatch"
	"github.com/nyaruka/gocommon/dates"
	"github.com/nyaruka/rp-archiver/archives"
	"github.com/nyaruka/rp-archiver/runtime"
	slogmulti "github.com/samber/slog-multi"
	slogsentry "github.com/samber/slog-sentry/v2"
)

var (
	// https://goreleaser.com/cookbooks/using-main.version
	version = "dev"
	date    = "unknown"
)

func main() {
	config := runtime.NewDefaultConfig()
	loader := ezconf.NewLoader(&config, "archiver", "Archives RapidPro runs and msgs to S3", []string{"archiver.toml"})
	loader.MustLoad()

	if config.KeepFiles && !config.UploadToS3 {
		log.Fatal("cannot delete archives and also not upload to s3")
	}

	var level slog.Level
	err := level.UnmarshalText([]byte(config.LogLevel))
	if err != nil {
		log.Fatalf("invalid log level %s", level)
		os.Exit(1)
	}

	// configure our logger
	logHandler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level})
	slog.SetDefault(slog.New(logHandler))

	logger := slog.With("comp", "main")
	logger.Info("starting archiver", "version", version, "released", date)

	// if we have a DSN entry, try to initialize it
	if config.SentryDSN != "" {
		err := sentry.Init(sentry.ClientOptions{
			Dsn:           config.SentryDSN,
			EnableTracing: false,
		})
		if err != nil {
			log.Fatalf("error initiating sentry client, error %s, dsn %s", err, config.SentryDSN)
			os.Exit(1)
		}

		defer sentry.Flush(2 * time.Second)

		logger = slog.New(
			slogmulti.Fanout(
				logHandler,
				slogsentry.Option{Level: slog.LevelError}.NewSentryHandler(),
			),
		)
		logger = logger.With("release", version)
		slog.SetDefault(logger)
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

	rt := &runtime.Runtime{
		Config: config,
	}

	rt.DB, err = sqlx.Open("postgres", config.DB)
	if err != nil {
		logger.Error("error connecting to db", "error", err)
	} else {
		rt.DB.SetMaxOpenConns(2)
		logger.Info("db ok", "state", "starting")
	}

	if config.UploadToS3 {
		rt.S3, err = archives.NewS3Client(config)
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

	rt.CW, err = cwatch.NewService(config.AWSAccessKeyID, config.AWSSecretAccessKey, config.AWSRegion, config.CloudwatchNamespace, config.DeploymentID)
	if err != nil {
		logger.Error("unable to create cloudwatch service", "error", err)
	} else {
		logger.Info("cloudwatch service ok", "state", "starting")
	}

	if config.Once {
		doArchival(rt)
	} else {
		for {
			nextArchival := getNextArchivalTime(timeOfDay)
			napTime := time.Until(nextArchival)

			logger.Info("sleeping until next archival", "sleep_time", napTime, "next_archival", nextArchival)
			time.Sleep(napTime)

			doArchival(rt)
		}
	}

	wg.Wait()
}

func doArchival(rt *runtime.Runtime) {
	for {
		// try to archive all active orgs, and if it fails, wait 5 minutes and try again
		err := archives.ArchiveActiveOrgs(rt)
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
