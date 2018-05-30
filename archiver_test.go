package archiver

import (
	"compress/gzip"
	"context"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/nyaruka/ezconf"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func setup(t *testing.T) *sqlx.DB {
	testDB, err := ioutil.ReadFile("testdb.sql")
	assert.NoError(t, err)

	db, err := sqlx.Open("postgres", "postgres://localhost/archiver_test?sslmode=disable")
	assert.NoError(t, err)

	_, err = db.Exec(string(testDB))
	assert.NoError(t, err)
	logrus.SetLevel(logrus.DebugLevel)

	return db
}

func TestGetMissingDayArchives(t *testing.T) {
	db := setup(t)

	// get the tasks for our org
	ctx := context.Background()
	orgs, err := GetActiveOrgs(ctx, db)
	assert.NoError(t, err)

	now := time.Date(2018, 1, 8, 12, 30, 0, 0, time.UTC)

	// org 1 is too new, no tasks
	tasks, err := GetMissingDailyArchives(ctx, db, now, orgs[0], MessageType)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(tasks))

	// org 2 should have some
	tasks, err = GetMissingDailyArchives(ctx, db, now, orgs[1], MessageType)
	assert.NoError(t, err)
	assert.Equal(t, 61, len(tasks))
	assert.Equal(t, time.Date(2017, 8, 10, 0, 0, 0, 0, time.UTC), tasks[0].StartDate)
	assert.Equal(t, time.Date(2017, 10, 10, 0, 0, 0, 0, time.UTC), tasks[60].StartDate)

	// org 3 is the same as 2, but two of the tasks have already been built
	tasks, err = GetMissingDailyArchives(ctx, db, now, orgs[2], MessageType)
	assert.NoError(t, err)
	assert.Equal(t, 31, len(tasks))
	assert.Equal(t, time.Date(2017, 8, 11, 0, 0, 0, 0, time.UTC), tasks[0].StartDate)
	assert.Equal(t, time.Date(2017, 10, 1, 0, 0, 0, 0, time.UTC), tasks[21].StartDate)
	assert.Equal(t, time.Date(2017, 10, 10, 0, 0, 0, 0, time.UTC), tasks[30].StartDate)
}

func TestGetMissingMonthArchives(t *testing.T) {
	db := setup(t)

	// get the tasks for our org
	ctx := context.Background()
	orgs, err := GetActiveOrgs(ctx, db)
	assert.NoError(t, err)

	now := time.Date(2018, 1, 8, 12, 30, 0, 0, time.UTC)

	// org 1 is too new, no tasks
	tasks, err := GetMissingMonthlyArchives(ctx, db, now, orgs[0], MessageType)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(tasks))

	// org 2 should have some
	tasks, err = GetMissingMonthlyArchives(ctx, db, now, orgs[1], MessageType)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(tasks))
	assert.Equal(t, time.Date(2017, 8, 1, 0, 0, 0, 0, time.UTC), tasks[0].StartDate)
	assert.Equal(t, time.Date(2017, 9, 1, 0, 0, 0, 0, time.UTC), tasks[1].StartDate)

	// org 3 is the same as 2, but two of the tasks have already been built
	tasks, err = GetMissingMonthlyArchives(ctx, db, now, orgs[2], MessageType)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(tasks))
	assert.Equal(t, time.Date(2017, 8, 1, 0, 0, 0, 0, time.UTC), tasks[0].StartDate)
}

func TestCreateMsgArchive(t *testing.T) {
	db := setup(t)
	ctx := context.Background()

	err := EnsureTempArchiveDirectory("/tmp")
	assert.NoError(t, err)

	orgs, err := GetActiveOrgs(ctx, db)
	assert.NoError(t, err)
	now := time.Date(2018, 1, 8, 12, 30, 0, 0, time.UTC)

	tasks, err := GetMissingDailyArchives(ctx, db, now, orgs[1], MessageType)
	assert.NoError(t, err)
	assert.Equal(t, 61, len(tasks))
	task := tasks[0]

	// build our first task, should have no messages
	err = CreateArchiveFile(ctx, db, task, "/tmp")
	assert.NoError(t, err)

	// should have no records and be an empty gzip file
	assert.Equal(t, 0, task.RecordCount)
	assert.Equal(t, int64(23), task.Size)
	assert.Equal(t, "f0d79988b7772c003d04a28bd7417a62", task.Hash)

	DeleteArchiveFile(task)

	// build our third task, should have a single message
	task = tasks[2]
	err = CreateArchiveFile(ctx, db, task, "/tmp")
	assert.NoError(t, err)

	// should have two records, second will have attachments
	assert.Equal(t, 2, task.RecordCount)
	assert.Equal(t, int64(448), task.Size)
	assert.Equal(t, "74ab5f70262ccd7b10ef0ae7274c806d", task.Hash)
	assertArchiveFile(t, task, "messages1.jsonl")

	DeleteArchiveFile(task)
	_, err = os.Stat(task.ArchiveFile)
	assert.True(t, os.IsNotExist(err))

	// test the anonymous case
	tasks, err = GetMissingDailyArchives(ctx, db, now, orgs[2], MessageType)
	assert.NoError(t, err)
	assert.Equal(t, 31, len(tasks))
	task = tasks[0]

	err = CreateArchiveFile(ctx, db, task, "/tmp")
	assert.NoError(t, err)

	// should have one record
	assert.Equal(t, 1, task.RecordCount)
	assert.Equal(t, int64(283), task.Size)
	assert.Equal(t, "d03b1ab8d3312b37d5e0ae38b88e1ea7", task.Hash)
	assertArchiveFile(t, task, "messages2.jsonl")

	DeleteArchiveFile(task)
}

func assertArchiveFile(t *testing.T, archive *Archive, truthName string) {
	testFile, err := os.Open(archive.ArchiveFile)
	assert.NoError(t, err)

	zTestReader, err := gzip.NewReader(testFile)
	assert.NoError(t, err)
	test, err := ioutil.ReadAll(zTestReader)
	assert.NoError(t, err)

	truth, err := ioutil.ReadFile("./testdata/" + truthName)
	assert.NoError(t, err)

	assert.Equal(t, truth, test)
}

func TestCreateRunArchive(t *testing.T) {
	db := setup(t)
	ctx := context.Background()

	err := EnsureTempArchiveDirectory("/tmp")
	assert.NoError(t, err)

	orgs, err := GetActiveOrgs(ctx, db)
	assert.NoError(t, err)
	now := time.Date(2018, 1, 8, 12, 30, 0, 0, time.UTC)

	tasks, err := GetMissingDailyArchives(ctx, db, now, orgs[1], RunType)
	assert.NoError(t, err)
	assert.Equal(t, 62, len(tasks))
	task := tasks[0]

	err = CreateArchiveFile(ctx, db, task, "/tmp")
	assert.NoError(t, err)

	// should have no records and be an empty gzip file
	assert.Equal(t, 0, task.RecordCount)
	assert.Equal(t, int64(23), task.Size)
	assert.Equal(t, "f0d79988b7772c003d04a28bd7417a62", task.Hash)

	DeleteArchiveFile(task)

	task = tasks[2]
	err = CreateArchiveFile(ctx, db, task, "/tmp")
	assert.NoError(t, err)

	// should have two record
	assert.Equal(t, 2, task.RecordCount)
	assert.Equal(t, int64(581), task.Size)
	assert.Equal(t, "d2111b94c94756147838129ca0618f38", task.Hash)
	assertArchiveFile(t, task, "runs1.jsonl")

	DeleteArchiveFile(task)
	_, err = os.Stat(task.ArchiveFile)
	assert.True(t, os.IsNotExist(err))

	// ok, let's do an anon org
	tasks, err = GetMissingDailyArchives(ctx, db, now, orgs[2], RunType)
	assert.NoError(t, err)
	assert.Equal(t, 62, len(tasks))
	task = tasks[0]

	// build our first task, should have no messages
	err = CreateArchiveFile(ctx, db, task, "/tmp")
	assert.NoError(t, err)

	// should have one record
	assert.Equal(t, 1, task.RecordCount)
	assert.Equal(t, int64(393), task.Size)
	assert.Equal(t, "4f3beb90ee4dc586db7b04ddc7e0117d", task.Hash)
	assertArchiveFile(t, task, "runs2.jsonl")

	DeleteArchiveFile(task)
}

func TestWriteArchiveToDB(t *testing.T) {
	db := setup(t)
	ctx := context.Background()

	orgs, err := GetActiveOrgs(ctx, db)
	assert.NoError(t, err)
	now := time.Date(2018, 1, 8, 12, 30, 0, 0, time.UTC)

	existing, err := GetCurrentArchives(ctx, db, orgs[2], MessageType)
	assert.NoError(t, err)

	tasks, err := GetMissingDailyArchives(ctx, db, now, orgs[2], MessageType)
	assert.NoError(t, err)
	assert.Equal(t, 31, len(tasks))
	assert.Equal(t, time.Date(2017, 8, 11, 0, 0, 0, 0, time.UTC), tasks[0].StartDate)

	task := tasks[0]
	task.Dailies = []*Archive{existing[0], existing[1]}

	err = WriteArchiveToDB(ctx, db, task)

	assert.NoError(t, err)
	assert.Equal(t, 5, task.ID)
	assert.Equal(t, false, task.IsPurged)

	// if we recalculate our tasks, we should have one less now
	existing, err = GetCurrentArchives(ctx, db, orgs[2], MessageType)
	assert.Equal(t, task.ID, *existing[0].Rollup)
	assert.Equal(t, task.ID, *existing[2].Rollup)

	assert.NoError(t, err)
	tasks, err = GetMissingDailyArchives(ctx, db, now, orgs[2], MessageType)
	assert.NoError(t, err)
	assert.Equal(t, 30, len(tasks))
	assert.Equal(t, time.Date(2017, 8, 12, 0, 0, 0, 0, time.UTC), tasks[0].StartDate)
}

func TestArchiveOrgMessages(t *testing.T) {
	db := setup(t)
	ctx := context.Background()

	orgs, err := GetActiveOrgs(ctx, db)
	assert.NoError(t, err)
	now := time.Date(2018, 1, 8, 12, 30, 0, 0, time.UTC)

	config := NewConfig()
	os.Args = []string{"rp-archiver"}

	loader := ezconf.NewLoader(&config, "archiver", "Archives RapidPro runs and msgs to S3", nil)
	loader.MustLoad()

	config.DeleteRecords = true

	// AWS S3 config in the environment needed to download from S3
	if config.AWSAccessKeyID != "missing_aws_access_key_id" && config.AWSSecretAccessKey != "missing_aws_secret_access_key" {
		s3Client, err := NewS3Client(config)
		assert.NoError(t, err)

		created, deleted, err := ArchiveOrg(ctx, now, config, db, s3Client, orgs[1], MessageType)
		assert.NoError(t, err)

		assert.Equal(t, 63, len(created))
		assert.Equal(t, time.Date(2017, 8, 10, 0, 0, 0, 0, time.UTC), created[0].StartDate)
		assert.Equal(t, DayPeriod, created[0].Period)

		assert.Equal(t, time.Date(2017, 8, 11, 0, 0, 0, 0, time.UTC), created[1].StartDate)
		assert.Equal(t, DayPeriod, created[1].Period)

		assert.Equal(t, time.Date(2017, 10, 10, 0, 0, 0, 0, time.UTC), created[60].StartDate)
		assert.Equal(t, DayPeriod, created[60].Period)

		assert.Equal(t, time.Date(2017, 8, 1, 0, 0, 0, 0, time.UTC), created[61].StartDate)
		assert.Equal(t, MonthPeriod, created[61].Period)

		assert.Equal(t, time.Date(2017, 9, 1, 0, 0, 0, 0, time.UTC), created[62].StartDate)
		assert.Equal(t, MonthPeriod, created[62].Period)

		assert.Equal(t, 0, created[0].RecordCount)
		assert.Equal(t, int64(23), created[0].Size)
		assert.Equal(t, "f0d79988b7772c003d04a28bd7417a62", created[1].Hash)

		assert.Equal(t, 2, created[2].RecordCount)
		assert.Equal(t, int64(448), created[2].Size)
		assert.Equal(t, "74ab5f70262ccd7b10ef0ae7274c806d", created[2].Hash)

		assert.Equal(t, 1, created[3].RecordCount)
		assert.Equal(t, int64(299), created[3].Size)
		assert.Equal(t, "74ab5f70262ccd7b10ef0ae7274c806d", created[2].Hash)

		assert.Equal(t, 3, created[61].RecordCount)
		assert.Equal(t, int64(470), created[61].Size)
		assert.Equal(t, "f0d79988b7772c003d04a28bd7417a62", created[0].Hash)

		assert.Equal(t, 0, created[62].RecordCount)
		assert.Equal(t, int64(23), created[62].Size)
		assert.Equal(t, "f0d79988b7772c003d04a28bd7417a62", created[62].Hash)

		assert.Equal(t, 63, len(deleted))
		assert.Equal(t, time.Date(2017, 8, 1, 0, 0, 0, 0, time.UTC), deleted[0].StartDate)
		assert.Equal(t, MonthPeriod, deleted[0].Period)
	}
}

func TestArchiveOrgRuns(t *testing.T) {
	db := setup(t)
	ctx := context.Background()

	orgs, err := GetActiveOrgs(ctx, db)
	assert.NoError(t, err)
	now := time.Date(2018, 1, 8, 12, 30, 0, 0, time.UTC)

	config := NewConfig()
	os.Args = []string{"rp-archiver"}

	loader := ezconf.NewLoader(&config, "archiver", "Archives RapidPro runs and msgs to S3", nil)
	loader.MustLoad()

	// AWS S3 config in the environment needed to download from S3
	if config.AWSAccessKeyID != "missing_aws_access_key_id" && config.AWSSecretAccessKey != "missing_aws_secret_access_key" {
		s3Client, err := NewS3Client(config)
		assert.NoError(t, err)

		created, _, err := ArchiveOrg(ctx, now, config, db, s3Client, orgs[2], RunType)
		assert.NoError(t, err)

		assert.Equal(t, 12, len(created))
		assert.Equal(t, time.Date(2017, 8, 1, 0, 0, 0, 0, time.UTC), created[0].StartDate)
		assert.Equal(t, MonthPeriod, created[0].Period)

		assert.Equal(t, time.Date(2017, 9, 1, 0, 0, 0, 0, time.UTC), created[1].StartDate)
		assert.Equal(t, MonthPeriod, created[1].Period)

		assert.Equal(t, time.Date(2017, 10, 1, 0, 0, 0, 0, time.UTC), created[2].StartDate)
		assert.Equal(t, DayPeriod, created[2].Period)

		assert.Equal(t, time.Date(2017, 10, 10, 0, 0, 0, 0, time.UTC), created[11].StartDate)
		assert.Equal(t, DayPeriod, created[11].Period)

		assert.Equal(t, 1, created[0].RecordCount)
		assert.Equal(t, int64(393), created[0].Size)
		assert.Equal(t, "4f3beb90ee4dc586db7b04ddc7e0117d", created[0].Hash)

		assert.Equal(t, 0, created[1].RecordCount)
		assert.Equal(t, int64(23), created[1].Size)
		assert.Equal(t, "f0d79988b7772c003d04a28bd7417a62", created[1].Hash)

		assert.Equal(t, 1, created[11].RecordCount)
		assert.Equal(t, int64(385), created[11].Size)
		assert.Equal(t, "e4ac24080ca5a05539d058cd7fe63291", created[11].Hash)
	}
}
