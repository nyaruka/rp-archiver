package archiver

import (
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

	existing, err := GetCurrentArchives(ctx, db, orgs[0], MessageType)
	assert.NoError(t, err)
	now := time.Date(2018, 1, 8, 12, 30, 0, 0, time.UTC)

	// org 1 is too new, no tasks
	tasks, err := GetMissingDayArchives(existing, now, orgs[0], MessageType)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(tasks))

	// org 2 should have some
	existing, err = GetCurrentArchives(ctx, db, orgs[1], MessageType)
	assert.NoError(t, err)
	tasks, err = GetMissingDayArchives(existing, now, orgs[1], MessageType)
	assert.NoError(t, err)
	assert.Equal(t, 62, len(tasks))
	assert.Equal(t, time.Date(2017, 8, 10, 0, 0, 0, 0, time.UTC), tasks[0].StartDate)
	assert.Equal(t, time.Date(2017, 10, 10, 0, 0, 0, 0, time.UTC), tasks[61].StartDate)

	// org 3 is the same as 2, but two of the tasks have already been built
	existing, err = GetCurrentArchives(ctx, db, orgs[2], MessageType)
	assert.NoError(t, err)
	tasks, err = GetMissingDayArchives(existing, now, orgs[2], MessageType)
	assert.NoError(t, err)
	assert.Equal(t, 60, len(tasks))
	assert.Equal(t, time.Date(2017, 8, 11, 0, 0, 0, 0, time.UTC), tasks[0].StartDate)
	assert.Equal(t, time.Date(2017, 10, 10, 0, 0, 0, 0, time.UTC), tasks[59].StartDate)
}

func TestGetMissingMonthArchives(t *testing.T) {
	db := setup(t)

	// get the tasks for our org
	ctx := context.Background()
	orgs, err := GetActiveOrgs(ctx, db)
	assert.NoError(t, err)

	existing, err := GetCurrentArchives(ctx, db, orgs[0], MessageType)
	assert.NoError(t, err)
	now := time.Date(2018, 1, 8, 12, 30, 0, 0, time.UTC)

	// org 1 is too new, no tasks
	tasks, err := GetMissingMonthArchives(existing, now, orgs[0], MessageType)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(tasks))

	// org 2 should have some
	existing, err = GetCurrentArchives(ctx, db, orgs[1], MessageType)
	assert.NoError(t, err)
	tasks, err = GetMissingMonthArchives(existing, now, orgs[1], MessageType)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(tasks))
	assert.Equal(t, time.Date(2017, 8, 1, 0, 0, 0, 0, time.UTC), tasks[0].StartDate)
	assert.Equal(t, time.Date(2017, 9, 1, 0, 0, 0, 0, time.UTC), tasks[1].StartDate)

	// org 3 is the same as 2, but two of the tasks have already been built
	existing, err = GetCurrentArchives(ctx, db, orgs[2], MessageType)
	assert.NoError(t, err)
	tasks, err = GetMissingMonthArchives(existing, now, orgs[2], MessageType)
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

	existing, err := GetCurrentArchives(ctx, db, orgs[0], MessageType)
	assert.NoError(t, err)
	tasks, err := GetMissingDayArchives(existing, now, orgs[1], MessageType)
	assert.NoError(t, err)
	assert.Equal(t, 62, len(tasks))
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
	assert.Equal(t, int64(442), task.Size)
	assert.Equal(t, "7c39eb3244c34841cf5ca0382519142e", task.Hash)

	DeleteArchiveFile(task)
	_, err = os.Stat(task.ArchiveFile)
	assert.True(t, os.IsNotExist(err))
}

func TestWriteArchiveToDB(t *testing.T) {
	db := setup(t)
	ctx := context.Background()

	orgs, err := GetActiveOrgs(ctx, db)
	assert.NoError(t, err)
	now := time.Date(2018, 1, 8, 12, 30, 0, 0, time.UTC)

	existing, err := GetCurrentArchives(ctx, db, orgs[2], MessageType)
	assert.NoError(t, err)

	tasks, err := GetMissingDayArchives(existing, now, orgs[2], MessageType)
	assert.NoError(t, err)
	assert.Equal(t, 60, len(tasks))
	assert.Equal(t, time.Date(2017, 8, 11, 0, 0, 0, 0, time.UTC), tasks[0].StartDate)

	task := tasks[0]
	task.Dailies = []*Archive{existing[0], existing[1]}

	err = WriteArchiveToDB(ctx, db, task)

	assert.NoError(t, err)
	assert.Equal(t, 4, task.ID)
	assert.Equal(t, false, task.IsPurged)

	// if we recalculate our tasks, we should have one less now
	existing, err = GetCurrentArchives(ctx, db, orgs[2], MessageType)
	assert.Equal(t, task.ID, *existing[0].Rollup)
	assert.Equal(t, task.ID, *existing[2].Rollup)

	assert.NoError(t, err)
	tasks, err = GetMissingDayArchives(existing, now, orgs[2], MessageType)
	assert.NoError(t, err)
	assert.Equal(t, 59, len(tasks))
	assert.Equal(t, time.Date(2017, 8, 12, 0, 0, 0, 0, time.UTC), tasks[0].StartDate)
}

func TestArchiveOrg(t *testing.T) {
	db := setup(t)
	ctx := context.Background()

	orgs, err := GetActiveOrgs(ctx, db)
	assert.NoError(t, err)
	now := time.Date(2018, 1, 8, 12, 30, 0, 0, time.UTC)

	config := NewConfig()
	os.Args = []string{"rp-archiver"}

	loader := ezconf.NewLoader(&config, "archiver", "Archives RapidPro flows, msgs and sessions to S3", nil)
	loader.MustLoad()

	// AWS S3 config in the environment needed to download from S3
	if config.AWSAccessKeyID != "missing_aws_access_key_id" && config.AWSSecretAccessKey != "missing_aws_secret_access_key" {

		s3Client, err := NewS3Client(config)
		assert.NoError(t, err)

		archives, err := ArchiveOrg(ctx, now, config, db, s3Client, orgs[1], MessageType)
		assert.NoError(t, err)

		assert.Equal(t, 64, len(archives))
		assert.Equal(t, time.Date(2017, 8, 10, 0, 0, 0, 0, time.UTC), archives[0].StartDate)
		assert.Equal(t, time.Date(2017, 10, 10, 0, 0, 0, 0, time.UTC), archives[61].StartDate)
		assert.Equal(t, time.Date(2017, 8, 1, 0, 0, 0, 0, time.UTC), archives[62].StartDate)
		assert.Equal(t, time.Date(2017, 9, 1, 0, 0, 0, 0, time.UTC), archives[63].StartDate)

		assert.Equal(t, 0, archives[0].RecordCount)
		assert.Equal(t, int64(23), archives[0].Size)
		assert.Equal(t, "f0d79988b7772c003d04a28bd7417a62", archives[0].Hash)

		assert.Equal(t, 2, archives[2].RecordCount)
		assert.Equal(t, int64(442), archives[2].Size)
		assert.Equal(t, "7c39eb3244c34841cf5ca0382519142e", archives[2].Hash)

		assert.Equal(t, 1, archives[3].RecordCount)
		assert.Equal(t, int64(296), archives[3].Size)
		assert.Equal(t, "92c6ddd5ed1419a7f71156bd32fcb453", archives[3].Hash)

		assert.Equal(t, 3, archives[62].RecordCount)
		assert.Equal(t, int64(464), archives[62].Size)
		assert.Equal(t, "258421e7e296cced927bb7ecd3e35287", archives[62].Hash)

		assert.Equal(t, 0, archives[63].RecordCount)
		assert.Equal(t, int64(23), archives[63].Size)
		assert.Equal(t, "f0d79988b7772c003d04a28bd7417a62", archives[63].Hash)
	}
}
