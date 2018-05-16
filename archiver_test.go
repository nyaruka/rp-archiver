package archiver

import (
	"context"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
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

func TestGetArchiveTasks(t *testing.T) {
	db := setup(t)

	// get the tasks for our org
	ctx := context.Background()
	orgs, err := GetActiveOrgs(ctx, db)
	assert.NoError(t, err)

	now := time.Date(2018, 1, 8, 12, 30, 0, 0, time.UTC)

	// org 1 is too new, no tasks
	tasks, err := GetArchiveTasks(ctx, db, now, orgs[0], MessageType)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(tasks))

	// org 2 should have some
	tasks, err = GetArchiveTasks(ctx, db, now, orgs[1], MessageType)
	assert.NoError(t, err)
	assert.Equal(t, 62, len(tasks))
	assert.Equal(t, time.Date(2017, 8, 10, 0, 0, 0, 0, time.UTC), tasks[0].StartDate)
	assert.Equal(t, time.Date(2017, 10, 10, 0, 0, 0, 0, time.UTC), tasks[61].StartDate)

	// org 3 is the same as 2, but two of the tasks have already been built
	tasks, err = GetArchiveTasks(ctx, db, now, orgs[2], MessageType)
	assert.NoError(t, err)
	assert.Equal(t, 60, len(tasks))
	assert.Equal(t, time.Date(2017, 8, 11, 0, 0, 0, 0, time.UTC), tasks[0].StartDate)
	assert.Equal(t, time.Date(2017, 10, 10, 0, 0, 0, 0, time.UTC), tasks[59].StartDate)
}

func TestCreateMsgArchive(t *testing.T) {
	db := setup(t)
	ctx := context.Background()

	err := EnsureTempArchiveDirectory(ctx, "/tmp")
	assert.NoError(t, err)

	orgs, err := GetActiveOrgs(ctx, db)
	assert.NoError(t, err)
	now := time.Date(2018, 1, 8, 12, 30, 0, 0, time.UTC)

	tasks, err := GetArchiveTasks(ctx, db, now, orgs[1], MessageType)
	assert.NoError(t, err)
	assert.Equal(t, 62, len(tasks))
	task := tasks[0]

	// build our first task, should have no messages
	err = CreateMsgArchive(ctx, db, &task, "/tmp")
	assert.NoError(t, err)

	// should have no records and be an empty gzip file
	assert.Equal(t, 0, task.RecordCount)
	assert.Equal(t, int64(23), task.ArchiveSize)
	assert.Equal(t, "f0d79988b7772c003d04a28bd7417a62", task.ArchiveHash)

	DeleteArchiveFile(&task)

	// build our second task, should have a single message
	task = tasks[2]
	err = CreateMsgArchive(ctx, db, &task, "/tmp")
	assert.NoError(t, err)

	// should have two records, second will have attachments
	assert.Equal(t, 2, task.RecordCount)
	assert.Equal(t, int64(365), task.ArchiveSize)
	assert.Equal(t, "cc67ae0d1edb9caa4c8c56b3d4de58ee", task.ArchiveHash)

	DeleteArchiveFile(&task)
	_, err = os.Stat(task.ArchiveFile)
	assert.True(t, os.IsNotExist(err))
}

func TestWriteArchiveToDB(t *testing.T) {
	db := setup(t)
	ctx := context.Background()

	orgs, err := GetActiveOrgs(ctx, db)
	assert.NoError(t, err)
	now := time.Date(2018, 1, 8, 12, 30, 0, 0, time.UTC)

	tasks, err := GetArchiveTasks(ctx, db, now, orgs[1], MessageType)
	assert.NoError(t, err)
	assert.Equal(t, 62, len(tasks))
	assert.Equal(t, time.Date(2017, 8, 10, 0, 0, 0, 0, time.UTC), tasks[0].StartDate)

	task := tasks[0]
	err = WriteArchiveToDB(ctx, db, &task)

	assert.NoError(t, err)
	assert.Equal(t, 3, task.ID)
	assert.Equal(t, false, task.IsPurged)

	// if we recalculate our tasks, we should have one less now
	tasks, err = GetArchiveTasks(ctx, db, now, orgs[1], MessageType)
	assert.NoError(t, err)
	assert.Equal(t, 61, len(tasks))
	assert.Equal(t, time.Date(2017, 8, 11, 0, 0, 0, 0, time.UTC), tasks[0].StartDate)
}
