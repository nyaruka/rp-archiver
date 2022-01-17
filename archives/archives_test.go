package archives

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
	testDB, err := ioutil.ReadFile("../testdb.sql")
	assert.NoError(t, err)

	db, err := sqlx.Open("postgres", "postgres://temba:temba@localhost:5432/archiver_test?sslmode=disable&TimeZone=UTC")
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
	config := NewConfig()
	orgs, err := GetActiveOrgs(ctx, db, config)
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

	// org 3 again, but changing the archive period so we have no tasks
	orgs[2].RetentionPeriod = 200
	tasks, err = GetMissingDailyArchives(ctx, db, now, orgs[2], MessageType)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(tasks))

	// org 1 again, but lowering the archive period so we have tasks
	orgs[0].RetentionPeriod = 2
	tasks, err = GetMissingDailyArchives(ctx, db, now, orgs[0], MessageType)
	assert.NoError(t, err)
	assert.Equal(t, 58, len(tasks))
	assert.Equal(t, time.Date(2017, 11, 10, 0, 0, 0, 0, time.UTC), tasks[0].StartDate)
	assert.Equal(t, time.Date(2017, 12, 1, 0, 0, 0, 0, time.UTC), tasks[21].StartDate)
	assert.Equal(t, time.Date(2017, 12, 10, 0, 0, 0, 0, time.UTC), tasks[30].StartDate)

}

func TestGetMissingMonthArchives(t *testing.T) {
	db := setup(t)

	// get the tasks for our org
	ctx := context.Background()
	config := NewConfig()
	orgs, err := GetActiveOrgs(ctx, db, config)
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

	config := NewConfig()
	orgs, err := GetActiveOrgs(ctx, db, config)
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

	// build our third task, should have two messages
	task = tasks[2]
	err = CreateArchiveFile(ctx, db, task, "/tmp")
	assert.NoError(t, err)

	// should have two records, second will have attachments
	assert.Equal(t, 3, task.RecordCount)
	assert.Equal(t, int64(528), task.Size)
	assert.Equal(t, time.Date(2017, 8, 12, 0, 0, 0, 0, time.UTC), task.StartDate)
	assert.Equal(t, "b3bf00bf1234ea47f14ffd0171a8ead0", task.Hash)
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
	assert.Equal(t, int64(294), task.Size)
	assert.Equal(t, "bd163ead077774425aa559e30d48ca87", task.Hash)
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

	config := NewConfig()
	orgs, err := GetActiveOrgs(ctx, db, config)
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
	assert.Equal(t, int64(472), task.Size)
	assert.Equal(t, "734d437e1c66d09e033d698c732178f8", task.Hash)
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
	assert.Equal(t, int64(490), task.Size)
	assert.Equal(t, "c2138e3c3009a9c09fc55482903d93e4", task.Hash)
	assertArchiveFile(t, task, "runs2.jsonl")

	DeleteArchiveFile(task)
}

func TestWriteArchiveToDB(t *testing.T) {
	db := setup(t)
	ctx := context.Background()

	config := NewConfig()
	orgs, err := GetActiveOrgs(ctx, db, config)
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
	assert.Equal(t, false, task.NeedsDeletion)

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

const getMsgCount = `
SELECT COUNT(*) 
FROM msgs_msg 
WHERE org_id = $1 and created_on >= $2 and created_on < $3
`

func getCountInRange(db *sqlx.DB, query string, orgID int, start time.Time, end time.Time) (int, error) {
	var count int
	err := db.Get(&count, query, orgID, start, end)
	if err != nil {
		return -1, err
	}
	return count, nil
}

func TestArchiveOrgMessages(t *testing.T) {
	db := setup(t)
	ctx := context.Background()
	deleteTransactionSize = 1

	config := NewConfig()
	orgs, err := GetActiveOrgs(ctx, db, config)
	assert.NoError(t, err)
	now := time.Date(2018, 1, 8, 12, 30, 0, 0, time.UTC)

	os.Args = []string{"rp-archiver"}

	loader := ezconf.NewLoader(&config, "archiver", "Archives RapidPro runs and msgs to S3", nil)
	loader.MustLoad()

	config.Delete = true

	// AWS S3 config in the environment needed to download from S3
	if config.AWSAccessKeyID != "missing_aws_access_key_id" && config.AWSSecretAccessKey != "missing_aws_secret_access_key" {
		s3Client, err := NewS3Client(config)
		assert.NoError(t, err)

		assertCount(t, db, 4, `SELECT count(*) from msgs_broadcast WHERE org_id = $1`, 2)

		created, deleted, err := ArchiveOrg(ctx, now, config, db, s3Client, orgs[1], MessageType)
		assert.NoError(t, err)

		assert.Equal(t, 63, len(created))
		assert.Equal(t, time.Date(2017, 8, 10, 0, 0, 0, 0, time.UTC), created[0].StartDate)
		assert.Equal(t, DayPeriod, created[0].Period)
		assert.Equal(t, 0, created[0].RecordCount)
		assert.Equal(t, int64(23), created[0].Size)
		assert.Equal(t, "f0d79988b7772c003d04a28bd7417a62", created[0].Hash)

		assert.Equal(t, time.Date(2017, 8, 11, 0, 0, 0, 0, time.UTC), created[1].StartDate)
		assert.Equal(t, DayPeriod, created[1].Period)
		assert.Equal(t, 0, created[1].RecordCount)
		assert.Equal(t, int64(23), created[1].Size)
		assert.Equal(t, "f0d79988b7772c003d04a28bd7417a62", created[1].Hash)

		assert.Equal(t, time.Date(2017, 8, 12, 0, 0, 0, 0, time.UTC), created[2].StartDate)
		assert.Equal(t, DayPeriod, created[2].Period)
		assert.Equal(t, 3, created[2].RecordCount)
		assert.Equal(t, int64(528), created[2].Size)
		assert.Equal(t, "b3bf00bf1234ea47f14ffd0171a8ead0", created[2].Hash)

		assert.Equal(t, time.Date(2017, 8, 13, 0, 0, 0, 0, time.UTC), created[3].StartDate)
		assert.Equal(t, DayPeriod, created[3].Period)
		assert.Equal(t, 1, created[3].RecordCount)
		assert.Equal(t, int64(312), created[3].Size)
		assert.Equal(t, "32e61b1431217b59fca0170f637d78a3", created[3].Hash)

		assert.Equal(t, time.Date(2017, 10, 10, 0, 0, 0, 0, time.UTC), created[60].StartDate)
		assert.Equal(t, DayPeriod, created[60].Period)
		assert.Equal(t, 0, created[60].RecordCount)
		assert.Equal(t, int64(23), created[60].Size)
		assert.Equal(t, "f0d79988b7772c003d04a28bd7417a62", created[60].Hash)

		assert.Equal(t, time.Date(2017, 8, 1, 0, 0, 0, 0, time.UTC), created[61].StartDate)
		assert.Equal(t, MonthPeriod, created[61].Period)
		assert.Equal(t, 4, created[61].RecordCount)
		assert.Equal(t, int64(553), created[61].Size)
		assert.Equal(t, "156e45e29b6587cb85ccf75e03800b00", created[61].Hash)

		assert.Equal(t, time.Date(2017, 9, 1, 0, 0, 0, 0, time.UTC), created[62].StartDate)
		assert.Equal(t, MonthPeriod, created[62].Period)
		assert.Equal(t, 0, created[62].RecordCount)
		assert.Equal(t, int64(23), created[62].Size)
		assert.Equal(t, "f0d79988b7772c003d04a28bd7417a62", created[62].Hash)

		// no rollup for october since that had one invalid daily archive

		assert.Equal(t, 63, len(deleted))
		assert.Equal(t, time.Date(2017, 8, 1, 0, 0, 0, 0, time.UTC), deleted[0].StartDate)
		assert.Equal(t, MonthPeriod, deleted[0].Period)

		// shouldn't have any messages remaining for this org for those periods
		for _, d := range deleted {
			count, err := getCountInRange(
				db,
				getMsgCount,
				orgs[1].ID,
				d.StartDate,
				d.endDate(),
			)
			assert.NoError(t, err)
			assert.Equal(t, 0, count)
			assert.False(t, d.NeedsDeletion)
			assert.NotNil(t, d.DeletedOn)
		}

		// our one message in our existing archive (but that had an invalid URL) should still exist however
		count, err := getCountInRange(
			db,
			getMsgCount,
			orgs[1].ID,
			time.Date(2017, 10, 8, 0, 0, 0, 0, time.UTC),
			time.Date(2017, 10, 9, 0, 0, 0, 0, time.UTC),
		)
		assert.NoError(t, err)
		assert.Equal(t, 1, count)

		// and messages on our other orgs should be unaffected
		count, err = getCountInRange(
			db,
			getMsgCount,
			orgs[2].ID,
			time.Date(2015, 1, 1, 0, 0, 0, 0, time.UTC),
			time.Date(2020, 2, 1, 0, 0, 0, 0, time.UTC),
		)
		assert.NoError(t, err)
		assert.Equal(t, 1, count)

		// as is our newer message which was replied to
		count, err = getCountInRange(
			db,
			getMsgCount,
			orgs[1].ID,
			time.Date(2018, 1, 1, 0, 0, 0, 0, time.UTC),
			time.Date(2018, 2, 1, 0, 0, 0, 0, time.UTC),
		)
		assert.NoError(t, err)
		assert.Equal(t, 1, count)

		// one broadcast still exists because it has a schedule, the other because it still has msgs, the last because it is new
		assertCount(t, db, 3, `SELECT count(*) from msgs_broadcast WHERE org_id = $1`, 2)
	}
}

const getRunCount = `
SELECT COUNT(*) 
FROM flows_flowrun 
WHERE org_id = $1 and modified_on >= $2 and modified_on < $3
`

func assertCount(t *testing.T, db *sqlx.DB, expected int, query string, args ...interface{}) {
	var count int
	err := db.Get(&count, query, args...)
	assert.NoError(t, err, "error executing query: %s", query)
	assert.Equal(t, expected, count, "counts mismatch for query %s", query)
}

func TestArchiveOrgRuns(t *testing.T) {
	db := setup(t)
	ctx := context.Background()

	config := NewConfig()
	orgs, err := GetActiveOrgs(ctx, db, config)
	assert.NoError(t, err)
	now := time.Date(2018, 1, 8, 12, 30, 0, 0, time.UTC)

	os.Args = []string{"rp-archiver"}

	loader := ezconf.NewLoader(&config, "archiver", "Archives RapidPro runs and msgs to S3", nil)
	loader.MustLoad()

	config.Delete = true

	// AWS S3 config in the environment needed to download from S3
	if config.AWSAccessKeyID != "missing_aws_access_key_id" && config.AWSSecretAccessKey != "missing_aws_secret_access_key" {
		s3Client, err := NewS3Client(config)
		assert.NoError(t, err)

		created, deleted, err := ArchiveOrg(ctx, now, config, db, s3Client, orgs[2], RunType)
		assert.NoError(t, err)

		assert.Equal(t, 12, len(created))

		assert.Equal(t, time.Date(2017, 8, 1, 0, 0, 0, 0, time.UTC), created[0].StartDate)
		assert.Equal(t, MonthPeriod, created[0].Period)
		assert.Equal(t, 1, created[0].RecordCount)
		assert.Equal(t, int64(490), created[0].Size)
		assert.Equal(t, "c2138e3c3009a9c09fc55482903d93e4", created[0].Hash)

		assert.Equal(t, time.Date(2017, 9, 1, 0, 0, 0, 0, time.UTC), created[1].StartDate)
		assert.Equal(t, MonthPeriod, created[1].Period)
		assert.Equal(t, 0, created[1].RecordCount)
		assert.Equal(t, int64(23), created[1].Size)
		assert.Equal(t, "f0d79988b7772c003d04a28bd7417a62", created[1].Hash)

		assert.Equal(t, time.Date(2017, 10, 1, 0, 0, 0, 0, time.UTC), created[2].StartDate)
		assert.Equal(t, DayPeriod, created[2].Period)
		assert.Equal(t, 0, created[2].RecordCount)
		assert.Equal(t, int64(23), created[2].Size)
		assert.Equal(t, "f0d79988b7772c003d04a28bd7417a62", created[2].Hash)

		assert.Equal(t, time.Date(2017, 10, 10, 0, 0, 0, 0, time.UTC), created[11].StartDate)
		assert.Equal(t, DayPeriod, created[11].Period)
		assert.Equal(t, 2, created[11].RecordCount)
		assert.Equal(t, int64(1984), created[11].Size)
		assert.Equal(t, "869cc00ad4cca0371d07c88d8cf2bf26", created[11].Hash)

		assert.Equal(t, 12, len(deleted))

		// no runs remaining
		for _, d := range deleted {
			count, err := getCountInRange(
				db,
				getRunCount,
				orgs[2].ID,
				d.StartDate,
				d.endDate(),
			)
			assert.NoError(t, err)
			assert.Equal(t, 0, count)

			assert.False(t, d.NeedsDeletion)
			assert.NotNil(t, d.DeletedOn)
		}

		// other org runs unaffected
		count, err := getCountInRange(
			db,
			getRunCount,
			orgs[1].ID,
			time.Date(2015, 1, 1, 0, 0, 0, 0, time.UTC),
			time.Date(2020, 2, 1, 0, 0, 0, 0, time.UTC),
		)
		assert.NoError(t, err)
		assert.Equal(t, 2, count)

		// more recent run unaffected (even though it was parent)
		count, err = getCountInRange(
			db,
			getRunCount,
			orgs[2].ID,
			time.Date(2017, 12, 1, 0, 0, 0, 0, time.UTC),
			time.Date(2018, 1, 1, 0, 0, 0, 0, time.UTC),
		)
		assert.NoError(t, err)
		assert.Equal(t, 1, count)
	}
}
