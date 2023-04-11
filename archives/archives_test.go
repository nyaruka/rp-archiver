package archives

import (
	"compress/gzip"
	"context"
	"io"
	"os"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/nyaruka/ezconf"
	"github.com/nyaruka/gocommon/analytics"
	"github.com/nyaruka/gocommon/dates"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func setup(t *testing.T) *sqlx.DB {
	testDB, err := os.ReadFile("../testdb.sql")
	assert.NoError(t, err)

	db, err := sqlx.Open("postgres", "postgres://archiver_test:temba@localhost:5432/archiver_test?sslmode=disable&TimeZone=UTC")
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
	config := NewDefaultConfig()

	orgs, err := GetActiveOrgs(ctx, db, config)
	assert.NoError(t, err)
	assert.Len(t, orgs, 3)

	now := time.Date(2018, 1, 8, 12, 30, 0, 0, time.UTC)

	// org 1 is too new, no tasks
	tasks, err := GetMissingDailyArchives(ctx, db, now, orgs[0], MessageType)
	assert.NoError(t, err)
	assert.Len(t, tasks, 0)

	// org 2 should have some
	tasks, err = GetMissingDailyArchives(ctx, db, now, orgs[1], MessageType)
	assert.NoError(t, err)
	assert.Len(t, tasks, 61)
	assert.Equal(t, time.Date(2017, 8, 10, 0, 0, 0, 0, time.UTC), tasks[0].StartDate)
	assert.Equal(t, time.Date(2017, 10, 10, 0, 0, 0, 0, time.UTC), tasks[60].StartDate)

	// org 3 is the same as 2, but two of the tasks have already been built
	tasks, err = GetMissingDailyArchives(ctx, db, now, orgs[2], MessageType)
	assert.NoError(t, err)
	assert.Len(t, tasks, 31)
	assert.Equal(t, time.Date(2017, 8, 11, 0, 0, 0, 0, time.UTC), tasks[0].StartDate)
	assert.Equal(t, time.Date(2017, 10, 1, 0, 0, 0, 0, time.UTC), tasks[21].StartDate)
	assert.Equal(t, time.Date(2017, 10, 10, 0, 0, 0, 0, time.UTC), tasks[30].StartDate)

	// org 3 again, but changing the archive period so we have no tasks
	orgs[2].RetentionPeriod = 200
	tasks, err = GetMissingDailyArchives(ctx, db, now, orgs[2], MessageType)
	assert.NoError(t, err)
	assert.Len(t, tasks, 0)

	// org 1 again, but lowering the archive period so we have tasks
	orgs[0].RetentionPeriod = 2
	tasks, err = GetMissingDailyArchives(ctx, db, now, orgs[0], MessageType)
	assert.NoError(t, err)
	assert.Len(t, tasks, 58)
	assert.Equal(t, time.Date(2017, 11, 10, 0, 0, 0, 0, time.UTC), tasks[0].StartDate)
	assert.Equal(t, time.Date(2017, 12, 1, 0, 0, 0, 0, time.UTC), tasks[21].StartDate)
	assert.Equal(t, time.Date(2017, 12, 10, 0, 0, 0, 0, time.UTC), tasks[30].StartDate)

}

func TestGetMissingMonthArchives(t *testing.T) {
	db := setup(t)

	// get the tasks for our org
	ctx := context.Background()
	config := NewDefaultConfig()

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

	config := NewDefaultConfig()
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
	assert.Equal(t, int64(522), task.Size)
	assert.Equal(t, time.Date(2017, 8, 12, 0, 0, 0, 0, time.UTC), task.StartDate)
	assert.Equal(t, "c2c12d94eb758a3c06c5c4e0706934ff", task.Hash)
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
	assert.Equal(t, int64(293), task.Size)
	assert.Equal(t, "c8245a44279102a1612170df3787c32d", task.Hash)
	assertArchiveFile(t, task, "messages2.jsonl")

	DeleteArchiveFile(task)
}

func assertArchiveFile(t *testing.T, archive *Archive, truthName string) {
	testFile, err := os.Open(archive.ArchiveFile)
	assert.NoError(t, err)

	zTestReader, err := gzip.NewReader(testFile)
	assert.NoError(t, err)
	test, err := io.ReadAll(zTestReader)
	assert.NoError(t, err)

	truth, err := os.ReadFile("./testdata/" + truthName)
	assert.NoError(t, err)

	assert.Equal(t, truth, test)
}

func TestCreateRunArchive(t *testing.T) {
	db := setup(t)
	ctx := context.Background()

	err := EnsureTempArchiveDirectory("/tmp")
	assert.NoError(t, err)

	config := NewDefaultConfig()
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

	config := NewDefaultConfig()
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

	config := NewDefaultConfig()
	orgs, err := GetActiveOrgs(ctx, db, config)
	assert.NoError(t, err)
	now := time.Date(2018, 1, 8, 12, 30, 0, 0, time.UTC)

	os.Args = []string{"rp-archiver"}

	loader := ezconf.NewLoader(&config, "archiver", "Archives RapidPro runs and msgs to S3", nil)
	loader.MustLoad()

	config.Delete = true

	// AWS S3 config in the environment needed to download from S3
	if config.AWSAccessKeyID != "" && config.AWSSecretAccessKey != "" {
		s3Client, err := NewS3Client(config)
		assert.NoError(t, err)

		assertCount(t, db, 4, `SELECT count(*) from msgs_broadcast WHERE org_id = $1`, 2)

		dailiesCreated, dailiesFailed, monthliesCreated, monthliesFailed, deleted, err := ArchiveOrg(ctx, now, config, db, s3Client, orgs[1], MessageType)
		assert.NoError(t, err)

		assert.Equal(t, 61, len(dailiesCreated))
		assertArchive(t, dailiesCreated[0], time.Date(2017, 8, 10, 0, 0, 0, 0, time.UTC), DayPeriod, 0, 23, "f0d79988b7772c003d04a28bd7417a62")
		assertArchive(t, dailiesCreated[1], time.Date(2017, 8, 11, 0, 0, 0, 0, time.UTC), DayPeriod, 0, 23, "f0d79988b7772c003d04a28bd7417a62")
		assertArchive(t, dailiesCreated[2], time.Date(2017, 8, 12, 0, 0, 0, 0, time.UTC), DayPeriod, 3, 522, "c2c12d94eb758a3c06c5c4e0706934ff")
		assertArchive(t, dailiesCreated[3], time.Date(2017, 8, 13, 0, 0, 0, 0, time.UTC), DayPeriod, 1, 311, "9eaec21e28af92bc338d9b6bcd712109")
		assertArchive(t, dailiesCreated[4], time.Date(2017, 8, 14, 0, 0, 0, 0, time.UTC), DayPeriod, 0, 23, "f0d79988b7772c003d04a28bd7417a62")

		assert.Equal(t, 0, len(dailiesFailed))

		assert.Equal(t, 2, len(monthliesCreated))
		assertArchive(t, monthliesCreated[0], time.Date(2017, 8, 1, 0, 0, 0, 0, time.UTC), MonthPeriod, 4, 545, "d4ce6331f3c871d394ed3b916144ac85")
		assertArchive(t, monthliesCreated[1], time.Date(2017, 9, 1, 0, 0, 0, 0, time.UTC), MonthPeriod, 0, 23, "f0d79988b7772c003d04a28bd7417a62")

		assert.Equal(t, 0, len(monthliesFailed))

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

func assertArchive(t *testing.T, a *Archive, startDate time.Time, period ArchivePeriod, recordCount int, size int64, hash string) {
	assert.Equal(t, startDate, a.StartDate)
	assert.Equal(t, period, a.Period)
	assert.Equal(t, recordCount, a.RecordCount)
	assert.Equal(t, size, a.Size)
	assert.Equal(t, hash, a.Hash)
}

func TestArchiveOrgRuns(t *testing.T) {
	db := setup(t)
	ctx := context.Background()

	config := NewDefaultConfig()
	orgs, err := GetActiveOrgs(ctx, db, config)
	assert.NoError(t, err)
	now := time.Date(2018, 1, 8, 12, 30, 0, 0, time.UTC)

	os.Args = []string{"rp-archiver"}

	loader := ezconf.NewLoader(&config, "archiver", "Archives RapidPro runs and msgs to S3", nil)
	loader.MustLoad()

	config.Delete = true

	// AWS S3 config in the environment needed to download from S3
	if config.AWSAccessKeyID != "" && config.AWSSecretAccessKey != "" {
		s3Client, err := NewS3Client(config)
		assert.NoError(t, err)

		dailiesCreated, _, monthliesCreated, _, deleted, err := ArchiveOrg(ctx, now, config, db, s3Client, orgs[2], RunType)
		assert.NoError(t, err)

		assert.Equal(t, 10, len(dailiesCreated))
		assertArchive(t, dailiesCreated[0], time.Date(2017, 10, 1, 0, 0, 0, 0, time.UTC), DayPeriod, 0, 23, "f0d79988b7772c003d04a28bd7417a62")
		assertArchive(t, dailiesCreated[9], time.Date(2017, 10, 10, 0, 0, 0, 0, time.UTC), DayPeriod, 2, 1984, "869cc00ad4cca0371d07c88d8cf2bf26")

		assert.Equal(t, 2, len(monthliesCreated))
		assertArchive(t, monthliesCreated[0], time.Date(2017, 8, 1, 0, 0, 0, 0, time.UTC), MonthPeriod, 1, 490, "c2138e3c3009a9c09fc55482903d93e4")
		assertArchive(t, monthliesCreated[1], time.Date(2017, 9, 1, 0, 0, 0, 0, time.UTC), MonthPeriod, 0, 23, "f0d79988b7772c003d04a28bd7417a62")

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
		assert.Equal(t, 3, count)

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

		// org 2 has a run that can't be archived because it's still active - as it has no existing archives
		// this will manifest itself as a monthly which fails to save
		dailiesCreated, dailiesFailed, monthliesCreated, monthliesFailed, _, err := ArchiveOrg(ctx, now, config, db, s3Client, orgs[1], RunType)
		assert.NoError(t, err)

		assert.Equal(t, 31, len(dailiesCreated))
		assertArchive(t, dailiesCreated[0], time.Date(2017, 8, 10, 0, 0, 0, 0, time.UTC), DayPeriod, 0, 23, "f0d79988b7772c003d04a28bd7417a62")

		assert.Equal(t, 1, len(dailiesFailed))
		assertArchive(t, dailiesFailed[0], time.Date(2017, 8, 14, 0, 0, 0, 0, time.UTC), DayPeriod, 0, 0, "")

		assert.Equal(t, 1, len(monthliesCreated))
		assertArchive(t, monthliesCreated[0], time.Date(2017, 9, 1, 0, 0, 0, 0, time.UTC), MonthPeriod, 0, 23, "f0d79988b7772c003d04a28bd7417a62")

		assert.Equal(t, 1, len(monthliesFailed))
		assertArchive(t, monthliesFailed[0], time.Date(2017, 8, 1, 0, 0, 0, 0, time.UTC), MonthPeriod, 0, 0, "")
	}
}

func TestArchiveActiveOrgs(t *testing.T) {
	db := setup(t)
	config := NewDefaultConfig()

	os.Args = []string{"rp-archiver"}
	loader := ezconf.NewLoader(&config, "archiver", "Archives RapidPro runs and msgs to S3", nil)
	loader.MustLoad()

	mockAnalytics := analytics.NewMock()
	analytics.RegisterBackend(mockAnalytics)
	analytics.Start()

	dates.SetNowSource(dates.NewSequentialNowSource(time.Date(2018, 1, 8, 12, 30, 0, 0, time.UTC)))
	defer dates.SetNowSource(dates.DefaultNowSource)

	if config.AWSAccessKeyID != "" && config.AWSSecretAccessKey != "" {
		s3Client, err := NewS3Client(config)
		assert.NoError(t, err)

		err = ArchiveActiveOrgs(db, config, s3Client)
		assert.NoError(t, err)

		assert.Equal(t, map[string][]float64{
			"archiver.archive_elapsed":       {848.0},
			"archiver.orgs_archived":         {3},
			"archiver.msgs_records_archived": {5},
			"archiver.msgs_archives_created": {92},
			"archiver.msgs_archives_failed":  {0},
			"archiver.msgs_rollups_created":  {3},
			"archiver.msgs_rollups_failed":   {0},
			"archiver.runs_records_archived": {4},
			"archiver.runs_archives_created": {41},
			"archiver.runs_archives_failed":  {1},
			"archiver.runs_rollups_created":  {3},
			"archiver.runs_rollups_failed":   {1},
		}, mockAnalytics.Gauges)
	}

	analytics.Stop()
}
