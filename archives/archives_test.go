package archives

import (
	"compress/gzip"
	"context"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/nyaruka/gocommon/aws/cwatch"
	"github.com/nyaruka/rp-archiver/runtime"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setup(t *testing.T) (context.Context, *runtime.Runtime) {
	ctx := context.Background()
	config := runtime.NewDefaultConfig()
	config.DB = "postgres://archiver_test:temba@localhost:5432/archiver_test?sslmode=disable&TimeZone=UTC"

	// configure S3 to use a local minio instance
	config.AWSAccessKeyID = "root"
	config.AWSSecretAccessKey = "tembatemba"
	config.S3Endpoint = "http://localhost:9000"
	config.S3Minio = true
	config.DeploymentID = "test"

	testDB, err := os.ReadFile("../testdb.sql")
	require.NoError(t, err)

	db, err := sqlx.Open("postgres", config.DB)
	require.NoError(t, err)

	_, err = db.Exec(string(testDB))
	require.NoError(t, err)

	s3Client, err := NewS3Client(config)
	require.NoError(t, err)

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug})))

	CW, err := cwatch.NewService(config.AWSAccessKeyID, config.AWSSecretAccessKey, config.AWSRegion, config.CloudwatchNamespace, config.DeploymentID)
	require.NoError(t, err)

	return ctx, &runtime.Runtime{Config: config, DB: db, S3: s3Client, CW: CW}
}

func TestGetMissingDayArchives(t *testing.T) {
	ctx, rt := setup(t)

	orgs, err := GetActiveOrgs(ctx, rt)
	assert.NoError(t, err)
	assert.Len(t, orgs, 3)

	now := time.Date(2018, 1, 8, 12, 30, 0, 0, time.UTC)

	// org 1 is too new, no tasks
	tasks, err := GetMissingDailyArchives(ctx, rt.DB, now, orgs[0], MessageType)
	assert.NoError(t, err)
	assert.Len(t, tasks, 0)

	// org 2 should have some
	tasks, err = GetMissingDailyArchives(ctx, rt.DB, now, orgs[1], MessageType)
	assert.NoError(t, err)
	assert.Len(t, tasks, 61)
	assert.Equal(t, time.Date(2017, 8, 10, 0, 0, 0, 0, time.UTC), tasks[0].StartDate)
	assert.Equal(t, time.Date(2017, 10, 10, 0, 0, 0, 0, time.UTC), tasks[60].StartDate)

	// org 3 is the same as 2, but two of the tasks have already been built
	tasks, err = GetMissingDailyArchives(ctx, rt.DB, now, orgs[2], MessageType)
	assert.NoError(t, err)
	assert.Len(t, tasks, 31)
	assert.Equal(t, time.Date(2017, 8, 11, 0, 0, 0, 0, time.UTC), tasks[0].StartDate)
	assert.Equal(t, time.Date(2017, 10, 1, 0, 0, 0, 0, time.UTC), tasks[21].StartDate)
	assert.Equal(t, time.Date(2017, 10, 10, 0, 0, 0, 0, time.UTC), tasks[30].StartDate)

	// org 3 again, but changing the archive period so we have no tasks
	orgs[2].RetentionPeriod = 200
	tasks, err = GetMissingDailyArchives(ctx, rt.DB, now, orgs[2], MessageType)
	assert.NoError(t, err)
	assert.Len(t, tasks, 0)

	// org 1 again, but lowering the archive period so we have tasks
	orgs[0].RetentionPeriod = 2
	tasks, err = GetMissingDailyArchives(ctx, rt.DB, now, orgs[0], MessageType)
	assert.NoError(t, err)
	assert.Len(t, tasks, 58)
	assert.Equal(t, time.Date(2017, 11, 10, 0, 0, 0, 0, time.UTC), tasks[0].StartDate)
	assert.Equal(t, time.Date(2017, 12, 1, 0, 0, 0, 0, time.UTC), tasks[21].StartDate)
	assert.Equal(t, time.Date(2017, 12, 10, 0, 0, 0, 0, time.UTC), tasks[30].StartDate)

}

func TestGetMissingMonthArchives(t *testing.T) {
	ctx, rt := setup(t)

	orgs, err := GetActiveOrgs(ctx, rt)
	assert.NoError(t, err)

	now := time.Date(2018, 1, 8, 12, 30, 0, 0, time.UTC)

	// org 1 is too new, no tasks
	tasks, err := GetMissingMonthlyArchives(ctx, rt.DB, now, orgs[0], MessageType)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(tasks))

	// org 2 should have some
	tasks, err = GetMissingMonthlyArchives(ctx, rt.DB, now, orgs[1], MessageType)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(tasks))
	assert.Equal(t, time.Date(2017, 8, 1, 0, 0, 0, 0, time.UTC), tasks[0].StartDate)
	assert.Equal(t, time.Date(2017, 9, 1, 0, 0, 0, 0, time.UTC), tasks[1].StartDate)

	// org 3 is the same as 2, but two of the tasks have already been built
	tasks, err = GetMissingMonthlyArchives(ctx, rt.DB, now, orgs[2], MessageType)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(tasks))
	assert.Equal(t, time.Date(2017, 8, 1, 0, 0, 0, 0, time.UTC), tasks[0].StartDate)

}

func TestCreateMsgArchive(t *testing.T) {
	ctx, rt := setup(t)

	err := EnsureTempArchiveDirectory("/tmp")
	assert.NoError(t, err)

	orgs, err := GetActiveOrgs(ctx, rt)
	assert.NoError(t, err)
	now := time.Date(2018, 1, 8, 12, 30, 0, 0, time.UTC)

	tasks, err := GetMissingDailyArchives(ctx, rt.DB, now, orgs[1], MessageType)
	assert.NoError(t, err)
	assert.Equal(t, 61, len(tasks))
	task := tasks[0]

	// build our first task, should have no messages
	err = CreateArchiveFile(ctx, rt.DB, task, "/tmp")
	assert.NoError(t, err)

	// should have no records and be an empty gzip file
	assert.Equal(t, 0, task.RecordCount)
	assert.Equal(t, int64(23), task.Size)
	assert.Equal(t, "f0d79988b7772c003d04a28bd7417a62", task.Hash)

	DeleteArchiveFile(task)

	// build our third task, should have two messages
	task = tasks[2]
	err = CreateArchiveFile(ctx, rt.DB, task, "/tmp")
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
	tasks, err = GetMissingDailyArchives(ctx, rt.DB, now, orgs[2], MessageType)
	assert.NoError(t, err)
	assert.Equal(t, 31, len(tasks))
	task = tasks[0]

	err = CreateArchiveFile(ctx, rt.DB, task, "/tmp")
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
	ctx, rt := setup(t)

	err := EnsureTempArchiveDirectory("/tmp")
	assert.NoError(t, err)

	orgs, err := GetActiveOrgs(ctx, rt)
	assert.NoError(t, err)
	now := time.Date(2018, 1, 8, 12, 30, 0, 0, time.UTC)

	tasks, err := GetMissingDailyArchives(ctx, rt.DB, now, orgs[1], RunType)
	assert.NoError(t, err)
	assert.Equal(t, 62, len(tasks))
	task := tasks[0]

	err = CreateArchiveFile(ctx, rt.DB, task, "/tmp")
	assert.NoError(t, err)

	// should have no records and be an empty gzip file
	assert.Equal(t, 0, task.RecordCount)
	assert.Equal(t, int64(23), task.Size)
	assert.Equal(t, "f0d79988b7772c003d04a28bd7417a62", task.Hash)

	DeleteArchiveFile(task)

	task = tasks[2]
	err = CreateArchiveFile(ctx, rt.DB, task, "/tmp")
	assert.NoError(t, err)

	// should have two record
	assert.Equal(t, 3, task.RecordCount)
	assert.Equal(t, int64(578), task.Size)
	assert.Equal(t, "cd8ce82019986ac1f4ec1482aac7bca0", task.Hash)
	assertArchiveFile(t, task, "runs1.jsonl")

	DeleteArchiveFile(task)
	_, err = os.Stat(task.ArchiveFile)
	assert.True(t, os.IsNotExist(err))

	// ok, let's do an anon org
	tasks, err = GetMissingDailyArchives(ctx, rt.DB, now, orgs[2], RunType)
	assert.NoError(t, err)
	assert.Equal(t, 62, len(tasks))
	task = tasks[0]

	// build our first task, should have no messages
	err = CreateArchiveFile(ctx, rt.DB, task, "/tmp")
	assert.NoError(t, err)

	// should have one record
	assert.Equal(t, 1, task.RecordCount)
	assert.Equal(t, int64(465), task.Size)
	assert.Equal(t, "40abf2113ea7c25c5476ff3025d54b07", task.Hash)
	assertArchiveFile(t, task, "runs2.jsonl")

	DeleteArchiveFile(task)
}

func TestWriteArchiveToDB(t *testing.T) {
	ctx, rt := setup(t)

	orgs, err := GetActiveOrgs(ctx, rt)
	assert.NoError(t, err)
	now := time.Date(2018, 1, 8, 12, 30, 0, 0, time.UTC)

	existing, err := GetCurrentArchives(ctx, rt.DB, orgs[2], MessageType)
	assert.NoError(t, err)

	tasks, err := GetMissingDailyArchives(ctx, rt.DB, now, orgs[2], MessageType)
	assert.NoError(t, err)
	assert.Equal(t, 31, len(tasks))
	assert.Equal(t, time.Date(2017, 8, 11, 0, 0, 0, 0, time.UTC), tasks[0].StartDate)

	task := tasks[0]
	task.Dailies = []*Archive{existing[0], existing[1]}

	err = WriteArchiveToDB(ctx, rt.DB, task)

	assert.NoError(t, err)
	assert.Equal(t, 5, task.ID)
	assert.Equal(t, false, task.NeedsDeletion)

	// if we recalculate our tasks, we should have one less now
	existing, err = GetCurrentArchives(ctx, rt.DB, orgs[2], MessageType)
	assert.Equal(t, task.ID, *existing[0].Rollup)
	assert.Equal(t, task.ID, *existing[2].Rollup)

	assert.NoError(t, err)
	tasks, err = GetMissingDailyArchives(ctx, rt.DB, now, orgs[2], MessageType)
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
	ctx, rt := setup(t)

	deleteTransactionSize = 1

	orgs, err := GetActiveOrgs(ctx, rt)
	assert.NoError(t, err)
	now := time.Date(2018, 1, 8, 12, 30, 0, 0, time.UTC)

	rt.Config.Delete = true

	assertCount(t, rt.DB, 4, `SELECT count(*) from msgs_broadcast WHERE org_id = $1`, 2)

	dailiesCreated, dailiesFailed, monthliesCreated, monthliesFailed, deleted, err := ArchiveOrg(ctx, rt, now, orgs[1], MessageType)
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
			rt.DB,
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
		rt.DB,
		getMsgCount,
		orgs[1].ID,
		time.Date(2017, 10, 8, 0, 0, 0, 0, time.UTC),
		time.Date(2017, 10, 9, 0, 0, 0, 0, time.UTC),
	)
	assert.NoError(t, err)
	assert.Equal(t, 1, count)

	// and messages on our other orgs should be unaffected
	count, err = getCountInRange(
		rt.DB,
		getMsgCount,
		orgs[2].ID,
		time.Date(2015, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2020, 2, 1, 0, 0, 0, 0, time.UTC),
	)
	assert.NoError(t, err)
	assert.Equal(t, 1, count)

	// as is our newer message which was replied to
	count, err = getCountInRange(
		rt.DB,
		getMsgCount,
		orgs[1].ID,
		time.Date(2018, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2018, 2, 1, 0, 0, 0, 0, time.UTC),
	)
	assert.NoError(t, err)
	assert.Equal(t, 1, count)

	// one broadcast still exists because it has a schedule, the other because it still has msgs, the last because it is new
	assertCount(t, rt.DB, 3, `SELECT count(*) from msgs_broadcast WHERE org_id = $1`, 2)
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
	ctx, rt := setup(t)

	orgs, err := GetActiveOrgs(ctx, rt)
	assert.NoError(t, err)
	now := time.Date(2018, 1, 8, 12, 30, 0, 0, time.UTC)

	rt.Config.Delete = true

	dailiesCreated, _, monthliesCreated, _, deleted, err := ArchiveOrg(ctx, rt, now, orgs[2], RunType)
	assert.NoError(t, err)

	assert.Equal(t, 10, len(dailiesCreated))
	assertArchive(t, dailiesCreated[0], time.Date(2017, 10, 1, 0, 0, 0, 0, time.UTC), DayPeriod, 0, 23, "f0d79988b7772c003d04a28bd7417a62")
	assertArchive(t, dailiesCreated[9], time.Date(2017, 10, 10, 0, 0, 0, 0, time.UTC), DayPeriod, 2, 1953, "95475b968ceff15f2f90d539e1bd3d20")

	assert.Equal(t, 2, len(monthliesCreated))
	assertArchive(t, monthliesCreated[0], time.Date(2017, 8, 1, 0, 0, 0, 0, time.UTC), MonthPeriod, 1, 465, "40abf2113ea7c25c5476ff3025d54b07")
	assertArchive(t, monthliesCreated[1], time.Date(2017, 9, 1, 0, 0, 0, 0, time.UTC), MonthPeriod, 0, 23, "f0d79988b7772c003d04a28bd7417a62")

	assert.Equal(t, 12, len(deleted))

	// no runs remaining
	for _, d := range deleted {
		count, err := getCountInRange(
			rt.DB,
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
		rt.DB,
		getRunCount,
		orgs[1].ID,
		time.Date(2015, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2020, 2, 1, 0, 0, 0, 0, time.UTC),
	)
	assert.NoError(t, err)
	assert.Equal(t, 4, count)

	// more recent run unaffected (even though it was parent)
	count, err = getCountInRange(
		rt.DB,
		getRunCount,
		orgs[2].ID,
		time.Date(2017, 12, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2018, 1, 1, 0, 0, 0, 0, time.UTC),
	)
	assert.NoError(t, err)
	assert.Equal(t, 1, count)

	// org 2 has a run that can't be archived because it's still active - as it has no existing archives
	// this will manifest itself as a monthly which fails to save
	dailiesCreated, dailiesFailed, monthliesCreated, monthliesFailed, _, err := ArchiveOrg(ctx, rt, now, orgs[1], RunType)
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

func TestArchiveActiveOrgs(t *testing.T) {
	_, rt := setup(t)

	err := ArchiveActiveOrgs(rt)
	assert.NoError(t, err)

}
