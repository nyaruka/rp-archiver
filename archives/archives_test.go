package archives

import (
	"compress/gzip"
	"context"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	_ "github.com/lib/pq"
	"github.com/nyaruka/gocommon/aws/cwatch"
	"github.com/nyaruka/gocommon/dbutil/assertdb"
	"github.com/nyaruka/rp-archiver/runtime"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vinovest/sqlx"
)

func setup(t *testing.T) (context.Context, *runtime.Runtime) {
	ctx := t.Context()
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

	s3Client, err := NewS3Client(config, false)
	require.NoError(t, err)

	if s3Client.Test(ctx, "temba-archives") != nil {
		_, err = s3Client.Client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("temba-archives")})
		require.NoError(t, err)
	}

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug})))

	CW, err := cwatch.NewService(config.AWSAccessKeyID, config.AWSSecretAccessKey, config.AWSRegion, config.CloudwatchNamespace, config.DeploymentID)
	require.NoError(t, err)

	t.Cleanup(func() {
		s3Client.EmptyBucket(ctx, "temba-archives")
	})

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
	assert.Equal(t, int64(0), task.Size)
	assert.Equal(t, "", string(task.Hash))

	DeleteArchiveTempFile(task)

	// build our third task, should have two messages
	task = tasks[2]
	err = CreateArchiveFile(ctx, rt.DB, task, "/tmp")
	assert.NoError(t, err)

	// should have two records, second will have attachments
	assert.Equal(t, 3, task.RecordCount)
	assert.Equal(t, int64(625), task.Size)
	assert.Equal(t, time.Date(2017, 8, 12, 0, 0, 0, 0, time.UTC), task.StartDate)
	assert.Equal(t, "dd2b8dc865524ceb7080e26358fbda15", string(task.Hash))
	assertArchiveFile(t, task, "messages1.jsonl")

	DeleteArchiveTempFile(task)
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
	assert.Equal(t, int64(328), task.Size)
	assert.Equal(t, "ab7b71efd543c7309a39d2292cc975aa", string(task.Hash))
	assertArchiveFile(t, task, "messages2.jsonl")

	DeleteArchiveTempFile(task)
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

	assert.Equal(t, string(truth), string(test))
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
	assert.Equal(t, int64(0), task.Size)
	assert.Equal(t, "", string(task.Hash))

	DeleteArchiveTempFile(task)

	task = tasks[2]
	err = CreateArchiveFile(ctx, rt.DB, task, "/tmp")
	assert.NoError(t, err)

	// should have two record
	assert.Equal(t, 3, task.RecordCount)
	assert.Equal(t, int64(578), task.Size)
	assert.Equal(t, "cd8ce82019986ac1f4ec1482aac7bca0", string(task.Hash))
	assertArchiveFile(t, task, "runs1.jsonl")

	DeleteArchiveTempFile(task)
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
	assert.Equal(t, "40abf2113ea7c25c5476ff3025d54b07", string(task.Hash))
	assertArchiveFile(t, task, "runs2.jsonl")

	DeleteArchiveTempFile(task)
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

	assertCount(t, rt.DB, 4, `SELECT count(*) from msgs_broadcast WHERE org_id = $1`, 2)

	dailiesCreated, dailiesFailed, monthliesCreated, monthliesFailed, deleted, err := ArchiveOrg(ctx, rt, now, orgs[1], MessageType)
	assert.NoError(t, err)

	assert.Equal(t, 61, len(dailiesCreated))
	assertArchive(t, dailiesCreated[0], time.Date(2017, 8, 10, 0, 0, 0, 0, time.UTC), DayPeriod, 0, 0, "")
	assertArchive(t, dailiesCreated[1], time.Date(2017, 8, 11, 0, 0, 0, 0, time.UTC), DayPeriod, 0, 0, "")
	assertArchive(t, dailiesCreated[2], time.Date(2017, 8, 12, 0, 0, 0, 0, time.UTC), DayPeriod, 3, 625, "dd2b8dc865524ceb7080e26358fbda15")
	assertArchive(t, dailiesCreated[3], time.Date(2017, 8, 13, 0, 0, 0, 0, time.UTC), DayPeriod, 1, 346, "1cb0a61e6484e2dbda89b8baab452b8c")
	assertArchive(t, dailiesCreated[4], time.Date(2017, 8, 14, 0, 0, 0, 0, time.UTC), DayPeriod, 0, 0, "")

	// empty archives should not have location set (not uploaded to S3)
	assert.Empty(t, dailiesCreated[0].Location)
	assert.Empty(t, dailiesCreated[1].Location)
	// non-empty archives should have location set
	assert.NotEmpty(t, dailiesCreated[2].Location)
	assert.NotEmpty(t, dailiesCreated[3].Location)
	// empty archive again
	assert.Empty(t, dailiesCreated[4].Location)

	assert.Equal(t, 0, len(dailiesFailed))

	assert.Equal(t, 2, len(monthliesCreated))
	assertArchive(t, monthliesCreated[0], time.Date(2017, 8, 1, 0, 0, 0, 0, time.UTC), MonthPeriod, 4, 669, "bb5126c95df1f6927a16dad976775fa3")
	assertArchive(t, monthliesCreated[1], time.Date(2017, 9, 1, 0, 0, 0, 0, time.UTC), MonthPeriod, 0, 0, "")

	// non-empty monthly should have location, empty monthly should not
	assert.NotEmpty(t, monthliesCreated[0].Location)
	assert.Empty(t, monthliesCreated[1].Location)

	assert.Equal(t, 0, len(monthliesFailed))

	// empty archives don't need deletion (nothing uploaded to S3)
	assert.False(t, dailiesCreated[0].NeedsDeletion)
	assert.False(t, dailiesCreated[1].NeedsDeletion)
	// non-empty archives need deletion
	assert.True(t, dailiesCreated[2].NeedsDeletion)
	assert.True(t, dailiesCreated[3].NeedsDeletion)
	// empty archive again
	assert.False(t, dailiesCreated[4].NeedsDeletion)

	// only non-empty archives need deletion, so deleted count should be less than total created
	// count non-empty archives that need deletion
	nonEmptyCount := 0
	for _, a := range dailiesCreated {
		if a.RecordCount > 0 {
			nonEmptyCount++
		}
	}
	for _, a := range monthliesCreated {
		if a.RecordCount > 0 {
			nonEmptyCount++
		}
	}
	assert.Equal(t, nonEmptyCount, len(deleted))
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
	assert.Equal(t, startDate, a.StartDate, "start date mismatch for archive")
	assert.Equal(t, period, a.Period, "period mismatch for archive")
	assert.Equal(t, recordCount, a.RecordCount, "record count mismatch for archive")
	assert.Equal(t, size, a.Size, "size mismatch for archive")
	assert.Equal(t, hash, string(a.Hash), "hash mismatch for archive")
}

func TestArchiveOrgRuns(t *testing.T) {
	ctx, rt := setup(t)

	orgs, err := GetActiveOrgs(ctx, rt)
	assert.NoError(t, err)
	now := time.Date(2018, 1, 8, 12, 30, 0, 0, time.UTC)

	dailiesCreated, _, monthliesCreated, _, deleted, err := ArchiveOrg(ctx, rt, now, orgs[2], RunType)
	assert.NoError(t, err)

	assert.Equal(t, 10, len(dailiesCreated))
	assertArchive(t, dailiesCreated[0], time.Date(2017, 10, 1, 0, 0, 0, 0, time.UTC), DayPeriod, 0, 0, "")
	assertArchive(t, dailiesCreated[9], time.Date(2017, 10, 10, 0, 0, 0, 0, time.UTC), DayPeriod, 2, 1953, "95475b968ceff15f2f90d539e1bd3d20")

	assert.Equal(t, 2, len(monthliesCreated))
	assertArchive(t, monthliesCreated[0], time.Date(2017, 8, 1, 0, 0, 0, 0, time.UTC), MonthPeriod, 1, 465, "40abf2113ea7c25c5476ff3025d54b07")
	assertArchive(t, monthliesCreated[1], time.Date(2017, 9, 1, 0, 0, 0, 0, time.UTC), MonthPeriod, 0, 0, "")

	// only non-empty archives need deletion, so deleted count should be less than total created
	assert.Equal(t, 2, len(deleted))
	assert.Equal(t, monthliesCreated[0].ID, deleted[0].ID)
	assert.Equal(t, dailiesCreated[9].ID, deleted[1].ID)

	assertdb.Query(t, rt.DB, "SELECT count(*) FROM archives_archive").Returns(16)
	assertdb.Query(t, rt.DB, "SELECT count(*) FROM archives_archive WHERE location IS NOT NULL AND hash IS NOT NULL AND size > 0").Returns(6) // 2 new, 4 existing
	assertdb.Query(t, rt.DB, "SELECT count(*) FROM archives_archive WHERE location IS NULL AND hash IS NULL AND size = 0").Returns(10)

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

	// org 2 will create backfilled monthlies for 2017-08 and 2017-09.. and then only dailies for 2017-10-01 to 2017-10-10
	dailiesCreated, dailiesFailed, monthliesCreated, monthliesFailed, _, err := ArchiveOrg(ctx, rt, now, orgs[1], RunType)
	assert.NoError(t, err)

	assert.Equal(t, 10, len(dailiesCreated))
	assertArchive(t, dailiesCreated[0], time.Date(2017, 10, 1, 0, 0, 0, 0, time.UTC), DayPeriod, 0, 0, "")

	assert.Equal(t, 2, len(monthliesCreated))
	assertArchive(t, monthliesCreated[0], time.Date(2017, 8, 1, 0, 0, 0, 0, time.UTC), MonthPeriod, 4, 692, "98a8149eb3dbc1762368b78fcae86d24")

	assert.Equal(t, 0, len(dailiesFailed))
	assert.Equal(t, 0, len(monthliesFailed))
}

func TestArchiveActiveOrgs(t *testing.T) {
	_, rt := setup(t)

	err := ArchiveActiveOrgs(rt)
	assert.NoError(t, err)

}
