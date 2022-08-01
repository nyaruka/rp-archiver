package archives

import (
	"bufio"
	"compress/gzip"
	"context"
	"crypto/md5"
	"database/sql"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"github.com/nyaruka/gocommon/analytics"
	"github.com/nyaruka/gocommon/dates"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// ArchiveType is the type for the archives
type ArchiveType string

const (
	// RunType for run archives
	RunType = ArchiveType("run")

	// MessageType for message archives
	MessageType = ArchiveType("message")

	// SessionType for session archives
	SessionType = ArchiveType("session")
)

// ArchivePeriod is the period of data in the archive
type ArchivePeriod string

const (
	// DayPeriod id the period of a day (24 hours) from archive start date
	DayPeriod = ArchivePeriod("D")

	// MonthPeriod is the period of a month from archive start date
	MonthPeriod = ArchivePeriod("M")
)

// Org represents the model for an org
type Org struct {
	ID              int       `db:"id"`
	Name            string    `db:"name"`
	CreatedOn       time.Time `db:"created_on"`
	IsAnon          bool      `db:"is_anon"`
	RetentionPeriod int
}

// Archive represents the model for an archive
type Archive struct {
	ID          int         `db:"id"`
	ArchiveType ArchiveType `db:"archive_type"`
	OrgID       int         `db:"org_id"`
	CreatedOn   time.Time   `db:"created_on"`

	StartDate time.Time     `db:"start_date"`
	Period    ArchivePeriod `db:"period"`

	RecordCount int    `db:"record_count"`
	Size        int64  `db:"size"`
	Hash        string `db:"hash"`
	URL         string `db:"url"`
	BuildTime   int    `db:"build_time"`

	NeedsDeletion bool       `db:"needs_deletion"`
	DeletedOn     *time.Time `db:"deleted_date"`
	Rollup        *int       `db:"rollup_id"`

	Org         Org
	ArchiveFile string
	Dailies     []*Archive
}

func (a *Archive) endDate() time.Time {
	endDate := a.StartDate
	if a.Period == DayPeriod {
		endDate = endDate.AddDate(0, 0, 1)
	} else {
		endDate = endDate.AddDate(0, 1, 0)
	}
	return endDate
}

const lookupActiveOrgs = `
SELECT o.id, o.name, o.created_on, o.is_anon 
FROM orgs_org o 
WHERE o.is_active = TRUE order by o.id
`

// GetActiveOrgs returns the active organizations sorted by id
func GetActiveOrgs(ctx context.Context, db *sqlx.DB, conf *Config) ([]Org, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	rows, err := db.QueryxContext(ctx, lookupActiveOrgs)
	if err != nil {
		return nil, errors.Wrapf(err, "error fetching active orgs")
	}
	defer rows.Close()

	orgs := make([]Org, 0, 10)
	for rows.Next() {
		org := Org{RetentionPeriod: conf.RetentionPeriod}
		err = rows.StructScan(&org)
		if err != nil {
			return nil, errors.Wrapf(err, "error scanning active org")
		}
		orgs = append(orgs, org)
	}

	return orgs, nil
}

const lookupOrgArchives = `
SELECT id, org_id, start_date::timestamp with time zone as start_date, period, archive_type, hash, size, record_count, url, rollup_id, needs_deletion
FROM archives_archive WHERE org_id = $1 AND archive_type = $2 
ORDER BY start_date asc, period desc
`

// GetCurrentArchives returns all the current archives for the passed in org and record type
func GetCurrentArchives(ctx context.Context, db *sqlx.DB, org Org, archiveType ArchiveType) ([]*Archive, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	archives := make([]*Archive, 0, 1)
	err := db.SelectContext(ctx, &archives, lookupOrgArchives, org.ID, archiveType)
	if err != nil && err != sql.ErrNoRows {
		return nil, errors.Wrapf(err, "error selecting current archives for org: %d and type: %s", org.ID, archiveType)
	}

	return archives, nil
}

const lookupArchivesNeedingDeletion = `
SELECT id, org_id, start_date::timestamp with time zone as start_date, period, archive_type, hash, size, record_count, url, rollup_id, needs_deletion 
FROM archives_archive WHERE org_id = $1 AND archive_type = $2 AND needs_deletion = TRUE
ORDER BY start_date asc, period desc
`

// GetArchivesNeedingDeletion returns all the archives which need to be deleted
func GetArchivesNeedingDeletion(ctx context.Context, db *sqlx.DB, org Org, archiveType ArchiveType) ([]*Archive, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	archives := make([]*Archive, 0, 1)
	err := db.SelectContext(ctx, &archives, lookupArchivesNeedingDeletion, org.ID, archiveType)
	if err != nil && err != sql.ErrNoRows {
		return nil, errors.Wrapf(err, "error selecting archives needing deletion for org: %d and type: %s", org.ID, archiveType)
	}

	return archives, nil
}

const lookupCountOrgArchives = `
SELECT count(id) 
FROM archives_archive 
WHERE org_id = $1 AND archive_type = $2
`

// GetCurrentArchiveCount returns the archive count for the passed in org and record type
func GetCurrentArchiveCount(ctx context.Context, db *sqlx.DB, org Org, archiveType ArchiveType) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	var archiveCount int

	err := db.GetContext(ctx, &archiveCount, lookupCountOrgArchives, org.ID, archiveType)
	if err != nil {
		return 0, errors.Wrapf(err, "error querying archive count for org: %d and type: %s", org.ID, archiveType)
	}

	return archiveCount, nil
}

// between is inclusive on both sides
const lookupOrgDailyArchivesForDateRange = `
SELECT id, start_date::timestamp with time zone as start_date, period, archive_type, hash, size, record_count, url, rollup_id
FROM archives_archive
WHERE org_id = $1 AND archive_type = $2 AND period = $3 AND start_date BETWEEN $4 AND $5
ORDER BY start_date asc
`

// GetDailyArchivesForDateRange returns all the current archives for the passed in org and record type and date range
func GetDailyArchivesForDateRange(ctx context.Context, db *sqlx.DB, org Org, archiveType ArchiveType, startDate time.Time, endDate time.Time) ([]*Archive, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	existingArchives := make([]*Archive, 0, 1)

	err := db.SelectContext(ctx, &existingArchives, lookupOrgDailyArchivesForDateRange, org.ID, archiveType, DayPeriod, startDate, endDate)
	if err != nil && err != sql.ErrNoRows {
		return nil, errors.Wrapf(err, "error selecting daily archives for org: %d and type: %s", org.ID, archiveType)
	}

	return existingArchives, nil
}

// GetMissingDailyArchives calculates what archives need to be generated for the passed in org this is calculated per day
func GetMissingDailyArchives(ctx context.Context, db *sqlx.DB, now time.Time, org Org, archiveType ArchiveType) ([]*Archive, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	// our first archive would be active days from today
	endDate := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC).AddDate(0, 0, -org.RetentionPeriod)
	orgUTC := org.CreatedOn.In(time.UTC)
	startDate := time.Date(orgUTC.Year(), orgUTC.Month(), orgUTC.Day(), 0, 0, 0, 0, time.UTC)

	return GetMissingDailyArchivesForDateRange(ctx, db, startDate, endDate, org, archiveType)
}

const lookupMissingDailyArchive = `
WITH month_days(missing_day) AS (
  select GENERATE_SERIES($1::timestamp with time zone, $2::timestamp with time zone, '1 day')::date
), curr_archives AS (
  SELECT start_date FROM archives_archive WHERE org_id = $3 AND period = $4 AND archive_type=$5
UNION DISTINCT
  -- also get the overlapping days for the monthly rolled up archives
  SELECT GENERATE_SERIES(start_date, (start_date + '1 month'::interval) - '1 second'::interval, '1 day')::date AS start_date
  FROM archives_archive WHERE org_id = $3 AND period = 'M' AND archive_type=$5
)
SELECT missing_day::timestamp WITH TIME ZONE FROM month_days LEFT JOIN curr_archives ON curr_archives.start_date = month_days.missing_day
WHERE curr_archives.start_date IS NULL
`

// GetMissingDailyArchivesForDateRange returns all them missing daily archives between the two passed in date ranges
func GetMissingDailyArchivesForDateRange(ctx context.Context, db *sqlx.DB, startDate time.Time, endDate time.Time, org Org, archiveType ArchiveType) ([]*Archive, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	missing := make([]*Archive, 0, 1)

	rows, err := db.QueryxContext(ctx, lookupMissingDailyArchive, startDate, endDate, org.ID, DayPeriod, archiveType)
	if err != nil {
		return nil, errors.Wrapf(err, "error getting missing daily archives for org: %d and type: %s", org.ID, archiveType)
	}
	defer rows.Close()

	var missingDay time.Time
	for rows.Next() {

		err = rows.Scan(&missingDay)
		if err != nil {
			return nil, errors.Wrapf(err, "error scanning missing daily archive for org: %d and type: %s", org.ID, archiveType)
		}
		archive := Archive{
			Org:         org,
			OrgID:       org.ID,
			StartDate:   missingDay,
			ArchiveType: archiveType,
			Period:      DayPeriod,
		}

		missing = append(missing, &archive)
	}

	return missing, nil
}

// startDate is truncated to the first of the month
// endDate for range is not inclusive so we must deduct 1 second
const lookupMissingMonthlyArchive = `
WITH month_days(missing_month) AS (
  SELECT generate_series(date_trunc('month', $1::timestamp with time zone), $2::timestamp with time zone - '1 second'::interval, '1 month')::date
), curr_archives AS (
  SELECT start_date FROM archives_archive WHERE org_id = $3 and period = $4 and archive_type=$5
)
SELECT missing_month::timestamp with time zone from month_days LEFT JOIN curr_archives ON curr_archives.start_date = month_days.missing_month
WHERE curr_archives.start_date IS NULL
`

// GetMissingMonthlyArchives gets which montly archives are currently missing for this org
func GetMissingMonthlyArchives(ctx context.Context, db *sqlx.DB, now time.Time, org Org, archiveType ArchiveType) ([]*Archive, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Minute*5)
	defer cancel()

	lastActive := now.AddDate(0, 0, -org.RetentionPeriod)
	endDate := time.Date(lastActive.Year(), lastActive.Month(), 1, 0, 0, 0, 0, time.UTC)

	orgUTC := org.CreatedOn.In(time.UTC)
	startDate := time.Date(orgUTC.Year(), orgUTC.Month(), 1, 0, 0, 0, 0, time.UTC)

	missing := make([]*Archive, 0, 1)

	rows, err := db.QueryxContext(ctx, lookupMissingMonthlyArchive, startDate, endDate, org.ID, MonthPeriod, archiveType)
	if err != nil {
		return nil, errors.Wrapf(err, "error getting missing monthly archive for org: %d and type: %s", org.ID, archiveType)
	}
	defer rows.Close()

	var missingMonth time.Time
	for rows.Next() {

		err = rows.Scan(&missingMonth)
		if err != nil {
			return nil, errors.Wrapf(err, "error scanning missing monthly archive for org: %d and type: %s", org.ID, archiveType)
		}
		archive := Archive{
			Org:         org,
			OrgID:       org.ID,
			StartDate:   missingMonth,
			ArchiveType: archiveType,
			Period:      MonthPeriod,
		}

		missing = append(missing, &archive)
	}

	return missing, nil
}

// BuildRollupArchive builds a monthly archive from the files present on S3
func BuildRollupArchive(ctx context.Context, db *sqlx.DB, conf *Config, s3Client s3iface.S3API, monthlyArchive *Archive, now time.Time, org Org, archiveType ArchiveType) error {
	ctx, cancel := context.WithTimeout(ctx, time.Hour)
	defer cancel()

	start := dates.Now()

	// figure out the first day in the monthlyArchive we'll archive
	startDate := monthlyArchive.StartDate
	endDate := startDate.AddDate(0, 1, 0).Add(time.Nanosecond * -1)
	if monthlyArchive.StartDate.Before(org.CreatedOn) {
		orgUTC := org.CreatedOn.In(time.UTC)
		startDate = time.Date(orgUTC.Year(), orgUTC.Month(), orgUTC.Day(), 0, 0, 0, 0, time.UTC)
	}

	// grab all the daily archives we need
	missingDailies, err := GetMissingDailyArchivesForDateRange(ctx, db, startDate, endDate, org, archiveType)
	if err != nil {
		return err
	}

	if len(missingDailies) != 0 {
		return fmt.Errorf("missing %d daily archives", len(missingDailies))
	}

	// great, we have all the dailies we need, download them
	filename := fmt.Sprintf("%s_%d_%s_%d_%02d_", monthlyArchive.ArchiveType, monthlyArchive.Org.ID, monthlyArchive.Period, monthlyArchive.StartDate.Year(), monthlyArchive.StartDate.Month())
	file, err := ioutil.TempFile(conf.TempDir, filename)
	if err != nil {
		return errors.Wrapf(err, "error creating temp file: %s", filename)
	}
	writerHash := md5.New()
	gzWriter := gzip.NewWriter(io.MultiWriter(file, writerHash))
	writer := bufio.NewWriter(gzWriter)
	defer file.Close()

	recordCount := 0

	dailies, err := GetDailyArchivesForDateRange(ctx, db, org, archiveType, startDate, endDate)
	if err != nil {
		return err
	}

	// calculate total expected size
	estimatedSize := int64(0)
	for _, d := range dailies {
		estimatedSize += d.Size
	}

	// for each daily
	for _, daily := range dailies {
		// if there are no records in this daily, just move on
		if daily.RecordCount == 0 {
			continue
		}

		reader, err := GetS3File(ctx, s3Client, daily.URL)
		if err != nil {
			return errors.Wrapf(err, "error reading S3 URL: %s", daily.URL)
		}

		// set up our reader to calculate our hash along the way
		readerHash := md5.New()
		teeReader := io.TeeReader(reader, readerHash)
		gzipReader, err := gzip.NewReader(teeReader)
		if err != nil {
			return errors.Wrapf(err, "error creating gzip reader")
		}

		// copy this daily file (uncompressed) to our new monthly file
		_, err = io.Copy(writer, gzipReader)
		if err != nil {
			return errors.Wrapf(err, "error copying from s3 to disk for URL: %s", daily.URL)
		}

		reader.Close()
		gzipReader.Close()

		// check our hash that everything was written out
		hash := hex.EncodeToString(readerHash.Sum(nil))
		if hash != daily.Hash {
			return fmt.Errorf("daily hash mismatch. expected: %s, got %s", daily.Hash, hash)
		}

		recordCount += daily.RecordCount
	}

	monthlyArchive.ArchiveFile = file.Name()
	err = writer.Flush()
	if err != nil {
		return err
	}

	err = gzWriter.Close()
	if err != nil {
		return err
	}

	// calculate our size and hash
	monthlyArchive.Hash = hex.EncodeToString(writerHash.Sum(nil))
	stat, err := file.Stat()
	if err != nil {
		return errors.Wrapf(err, "error statting file: %s", file.Name())
	}
	monthlyArchive.Size = stat.Size()
	monthlyArchive.RecordCount = recordCount
	monthlyArchive.BuildTime = int(dates.Since(start) / time.Millisecond)
	monthlyArchive.Dailies = dailies
	monthlyArchive.NeedsDeletion = false

	return nil
}

// EnsureTempArchiveDirectory checks that we can write to our archive directory, creating it first if needbe
func EnsureTempArchiveDirectory(path string) error {
	if len(path) == 0 {
		return fmt.Errorf("path argument cannot be empty")
	}

	// check if path is a directory we can write to
	fileInfo, err := os.Stat(path)
	if os.IsNotExist(err) {
		return os.MkdirAll(path, 0700)
	} else if err != nil {
		return errors.Wrapf(err, "error statting temp dir: %s", path)
	}

	// is path a directory
	if !fileInfo.IsDir() {
		return fmt.Errorf("path '%s' is not a directory", path)
	}

	testFilePath := filepath.Join(path, ".test_file")
	testFile, err := os.Create(testFilePath)
	if err != nil {
		return fmt.Errorf("directory '%s' is not writable", path)
	}

	defer testFile.Close()

	err = os.Remove(testFilePath)
	return err
}

// CreateArchiveFile is responsible for writing an archive file for the passed in archive from our database
func CreateArchiveFile(ctx context.Context, db *sqlx.DB, archive *Archive, archivePath string) error {
	ctx, cancel := context.WithTimeout(ctx, time.Hour*3)
	defer cancel()

	start := dates.Now()

	log := logrus.WithFields(logrus.Fields{
		"org_id":       archive.Org.ID,
		"archive_type": archive.ArchiveType,
		"start_date":   archive.StartDate,
		"end_date":     archive.endDate(),
		"period":       archive.Period,
	})

	filename := fmt.Sprintf("%s_%d_%s%d%02d%02d_", archive.ArchiveType, archive.Org.ID, archive.Period, archive.StartDate.Year(), archive.StartDate.Month(), archive.StartDate.Day())
	file, err := ioutil.TempFile(archivePath, filename)
	if err != nil {
		return errors.Wrapf(err, "error creating temp file: %s", filename)
	}

	defer func() {
		// we only set the archive filename when we succeed
		if archive.ArchiveFile == "" {
			err = os.Remove(file.Name())
			if err != nil {
				log.WithError(err).WithField("filename", file.Name()).Error("error cleaning up archive file")
			}
		}
	}()

	hash := md5.New()
	gzWriter := gzip.NewWriter(io.MultiWriter(file, hash))
	writer := bufio.NewWriter(gzWriter)
	defer file.Close()

	log.WithFields(logrus.Fields{
		"filename": file.Name(),
	}).Debug("creating new archive file")

	recordCount := 0
	switch archive.ArchiveType {
	case MessageType:
		recordCount, err = writeMessageRecords(ctx, db, archive, writer)
	case RunType:
		recordCount, err = writeRunRecords(ctx, db, archive, writer)
	default:
		err = fmt.Errorf("unknown archive type: %s", archive.ArchiveType)
	}

	if err != nil {
		return errors.Wrapf(err, "error writing archive")
	}

	err = writer.Flush()
	if err != nil {
		return errors.Wrapf(err, "error flushing archive file")
	}

	err = gzWriter.Close()
	if err != nil {
		return errors.Wrapf(err, "error closing archive gzip writer")
	}

	// calculate our size and hash
	archive.Hash = hex.EncodeToString(hash.Sum(nil))
	stat, err := file.Stat()
	if err != nil {
		return errors.Wrapf(err, "error calculating archive hash")
	}

	if stat.Size() > 5e9 {
		return fmt.Errorf("archive too large, must be smaller than 5 gigs, build dailies if possible")
	}

	archive.ArchiveFile = file.Name()
	archive.Size = stat.Size()
	archive.RecordCount = recordCount
	archive.BuildTime = int(dates.Since(start) / time.Millisecond)

	log.WithFields(logrus.Fields{
		"record_count": recordCount,
		"filename":     file.Name(),
		"file_size":    archive.Size,
		"file_hash":    archive.Hash,
		"elapsed":      dates.Since(start),
	}).Debug("completed writing archive file")

	return nil
}

// UploadArchive uploads the passed archive file to S3
func UploadArchive(ctx context.Context, s3Client s3iface.S3API, bucket string, archive *Archive) error {
	ctx, cancel := context.WithTimeout(ctx, time.Minute*15)
	defer cancel()

	archivePath := ""
	if archive.Period == DayPeriod {
		archivePath = fmt.Sprintf(
			"/%d/%s_%s%d%02d%02d_%s.jsonl.gz",
			archive.Org.ID, archive.ArchiveType, archive.Period,
			archive.StartDate.Year(), archive.StartDate.Month(), archive.StartDate.Day(),
			archive.Hash)
	} else {
		archivePath = fmt.Sprintf(
			"/%d/%s_%s%d%02d_%s.jsonl.gz",
			archive.Org.ID, archive.ArchiveType, archive.Period,
			archive.StartDate.Year(), archive.StartDate.Month(),
			archive.Hash)
	}

	err := UploadToS3(ctx, s3Client, bucket, archivePath, archive)
	if err != nil {
		return errors.Wrapf(err, "error uploading archive to S3")
	}

	archive.NeedsDeletion = true

	logrus.WithFields(logrus.Fields{
		"org_id":       archive.Org.ID,
		"archive_type": archive.ArchiveType,
		"start_date":   archive.StartDate,
		"period":       archive.Period,
		"url":          archive.URL,
		"file_size":    archive.Size,
		"file_hash":    archive.Hash,
	}).Debug("completed uploading archive file")

	return nil
}

const insertArchive = `
INSERT INTO archives_archive(archive_type, org_id, created_on, start_date, period, record_count, size, hash, url, needs_deletion, build_time, rollup_id)
VALUES(:archive_type, :org_id, :created_on, :start_date, :period, :record_count, :size, :hash, :url, :needs_deletion, :build_time, :rollup_id)
RETURNING id
`

// WriteArchiveToDB write an archive to the Database
func WriteArchiveToDB(ctx context.Context, db *sqlx.DB, archive *Archive) error {
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	archive.OrgID = archive.Org.ID
	archive.CreatedOn = dates.Now()

	tx, err := db.BeginTxx(ctx, nil)
	if err != nil {
		return errors.Wrapf(err, "error starting transaction")
	}

	rows, err := tx.NamedQuery(insertArchive, archive)
	if err != nil {
		tx.Rollback()
		return errors.Wrapf(err, "error inserting archive")
	}

	rows.Next()
	err = rows.Scan(&archive.ID)
	if err != nil {
		tx.Rollback()
		return errors.Wrapf(err, "error reading new archive id")
	}
	rows.Close()

	// if we have children to update do so
	if len(archive.Dailies) > 0 {
		// build our list of ids
		childIDs := make([]int, 0, len(archive.Dailies))
		for _, c := range archive.Dailies {
			childIDs = append(childIDs, c.ID)
		}

		result, err := tx.ExecContext(ctx, `UPDATE archives_archive SET rollup_id = $1 WHERE id = ANY($2)`, archive.ID, pq.Array(childIDs))
		if err != nil {
			tx.Rollback()
			return errors.Wrapf(err, "error updating rollup ids")
		}
		affected, err := result.RowsAffected()
		if err != nil {
			tx.Rollback()
			return errors.Wrapf(err, "error getting number of rollup ids updated")
		}
		if int(affected) != len(childIDs) {
			tx.Rollback()
			return fmt.Errorf("mismatch in number of children updated and number of rows updated")
		}
	}

	err = tx.Commit()
	if err != nil {
		tx.Rollback()
		return errors.Wrapf(err, "error committing new archive transaction")
	}
	return nil
}

// DeleteArchiveFile removes our own disk archive file
func DeleteArchiveFile(archive *Archive) error {
	if archive.ArchiveFile == "" {
		return nil
	}

	err := os.Remove(archive.ArchiveFile)

	if err != nil {
		return errors.Wrapf(err, "error deleting temp archive file: %s", archive.ArchiveFile)
	}

	logrus.WithFields(logrus.Fields{
		"org_id":        archive.Org.ID,
		"archive_type":  archive.ArchiveType,
		"start_date":    archive.StartDate,
		"periond":       archive.Period,
		"db_archive_id": archive.ID,
		"filename":      archive.ArchiveFile,
	}).Debug("deleted temporary archive file")
	return nil
}

// CreateOrgArchives builds all the missing archives for the passed in org
func CreateOrgArchives(ctx context.Context, now time.Time, config *Config, db *sqlx.DB, s3Client s3iface.S3API, org Org, archiveType ArchiveType) ([]*Archive, []*Archive, []*Archive, []*Archive, error) {
	archiveCount, err := GetCurrentArchiveCount(ctx, db, org, archiveType)
	if err != nil {
		return nil, nil, nil, nil, errors.Wrapf(err, "error getting current archive count")
	}

	var dailiesCreated, dailiesFailed, monthliesCreated, monthliesFailed []*Archive

	// no existing archives means this might be a backfill, figure out if there are full months we can build first
	if archiveCount == 0 {
		archives, err := GetMissingMonthlyArchives(ctx, db, now, org, archiveType)
		if err != nil {
			return nil, nil, nil, nil, errors.Wrapf(err, "error getting missing monthly archives")
		}

		// we first create monthly archives
		monthliesCreated, monthliesFailed = createArchives(ctx, db, config, s3Client, org, archives)
	}

	// then add in daily archives taking into account the monthly that have been built
	daily, err := GetMissingDailyArchives(ctx, db, now, org, archiveType)
	if err != nil {
		return nil, nil, nil, nil, errors.Wrapf(err, "error getting missing daily archives")
	}

	// we then create missing daily archives
	dailiesCreated, dailiesFailed = createArchives(ctx, db, config, s3Client, org, daily)

	defer ctx.Done()

	return dailiesCreated, dailiesFailed, monthliesCreated, monthliesFailed, nil
}

func createArchive(ctx context.Context, db *sqlx.DB, config *Config, s3Client s3iface.S3API, archive *Archive) error {
	err := CreateArchiveFile(ctx, db, archive, config.TempDir)
	if err != nil {
		return errors.Wrap(err, "error writing archive file")
	}

	defer func() {
		if !config.KeepFiles {
			err := DeleteArchiveFile(archive)
			if err != nil {
				logrus.WithError(err).Error("error deleting temporary archive file")
			}
		}
	}()

	if config.UploadToS3 {
		err = UploadArchive(ctx, s3Client, config.S3Bucket, archive)
		if err != nil {
			return errors.Wrap(err, "error writing archive to s3")
		}
	}

	err = WriteArchiveToDB(ctx, db, archive)
	if err != nil {
		return errors.Wrap(err, "error writing record to db")
	}

	return nil
}

func createArchives(ctx context.Context, db *sqlx.DB, config *Config, s3Client s3iface.S3API, org Org, archives []*Archive) ([]*Archive, []*Archive) {
	log := logrus.WithFields(logrus.Fields{"org_id": org.ID, "org_name": org.Name})

	created := make([]*Archive, 0, len(archives))
	failed := make([]*Archive, 0, 5)

	for _, archive := range archives {
		log.WithFields(logrus.Fields{"start_date": archive.StartDate, "end_date": archive.endDate(), "period": archive.Period, "archive_type": archive.ArchiveType}).Debug("starting archive")
		start := dates.Now()

		err := createArchive(ctx, db, config, s3Client, archive)
		if err != nil {
			log.WithError(err).Error("error creating archive")
			failed = append(failed, archive)
		} else {
			log.WithFields(logrus.Fields{"id": archive.ID, "record_count": archive.RecordCount, "elapsed": dates.Since(start)}).Debug("archive complete")
			created = append(created, archive)
		}
	}

	return created, failed
}

// RollupOrgArchives rolls up monthly archives from our daily archives
func RollupOrgArchives(ctx context.Context, now time.Time, config *Config, db *sqlx.DB, s3Client s3iface.S3API, org Org, archiveType ArchiveType) ([]*Archive, []*Archive, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Hour*3)
	defer cancel()

	log := logrus.WithFields(logrus.Fields{"org_id": org.ID, "org_name": org.Name, "archive_type": archiveType})

	// get our missing monthly archives
	archives, err := GetMissingMonthlyArchives(ctx, db, now, org, archiveType)
	if err != nil {
		return nil, nil, err
	}

	created := make([]*Archive, 0, len(archives))
	failed := make([]*Archive, 0, 1)

	// build them from rollups
	for _, archive := range archives {
		log := log.WithFields(logrus.Fields{"start_date": archive.StartDate})
		start := dates.Now()

		err = BuildRollupArchive(ctx, db, config, s3Client, archive, now, org, archiveType)
		if err != nil {
			log.WithError(err).Error("error building monthly archive")
			failed = append(failed, archive)
			continue
		}

		if config.UploadToS3 {
			err = UploadArchive(ctx, s3Client, config.S3Bucket, archive)
			if err != nil {
				log.WithError(err).Error("error writing archive to s3")
				failed = append(failed, archive)
				continue
			}
		}

		err = WriteArchiveToDB(ctx, db, archive)
		if err != nil {
			log.WithError(err).Error("error writing record to db")
			failed = append(failed, archive)
			continue
		}

		if !config.KeepFiles {
			err := DeleteArchiveFile(archive)
			if err != nil {
				log.WithError(err).Error("error deleting temporary file")
				continue
			}
		}

		log.WithFields(logrus.Fields{"id": archive.ID, "record_count": archive.RecordCount, "elapsed": dates.Since(start)}).Info("rollup created")
		created = append(created, archive)
	}

	return created, failed, nil
}

const setArchiveDeleted = `
UPDATE archives_archive 
SET needs_deletion = FALSE, deleted_on = $2
WHERE id = $1
`

var deleteTransactionSize = 100

// DeleteArchivedOrgRecords deletes all the records for the given org based on archives already created
func DeleteArchivedOrgRecords(ctx context.Context, now time.Time, config *Config, db *sqlx.DB, s3Client s3iface.S3API, org Org, archiveType ArchiveType) ([]*Archive, error) {
	// get all the archives that haven't yet been deleted
	archives, err := GetArchivesNeedingDeletion(ctx, db, org, archiveType)
	if err != nil {
		return nil, fmt.Errorf("error finding archives needing deletion '%s'", archiveType)
	}

	// for each archive
	deleted := make([]*Archive, 0, len(archives))
	for _, a := range archives {
		log := logrus.WithFields(logrus.Fields{
			"archive_id": a.ID,
			"org_id":     a.OrgID,
			"type":       a.ArchiveType,
			"count":      a.RecordCount,
			"start":      a.StartDate,
			"period":     a.Period,
		})

		start := dates.Now()

		switch a.ArchiveType {
		case MessageType:
			err = DeleteArchivedMessages(ctx, config, db, s3Client, a)
			if err == nil {
				err = DeleteBroadcasts(ctx, now, config, db, org)
			}

		case RunType:
			err = DeleteArchivedRuns(ctx, config, db, s3Client, a)
		default:
			err = fmt.Errorf("unknown archive type: %s", a.ArchiveType)
		}

		if err != nil {
			log.WithError(err).Error("error deleting archive")
			continue
		}

		deleted = append(deleted, a)
		log.WithFields(logrus.Fields{"elapsed": dates.Since(start)}).Info("deleted archive records")
	}

	return deleted, nil
}

// ArchiveOrg looks for any missing archives for the passed in org, creating and uploading them as necessary, returning the created archives
func ArchiveOrg(ctx context.Context, now time.Time, cfg *Config, db *sqlx.DB, s3Client s3iface.S3API, org Org, archiveType ArchiveType) ([]*Archive, []*Archive, []*Archive, []*Archive, []*Archive, error) {
	log := logrus.WithFields(logrus.Fields{"org_id": org.ID, "org_name": org.Name})
	start := dates.Now()

	dailiesCreated, dailiesFailed, monthliesCreated, monthliesFailed, err := CreateOrgArchives(ctx, now, cfg, db, s3Client, org, archiveType)
	if err != nil {
		return nil, nil, nil, nil, nil, errors.Wrapf(err, "error creating archives")
	}

	if len(dailiesCreated) > 0 {
		elapsed := dates.Since(start)
		rate := float32(countRecords(dailiesCreated)) / (float32(elapsed) / float32(time.Second))
		log.WithFields(logrus.Fields{"elapsed": elapsed, "records_per_second": rate}).Info("completed archival for org")
	}

	rollupsCreated, rollupsFailed, err := RollupOrgArchives(ctx, now, cfg, db, s3Client, org, archiveType)
	if err != nil {
		return nil, nil, nil, nil, nil, errors.Wrapf(err, "error rolling up archives")
	}

	monthliesCreated = append(monthliesCreated, rollupsCreated...)
	monthliesFailed = append(monthliesFailed, rollupsFailed...)
	monthliesFailed = removeDuplicates(monthliesFailed) // don't double report monthlies that fail being built from db and rolled up from dailies

	// finally delete any archives not yet actually archived
	var deleted []*Archive
	if cfg.Delete {
		deleted, err = DeleteArchivedOrgRecords(ctx, now, cfg, db, s3Client, org, archiveType)
		if err != nil {
			return dailiesCreated, dailiesFailed, monthliesCreated, monthliesFailed, nil, errors.Wrapf(err, "error deleting archived records")
		}
	}

	return dailiesCreated, dailiesFailed, monthliesCreated, monthliesFailed, deleted, nil
}

// ArchiveActiveOrgs fetches active orgs and archives messages and runs
func ArchiveActiveOrgs(db *sqlx.DB, cfg *Config, s3Client s3iface.S3API) error {
	start := dates.Now()

	// get our active orgs
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	orgs, err := GetActiveOrgs(ctx, db, cfg)
	cancel()

	if err != nil {
		return errors.Wrap(err, "error getting active orgs")
	}

	totalRunsRecordsArchived, totalMsgsRecordsArchived := 0, 0
	totalRunsArchivesCreated, totalMsgsArchivesCreated := 0, 0
	totalRunsArchivesFailed, totalMsgsArchivesFailed := 0, 0
	totalRunsRollupsCreated, totalMsgsRollupsCreated := 0, 0
	totalRunsRollupsFailed, totalMsgsRollupsFailed := 0, 0

	// for each org, do our export
	for _, org := range orgs {
		// no single org should take more than 12 hours
		ctx, cancel := context.WithTimeout(context.Background(), time.Hour*12)
		log := logrus.WithField("org_id", org.ID).WithField("org_name", org.Name)

		if cfg.ArchiveMessages {
			dailiesCreated, dailiesFailed, monthliesCreated, monthliesFailed, _, err := ArchiveOrg(ctx, start, cfg, db, s3Client, org, MessageType)
			if err != nil {
				log.WithError(err).WithField("archive_type", MessageType).Error("error archiving org messages")
			}
			totalMsgsRecordsArchived += countRecords(dailiesCreated)
			totalMsgsArchivesCreated += len(dailiesCreated)
			totalMsgsArchivesFailed += len(dailiesFailed)
			totalMsgsRollupsCreated += len(monthliesCreated)
			totalMsgsRollupsFailed += len(monthliesFailed)
		}
		if cfg.ArchiveRuns {
			dailiesCreated, dailiesFailed, monthliesCreated, monthliesFailed, _, err := ArchiveOrg(ctx, start, cfg, db, s3Client, org, RunType)
			if err != nil {
				log.WithError(err).WithField("archive_type", RunType).Error("error archiving org runs")
			}
			totalRunsRecordsArchived += countRecords(dailiesCreated)
			totalRunsArchivesCreated += len(dailiesCreated)
			totalRunsArchivesFailed += len(dailiesFailed)
			totalRunsRollupsCreated += len(monthliesCreated)
			totalRunsRollupsFailed += len(monthliesFailed)
		}

		cancel()
	}

	timeTaken := dates.Now().Sub(start)
	logrus.WithField("time_taken", timeTaken).WithField("num_orgs", len(orgs)).Info("archiving of active orgs complete")

	analytics.Gauge("archiver.archive_elapsed", timeTaken.Seconds())
	analytics.Gauge("archiver.orgs_archived", float64(len(orgs)))
	analytics.Gauge("archiver.msgs_records_archived", float64(totalMsgsRecordsArchived))
	analytics.Gauge("archiver.msgs_archives_created", float64(totalMsgsArchivesCreated))
	analytics.Gauge("archiver.msgs_archives_failed", float64(totalMsgsArchivesFailed))
	analytics.Gauge("archiver.msgs_rollups_created", float64(totalMsgsRollupsCreated))
	analytics.Gauge("archiver.msgs_rollups_failed", float64(totalMsgsRollupsFailed))
	analytics.Gauge("archiver.runs_records_archived", float64(totalRunsRecordsArchived))
	analytics.Gauge("archiver.runs_archives_created", float64(totalRunsArchivesCreated))
	analytics.Gauge("archiver.runs_archives_failed", float64(totalRunsArchivesFailed))
	analytics.Gauge("archiver.runs_rollups_created", float64(totalRunsRollupsCreated))
	analytics.Gauge("archiver.runs_rollups_failed", float64(totalRunsRollupsFailed))

	return nil
}
