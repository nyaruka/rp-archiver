package archiver

import (
	"bufio"
	"compress/gzip"
	"context"
	"crypto/md5"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"time"

	"os"
	"path/filepath"

	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
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
	ID         int       `db:"id"`
	Name       string    `db:"name"`
	CreatedOn  time.Time `db:"created_on"`
	IsAnon     bool      `db:"is_anon"`
	ActiveDays int
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
	DeletionDate  *time.Time `db:"deletion_date"`
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

func (a *Archive) coversDate(d time.Time) bool {
	end := a.endDate()
	return !a.StartDate.After(d) && end.After(d)
}

const lookupActiveOrgs = `SELECT id, name, created_on, is_anon FROM orgs_org WHERE is_active = TRUE order by id`

// GetActiveOrgs returns the active organizations sorted by id
func GetActiveOrgs(ctx context.Context, db *sqlx.DB) ([]Org, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	rows, err := db.QueryxContext(ctx, lookupActiveOrgs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	orgs := make([]Org, 0, 10)
	for rows.Next() {
		org := Org{ActiveDays: 90}
		err = rows.StructScan(&org)
		if err != nil {
			return nil, err
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
		return nil, err
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
		return nil, err
	}

	return archives, nil
}

const lookupCountOrgArchives = `
SELECT count(id) 
FROM archives_archive 
WHERE org_id = $1 AND archive_type = $2
`

// GetCurrentArchiveCount returns all the current archives for the passed in org and record type
func GetCurrentArchiveCount(ctx context.Context, db *sqlx.DB, org Org, archiveType ArchiveType) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	var archiveCount int

	rows, err := db.QueryxContext(ctx, lookupCountOrgArchives, org.ID, archiveType)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	rows.Next()
	err = rows.Scan(&archiveCount)

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
		return nil, err
	}

	return existingArchives, nil
}

// GetMissingDailyArchives calculates what archives need to be generated for the passed in org this is calculated per day
func GetMissingDailyArchives(ctx context.Context, db *sqlx.DB, now time.Time, org Org, archiveType ArchiveType) ([]*Archive, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	// our first archive would be active days from today
	endDate := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC).AddDate(0, 0, -org.ActiveDays)
	orgUTC := org.CreatedOn.In(time.UTC)
	startDate := time.Date(orgUTC.Year(), orgUTC.Month(), orgUTC.Day(), 0, 0, 0, 0, time.UTC)

	return GetMissingDailyArchivesForDateRange(ctx, db, startDate, endDate, org, archiveType)
}

const lookupMissingDailyArchive = `
WITH month_days(missing_day) AS (
  select generate_series($1::timestamp with time zone, $2::timestamp with time zone, '1 day')::date
), curr_archives AS (
  SELECT start_date FROM archives_archive WHERE org_id = $3 and period = $4 and archive_type=$5
UNION DISTINCT
  -- also get the overlapping days for the monthly rolled up archives
  SELECT generate_series(start_date, (start_date + '1 month'::interval) - '1 second'::interval, '1 day')::date as start_date
  FROM archives_archive WHERE org_id = $3 and period = 'M' and archive_type=$5
)
select missing_day::timestamp with time zone from month_days LEFT JOIN curr_archives ON curr_archives.start_date = month_days.missing_day
WHERE curr_archives.start_date IS NULL
`

// GetMissingDailyArchivesForDateRange returns all them missing daily archives between the two passed in date ranges
func GetMissingDailyArchivesForDateRange(ctx context.Context, db *sqlx.DB, startDate time.Time, endDate time.Time, org Org, archiveType ArchiveType) ([]*Archive, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	missing := make([]*Archive, 0, 1)

	rows, err := db.QueryxContext(ctx, lookupMissingDailyArchive, startDate, endDate, org.ID, DayPeriod, archiveType)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var missingDay time.Time
	for rows.Next() {

		err = rows.Scan(&missingDay)
		if err != nil {
			return nil, err
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

	lastActive := now.AddDate(0, 0, -org.ActiveDays)
	endDate := time.Date(lastActive.Year(), lastActive.Month(), 1, 0, 0, 0, 0, time.UTC)

	orgUTC := org.CreatedOn.In(time.UTC)
	startDate := time.Date(orgUTC.Year(), orgUTC.Month(), 1, 0, 0, 0, 0, time.UTC)

	missing := make([]*Archive, 0, 1)

	rows, err := db.QueryxContext(ctx, lookupMissingMonthlyArchive, startDate, endDate, org.ID, MonthPeriod, archiveType)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var missingMonth time.Time
	for rows.Next() {

		err = rows.Scan(&missingMonth)
		if err != nil {
			return nil, err
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

	start := time.Now()

	log := logrus.WithFields(logrus.Fields{
		"org_id":       monthlyArchive.Org.ID,
		"archive_type": monthlyArchive.ArchiveType,
		"start_date":   monthlyArchive.StartDate,
		"period":       monthlyArchive.Period,
	})

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
		return fmt.Errorf("missing '%d' daily archives", len(missingDailies))
	}

	// great, we have all the dailies we need, download them
	filename := fmt.Sprintf("%s_%d_%s_%d_%02d_", monthlyArchive.ArchiveType, monthlyArchive.Org.ID, monthlyArchive.Period, monthlyArchive.StartDate.Year(), monthlyArchive.StartDate.Month())
	file, err := ioutil.TempFile(conf.TempDir, filename)
	if err != nil {
		log.WithError(err).Error("error creating temp file")
		return err
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
	if estimatedSize > 4e9 {
		return fmt.Errorf("rollup size (%d) bigger than currently possible skipping", estimatedSize)
	}

	// for each daily
	for _, daily := range dailies {
		// if there are no records in this daily, just move on
		if daily.RecordCount == 0 {
			continue
		}

		reader, err := GetS3File(ctx, s3Client, daily.URL)
		if err != nil {
			log.WithError(err).Error("error getting daily S3 file")
			return err
		}

		// set up our reader to calculate our hash along the way
		readerHash := md5.New()
		teeReader := io.TeeReader(reader, readerHash)
		gzipReader, err := gzip.NewReader(teeReader)
		if err != nil {
			log.WithError(err).Error("error creating gzip reader")
			return err
		}

		// copy this daily file (uncompressed) to our new monthly file
		_, err = io.Copy(writer, gzipReader)
		if err != nil {
			log.WithError(err).Error("error copying from s3 to disk")
			return err
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
		return err
	}
	monthlyArchive.Size = stat.Size()
	monthlyArchive.RecordCount = recordCount
	monthlyArchive.BuildTime = int(time.Since(start) / time.Millisecond)
	monthlyArchive.Dailies = dailies
	monthlyArchive.NeedsDeletion = false

	return nil
}

const lookupMsgs = `
SELECT rec.visibility, row_to_json(rec) FROM (
	SELECT
	  mm.id,
	  broadcast_id as broadcast,
	  row_to_json(contact) as contact,
	  CASE WHEN oo.is_anon = False THEN ccu.identity ELSE null END as urn,
	  row_to_json(channel) as channel,
	  CASE WHEN direction = 'I' THEN 'in'
		WHEN direction = 'O' THEN 'out'
		ELSE NULL
	  END as direction,
	  CASE WHEN msg_type = 'F'
		THEN 'flow'
	  WHEN msg_type = 'V'
		THEN 'ivr'
	  WHEN msg_type = 'I'
		THEN 'inbox'
	  ELSE NULL
	  END as "type",
	  CASE when status = 'I' then 'initializing'
		WHEN status = 'P' then 'queued'
		WHEN status = 'Q' then 'queued'
		WHEN status = 'W' then 'wired'
		WHEN status = 'D' then 'delivered'
		WHEN status = 'H' then 'handled'
		WHEN status = 'E' then 'errored'
		WHEN status = 'F' then 'failed'
		WHEN status = 'R' then 'resent'
		ELSE NULL
	  END as status,
	
	  CASE WHEN visibility = 'V' THEN 'visible'
		WHEN visibility = 'A' THEN 'archived'
		WHEN visibility = 'D' THEN 'deleted'
		ELSE NULL
		END as visibility,
	  text,
	  (select coalesce(jsonb_agg(attach_row), '[]'::jsonb) FROM (select attach_data.attachment[1] as content_type, attach_data.attachment[2] as url FROM (select regexp_matches(unnest(attachments), '^(.*?):(.*)$') attachment) as attach_data) as attach_row) as attachments,
	  labels_agg.data as labels,
	  mm.created_on as created_on,
	  sent_on
	FROM msgs_msg mm JOIN contacts_contacturn ccu ON mm.contact_urn_id = ccu.id JOIN orgs_org oo ON ccu.org_id = oo.id
	  JOIN LATERAL (select uuid, name from contacts_contact cc where cc.id = mm.contact_id and cc.is_test = FALSE) as contact ON True
	  LEFT JOIN LATERAL (select uuid, name from channels_channel ch where ch.id = mm.channel_id) as channel ON True
	  LEFT JOIN LATERAL (select coalesce(jsonb_agg(label_row), '[]'::jsonb) as data from (select uuid, name from msgs_label ml INNER JOIN msgs_msg_labels mml ON ml.id = mml.label_id AND mml.msg_id = mm.id) as label_row) as labels_agg ON True

	  WHERE mm.org_id = $1 AND mm.created_on >= $2 AND mm.created_on < $3
	ORDER BY created_on ASC, id ASC) rec; 
`

const lookupFlowRuns = `
SELECT row_to_json(rec)
FROM (
   SELECT
     fr.id,
     row_to_json(flow_struct) as flow,
     row_to_json(contact_struct) as contact,
     fr.responded,
     (select coalesce(jsonb_agg(path_data), '[]'::jsonb) from (
		select path_row ->> 'node_uuid'  as node, (path_row ->> 'arrived_on')::timestamptz as time
		from jsonb_array_elements(fr.path :: jsonb) as path_row) as path_data
     ) as path,
     (select coalesce(jsonb_agg(values_data.tmp_values), '{}'::jsonb) from (
		select json_build_object(key, jsonb_build_object('name', value -> 'name', 'value', value -> 'value', 'input', value -> 'input', 'time', (value -> 'created_on')::text::timestamptz, 'category', value -> 'category', 'node', value -> 'node_uuid')) as tmp_values
		FROM jsonb_each(fr.results :: jsonb)) as values_data
	 ) as values,
	 CASE
		WHEN $1
			THEN '[]'::jsonb
		ELSE
			coalesce(fr.events, '[]'::jsonb)
	 END as events,
     fr.created_on,
     fr.modified_on,
     fr.exited_on,
     CASE
        WHEN exit_type = 'C'
          THEN 'completed'
        WHEN exit_type = 'I'
          THEN 'interrupted'
        WHEN exit_type = 'E'
          THEN 'expired'
        ELSE
          null
     END as exit_type

   FROM flows_flowrun fr
     JOIN LATERAL (SELECT uuid, name from flows_flow where flows_flow.id = fr.flow_id) as flow_struct ON True
     JOIN LATERAL (select uuid, name from contacts_contact cc where cc.id = fr.contact_id) as contact_struct ON True
   
   WHERE fr.org_id = $2 AND fr.modified_on >= $3 AND fr.modified_on < $4
   ORDER BY fr.modified_on ASC, id ASC
) as rec;
`

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
		return err
	}

	// is path a directory
	if !fileInfo.IsDir() {
		return fmt.Errorf("path '%s' is not a directory", path)
	}

	testFilePath := filepath.Join(path, ".test_file")
	testFile, err := os.Create(testFilePath)
	defer testFile.Close()

	if err != nil {
		return fmt.Errorf("directory '%s' is not writable", path)
	}

	err = os.Remove(testFilePath)
	return err
}

// CreateArchiveFile is responsible for writing an archive file for the passed in archive from our database
func CreateArchiveFile(ctx context.Context, db *sqlx.DB, archive *Archive, archivePath string) error {
	ctx, cancel := context.WithTimeout(ctx, time.Hour*3)
	defer cancel()

	start := time.Now()

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
		return err
	}
	hash := md5.New()
	gzWriter := gzip.NewWriter(io.MultiWriter(file, hash))
	writer := bufio.NewWriter(gzWriter)
	defer file.Close()

	log.WithFields(logrus.Fields{
		"filename": file.Name(),
	}).Debug("creating new archive file")

	var rows *sqlx.Rows
	if archive.ArchiveType == MessageType {
		rows, err = db.QueryxContext(ctx, lookupMsgs, archive.Org.ID, archive.StartDate, archive.endDate())
	} else if archive.ArchiveType == RunType {
		rows, err = db.QueryxContext(ctx, lookupFlowRuns, archive.Org.IsAnon, archive.Org.ID, archive.StartDate, archive.endDate())
	}
	if err != nil {
		return err
	}
	defer rows.Close()

	recordCount := 0
	var record, visibility string
	for rows.Next() {
		if archive.ArchiveType == MessageType {
			err = rows.Scan(&visibility, &record)
			if err != nil {
				return err
			}

			// skip over deleted rows
			if visibility == "deleted" {
				continue
			}
		} else if archive.ArchiveType == RunType {
			err = rows.Scan(&record)
			if err != nil {
				return err
			}
		}

		writer.WriteString(record)
		writer.WriteString("\n")
		recordCount++

		if recordCount%100000 == 0 {
			log.WithFields(logrus.Fields{
				"filename":     file.Name(),
				"record_count": recordCount,
				"elapsed":      time.Since(start),
			}).Debug("writing archive file")
		}
	}

	archive.ArchiveFile = file.Name()
	err = writer.Flush()
	if err != nil {
		return err
	}

	err = gzWriter.Close()
	if err != nil {
		return err
	}

	// calculate our size and hash
	archive.Hash = hex.EncodeToString(hash.Sum(nil))
	stat, err := file.Stat()
	if err != nil {
		return err
	}
	archive.Size = stat.Size()
	archive.RecordCount = recordCount
	archive.BuildTime = int(time.Since(start) / time.Millisecond)

	log.WithFields(logrus.Fields{
		"record_count": recordCount,
		"filename":     file.Name(),
		"file_size":    archive.Size,
		"file_hash":    archive.Hash,
		"elapsed":      time.Since(start),
	}).Debug("completed writing archive file")
	return nil
}

// UploadArchive uploads the passed archive file to S3
func UploadArchive(ctx context.Context, s3Client s3iface.S3API, bucket string, archive *Archive) error {
	ctx, cancel := context.WithTimeout(ctx, time.Minute*15)
	defer cancel()

	// s3 wants a base64 encoded hash instead of our hex encoded
	hashBytes, _ := hex.DecodeString(archive.Hash)
	hashBase64 := base64.StdEncoding.EncodeToString(hashBytes)

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

	url, err := PutS3File(
		ctx,
		s3Client,
		bucket,
		archivePath,
		"application/json",
		"gzip",
		archive.ArchiveFile,
		hashBase64,
	)

	if err != nil {
		return err
	}

	archive.URL = url
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

const updateRollups = `
UPDATE archives_archive 
SET rollup_id = $1 
WHERE ARRAY[id] <@ $2
`

// WriteArchiveToDB write an archive to the Database
func WriteArchiveToDB(ctx context.Context, db *sqlx.DB, archive *Archive) error {
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	archive.OrgID = archive.Org.ID
	archive.CreatedOn = time.Now()

	tx, err := db.BeginTxx(ctx, nil)
	if err != nil {
		logrus.WithError(err).Error("error starting transaction")
		return err
	}

	rows, err := tx.NamedQuery(insertArchive, archive)
	if err != nil {
		logrus.WithError(err).Error("error inserting archive")
		tx.Rollback()
		return err
	}

	rows.Next()
	err = rows.Scan(&archive.ID)
	if err != nil {
		logrus.WithError(err).Error("error reading new archive id")
		tx.Rollback()
		return err
	}
	rows.Close()

	// if we have children to update do so
	if len(archive.Dailies) > 0 {
		// build our list of ids
		childIDs := make([]int, 0, len(archive.Dailies))
		for _, c := range archive.Dailies {
			childIDs = append(childIDs, c.ID)
		}

		result, err := tx.ExecContext(ctx, updateRollups, archive.ID, pq.Array(childIDs))
		if err != nil {
			logrus.WithError(err).Error("error updating rollup ids")
			tx.Rollback()
			return err
		}
		affected, err := result.RowsAffected()
		if err != nil {
			logrus.WithError(err).Error("error getting number rollup ids updated")
			tx.Rollback()
			return err
		}
		if int(affected) != len(childIDs) {
			logrus.Error("mismatch in number of children and number of rows updated")
			tx.Rollback()
			return fmt.Errorf("mismatch in number of children updated")
		}
	}

	err = tx.Commit()
	if err != nil {
		logrus.WithError(err).Error("error comitting new archive")
		tx.Rollback()
	}
	return err
}

// DeleteArchiveFile removes our own disk archive file
func DeleteArchiveFile(archive *Archive) error {
	err := os.Remove(archive.ArchiveFile)

	if err != nil {
		return err
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
func CreateOrgArchives(ctx context.Context, now time.Time, config *Config, db *sqlx.DB, s3Client s3iface.S3API, org Org, archiveType ArchiveType) ([]*Archive, error) {
	log := logrus.WithFields(logrus.Fields{
		"org":    org.Name,
		"org_id": org.ID,
	})
	records := 0
	start := time.Now()

	archiveCount, err := GetCurrentArchiveCount(ctx, db, org, archiveType)
	if err != nil {
		return nil, fmt.Errorf("error getting current archives")
	}

	var archives []*Archive
	if archiveCount == 0 {
		// no existing archives means this might be a backfill, figure out if there are full monthes we can build first
		archives, err = GetMissingMonthlyArchives(ctx, db, now, org, archiveType)
		if err != nil {
			log.WithError(err).Error("error calculating missing monthly archives")
			return nil, err
		}

		// we first create monthly archives
		err = createArchives(ctx, db, config, s3Client, org, archives)
		if err != nil {
			return nil, err
		}

		// then add in daily archives taking into account the monthly that have been built
		daily, err := GetMissingDailyArchives(ctx, db, now, org, archiveType)
		if err != nil {
			log.WithError(err).Error("error calculating missing daily archives")
			return nil, err
		}
		// we then create missing daily archives
		err = createArchives(ctx, db, config, s3Client, org, daily)
		if err != nil {
			return nil, err
		}

		// append daily archives to the monthly archives
		archives = append(archives, daily...)
		defer ctx.Done()
	} else {
		// figure out any missing day archives
		archives, err = GetMissingDailyArchives(ctx, db, now, org, archiveType)
		if err != nil {
			log.WithError(err).Error("error calculating missing daily archives")
			return nil, err
		}

		err = createArchives(ctx, db, config, s3Client, org, archives)
		if err != nil {
			return nil, err
		}

	}

	// sum all records in the archives
	for _, archive := range archives {
		records += archive.RecordCount
	}

	if len(archives) > 0 {
		elapsed := time.Since(start)
		rate := float32(records) / (float32(elapsed) / float32(time.Second))
		log.WithFields(logrus.Fields{
			"elapsed":            elapsed,
			"records_per_second": rate,
		}).Info("completed archival for org")
	}

	return archives, nil
}

func createArchives(ctx context.Context, db *sqlx.DB, config *Config, s3Client s3iface.S3API, org Org, archives []*Archive) error {
	log := logrus.WithFields(logrus.Fields{
		"org":    org.Name,
		"org_id": org.ID,
	})

	for _, archive := range archives {
		log = log.WithFields(logrus.Fields{
			"start_date":   archive.StartDate,
			"end_date":     archive.endDate(),
			"period":       archive.Period,
			"archive_type": archive.ArchiveType,
		})
		log.Info("starting archive")
		start := time.Now()

		err := CreateArchiveFile(ctx, db, archive, config.TempDir)
		if err != nil {
			log.WithError(err).Error("error writing archive file")
			continue
		}

		if config.UploadToS3 {
			err = UploadArchive(ctx, s3Client, config.S3Bucket, archive)
			if err != nil {
				log.WithError(err).Error("error writing archive to s3")
				return err
			}
		}

		err = WriteArchiveToDB(ctx, db, archive)
		if err != nil {
			log.WithError(err).Error("error writing record to db")
			continue
		}

		// purge records that were archived

		if config.DeleteAfterUpload == true {
			err := DeleteArchiveFile(archive)
			if err != nil {
				log.WithError(err).Error("error deleting temporary file")
				continue
			}
		}

		elapsed := time.Since(start)
		log.WithFields(logrus.Fields{
			"id":           archive.ID,
			"record_count": archive.RecordCount,
			"elapsed":      elapsed,
		}).Info("archive complete")
	}

	return nil
}

// RollupOrgArchives rolls up monthly archives from our daily archives
func RollupOrgArchives(ctx context.Context, now time.Time, config *Config, db *sqlx.DB, s3Client s3iface.S3API, org Org, archiveType ArchiveType) ([]*Archive, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Hour*3)
	defer cancel()

	log := logrus.WithFields(logrus.Fields{
		"org":    org.Name,
		"org_id": org.ID,
	})
	created := make([]*Archive, 0, 1)

	// get our missing monthly archives
	archives, err := GetMissingMonthlyArchives(ctx, db, now, org, archiveType)
	if err != nil {
		return nil, fmt.Errorf("error calculating missing monthly archives for type '%s'", archiveType)
	}

	// build them from rollups
	for _, archive := range archives {
		log.WithFields(logrus.Fields{
			"start_date":   archive.StartDate,
			"archive_type": archive.ArchiveType,
		})
		start := time.Now()
		log.Info("starting rollup")

		err = BuildRollupArchive(ctx, db, config, s3Client, archive, now, org, archiveType)
		if err != nil {
			log.WithError(err).Error("error building monthly archive")
			continue
		}

		if config.UploadToS3 {
			err = UploadArchive(ctx, s3Client, config.S3Bucket, archive)
			if err != nil {
				log.WithError(err).Error("error writing archive to s3")
				continue
			}
		}

		err = WriteArchiveToDB(ctx, db, archive)
		if err != nil {
			log.WithError(err).Error("error writing record to db")
			continue
		}

		if config.DeleteAfterUpload == true {
			err := DeleteArchiveFile(archive)
			if err != nil {
				log.WithError(err).Error("error deleting temporary file")
				continue
			}
		}

		log.WithFields(logrus.Fields{
			"id":           archive.ID,
			"record_count": archive.RecordCount,
			"elapsed":      time.Since(start),
		}).Info("rollup complete")
		created = append(created, archive)
	}

	return created, nil
}

const selectOrgMessagesInRange = `
SELECT mm.id, mm.visibility, cc.is_test
FROM msgs_msg mm
LEFT JOIN contacts_contact cc ON cc.id = mm.contact_id
WHERE mm.org_id = $1 AND mm.created_on >= $2 AND mm.created_on < $3
ORDER BY mm.created_on ASC, mm.id ASC
`

const setMessageDeleteReason = `
UPDATE msgs_msg 
SET delete_reason = 'A' 
WHERE id IN(?)
`

const deleteMessageLogs = `
DELETE FROM channels_channellog 
WHERE msg_id IN(?)
`

const deleteMessageLabels = `
DELETE FROM msgs_msg_labels 
WHERE msg_id IN(?)
`

const unlinkResponses = `
UPDATE msgs_msg 
SET response_to_id = NULL 
WHERE response_to_id IN(?)
`

const deleteMessages = `
DELETE FROM msgs_msg 
WHERE id IN(?)
`

const setArchiveDeleted = `
UPDATE archives_archive 
SET needs_deletion = FALSE, deletion_date = $2
WHERE id = $1
`

// helper method to safely execute an IN query in the passed in transaction
func executeInQuery(ctx context.Context, tx *sqlx.Tx, query string, ids []int64) error {
	q, vs, err := sqlx.In(query, ids)
	if err != nil {
		return err
	}
	q = tx.Rebind(q)

	_, err = tx.ExecContext(ctx, q, vs...)
	if err != nil {
		tx.Rollback()
	}
	return err
}

var deleteTransactionSize = 100

// DeleteArchivedMessages takes the passed in archive, verifies the S3 file is still present (and correct), then selects
// all the messages in the archive date range, and if equal or fewer than the number archived, deletes them 1000 at a time
//
// Upon completion it updates the needs_deletion flag on the archive
func DeleteArchivedMessages(ctx context.Context, config *Config, db *sqlx.DB, s3Client s3iface.S3API, archive *Archive) error {
	outer, cancel := context.WithTimeout(ctx, time.Minute*15)
	defer cancel()

	start := time.Now()
	log := logrus.WithFields(logrus.Fields{
		"id":           archive.ID,
		"org_id":       archive.OrgID,
		"start_date":   archive.StartDate,
		"end_date":     archive.endDate(),
		"archive_type": archive.ArchiveType,
		"total_count":  archive.RecordCount,
	})
	log.Info("deleting messages")

	// first things first, make sure our file is present on S3
	md5, err := GetS3FileETAG(outer, s3Client, archive.URL)
	if err != nil {
		return err
	}

	// if our etag and archive md5 don't match, that's an error, return
	if md5 != archive.Hash {
		return fmt.Errorf("archive md5: %s and s3 etag: %s do not match", archive.Hash, md5)
	}

	// ok, archive file looks good, let's build up our list of message ids, this may be big but we are int64s so shouldn't be too big
	rows, err := db.QueryxContext(outer, selectOrgMessagesInRange, archive.OrgID, archive.StartDate, archive.endDate())
	if err != nil {
		return err
	}
	defer rows.Close()

	visibleCount := 0
	var msgID int64
	var visibility string
	var isTest bool
	msgIDs := make([]int64, 0, archive.RecordCount)
	for rows.Next() {
		err = rows.Scan(&msgID, &visibility, &isTest)
		if err != nil {
			return err
		}
		msgIDs = append(msgIDs, msgID)

		// keep track of the number of visible messages, ie, not deleted and not for test contacts
		if visibility != "D" && !isTest {
			visibleCount++
		}
	}
	rows.Close()

	log.WithFields(logrus.Fields{
		"msg_count": len(msgIDs),
	}).Debug("found messages")

	// verify we don't see more messages than there are in our archive (fewer is ok)
	if visibleCount > archive.RecordCount {
		return fmt.Errorf("more messages in the database: %d than in archive: %d", visibleCount, archive.RecordCount)
	}

	// ok, delete our messages 1000 at a time, we do this in transactions as it spans a few different queries
	for startIdx := 0; startIdx < len(msgIDs); startIdx += deleteTransactionSize {
		// no single batch should take more than a few minutes
		ctx, cancel := context.WithTimeout(ctx, time.Minute*5)
		defer cancel()

		start := time.Now()

		endIdx := startIdx + deleteTransactionSize
		if endIdx > len(msgIDs) {
			endIdx = len(msgIDs)
		}
		batchIDs := msgIDs[startIdx:endIdx]

		// start our transaction
		tx, err := db.BeginTxx(ctx, nil)
		if err != nil {
			return err
		}

		// first update our delete_reason
		err = executeInQuery(ctx, tx, setMessageDeleteReason, batchIDs)
		if err != nil {
			return fmt.Errorf("error updating delete reason: %s", err.Error())
		}

		// now delete any channel logs
		err = executeInQuery(ctx, tx, deleteMessageLogs, batchIDs)
		if err != nil {
			return fmt.Errorf("error removing channel logs: %s", err.Error())
		}

		// then any labels
		err = executeInQuery(ctx, tx, deleteMessageLabels, batchIDs)
		if err != nil {
			return fmt.Errorf("error removing message labels: %s", err.Error())
		}

		// unlink any responses
		err = executeInQuery(ctx, tx, unlinkResponses, batchIDs)
		if err != nil {
			return fmt.Errorf("error unlinking responses: %s", err.Error())
		}

		// finally, delete our messages
		err = executeInQuery(ctx, tx, deleteMessages, batchIDs)
		if err != nil {
			return fmt.Errorf("error deleting messages: %s", err.Error())
		}

		// commit our transaction
		err = tx.Commit()
		if err != nil {
			return fmt.Errorf("error committing message delete transaction: %s", err.Error())
		}

		log.WithFields(logrus.Fields{
			"elapsed": time.Since(start),
			"count":   len(batchIDs),
		}).Debug("deleted batch of messages")

		cancel()
	}

	outer, cancel = context.WithTimeout(ctx, time.Minute)
	defer cancel()

	deletionDate := time.Now()

	// all went well! mark our archive as no longer needing deletion
	_, err = db.ExecContext(outer, setArchiveDeleted, archive.ID, deletionDate)
	if err != nil {
		return fmt.Errorf("error setting archive as deleted: %s", err.Error())
	}
	archive.NeedsDeletion = false
	archive.DeletionDate = &deletionDate

	logrus.WithFields(logrus.Fields{
		"elapsed": time.Since(start),
	}).Info("completed deleting messages")

	return nil
}

// DeleteArchivedOrgRecords deletes all the records for the passeg in org based on archives already created
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

		if a.ArchiveType == MessageType {
			start := time.Now()

			err := DeleteArchivedMessages(ctx, config, db, s3Client, a)
			if err != nil {
				log.WithError(err).Error("Error deleting archive messages")
				continue
			}

			deleted = append(deleted, a)

			log.WithFields(logrus.Fields{
				"elapsed": time.Since(start),
			}).Info("deleted archive messages")
		}
	}

	return deleted, nil
}

// ArchiveOrg looks for any missing archives for the passed in org, creating and uploading them as necessary, returning the created archives
func ArchiveOrg(ctx context.Context, now time.Time, config *Config, db *sqlx.DB, s3Client s3iface.S3API, org Org, archiveType ArchiveType) ([]*Archive, []*Archive, error) {
	created, err := CreateOrgArchives(ctx, now, config, db, s3Client, org, archiveType)
	if err != nil {
		return nil, nil, err
	}

	monthlies, err := RollupOrgArchives(ctx, now, config, db, s3Client, org, archiveType)
	if err != nil {
		return nil, nil, err
	}

	for _, m := range monthlies {
		created = append(created, m)
	}

	// finally delete any archives not yet actually archived
	deleted := make([]*Archive, 0, 1)
	if config.DeleteRecords {
		deleted, err = DeleteArchivedOrgRecords(ctx, now, config, db, s3Client, org, archiveType)
		if err != nil {
			return created, deleted, err
		}
	}

	return created, deleted, nil
}
