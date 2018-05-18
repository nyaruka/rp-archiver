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

	IsPurged bool `db:"is_purged"`
	Rollup   *int `db:"rollup_id"`

	Org         Org
	ArchiveFile string
	Dailies     []*Archive
}

const lookupActiveOrgs = `SELECT id, name, created_on, is_anon FROM orgs_org WHERE is_active = TRUE order by id`

// GetActiveOrgs returns the active organizations sorted by id
func GetActiveOrgs(ctx context.Context, db *sqlx.DB) ([]Org, error) {
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

const lookupOrgArchives = `SELECT id, start_date, period, archive_type, hash, size, record_count, url, rollup_id FROM archives_archive WHERE org_id = $1 AND archive_type = $2 ORDER BY start_date asc`

// GetCurrentArchives returns all the current archives for the passed in org and record type
func GetCurrentArchives(ctx context.Context, db *sqlx.DB, org Org, archiveType ArchiveType) ([]*Archive, error) {
	existingArchives := []*Archive{}
	err := db.SelectContext(ctx, &existingArchives, lookupOrgArchives, org.ID, archiveType)
	if err != nil && err != sql.ErrNoRows {
		return nil, err
	}

	return existingArchives, nil
}

// GetMissingDayArchives calculates what archives need to be generated for the passed in org this is calculated per day
func GetMissingDayArchives(archives []*Archive, now time.Time, org Org, archiveType ArchiveType) ([]*Archive, error) {
	// our first archive would be active days from today
	endDate := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC).AddDate(0, 0, -org.ActiveDays)
	orgUTC := org.CreatedOn.In(time.UTC)
	startDate := time.Date(orgUTC.Year(), orgUTC.Month(), orgUTC.Day(), 0, 0, 0, 0, time.UTC)

	missing := make([]*Archive, 0, 1)
	archiveIDX := 0

	// walk forwards until we are after our end date
	for !startDate.After(endDate) {
		existing := false

		// advance our current archive idx until we are on our start date or later
		for archiveIDX < len(archives) && (archives[archiveIDX].StartDate.Before(startDate) || archives[archiveIDX].Period == MonthPeriod) {
			archiveIDX++
		}

		// do we already have this archive?
		if archiveIDX < len(archives) && archives[archiveIDX].StartDate.Equal(startDate) {
			existing = true
		}

		// this archive doesn't exist yet, we'll create it
		if !existing {
			archive := Archive{
				Org:         org,
				OrgID:       org.ID,
				StartDate:   startDate,
				ArchiveType: archiveType,
				Period:      DayPeriod,
			}
			missing = append(missing, &archive)
		}

		startDate = startDate.Add(time.Hour * 24)
	}

	return missing, nil
}

// GetMissingMonthArchives gets which archives need to be rolled up in the set that has been passed in
func GetMissingMonthArchives(archives []*Archive, now time.Time, org Org, archiveType ArchiveType) ([]*Archive, error) {
	lastActive := now.AddDate(0, 0, -org.ActiveDays)
	endDate := time.Date(lastActive.Year(), lastActive.Month(), 1, 0, 0, 0, 0, time.UTC)

	orgUTC := org.CreatedOn.In(time.UTC)
	startDate := time.Date(orgUTC.Year(), orgUTC.Month(), 1, 0, 0, 0, 0, time.UTC)

	missing := make([]*Archive, 0, 1)
	archiveIDX := 0

	// walk forwards while we are before our end date
	for startDate.Before(endDate) {
		existing := false

		// advance our current archive idx until we are on our start date or later
		for archiveIDX < len(archives) && (archives[archiveIDX].StartDate.Before(startDate) || archives[archiveIDX].Period == DayPeriod) {
			archiveIDX++
		}

		// do we already have this archive?
		if archiveIDX < len(archives) && archives[archiveIDX].StartDate.Equal(startDate) {
			existing = true
		}

		// this archive doesn't exist yet, we'll create it
		if !existing {
			archive := Archive{
				Org:         org,
				OrgID:       org.ID,
				StartDate:   startDate,
				ArchiveType: archiveType,
				Period:      MonthPeriod,
			}
			missing = append(missing, &archive)
		}

		// increment a month
		startDate = startDate.AddDate(0, 1, 0)
	}

	return missing, nil
}

// BuildMonthlyArchive builds a monthly archive from the files present on S3
func BuildMonthlyArchive(ctx context.Context, conf *Config, s3Client s3iface.S3API, archives []*Archive, month *Archive, now time.Time, org Org, archiveType ArchiveType) error {
	start := time.Now()

	log := logrus.WithFields(logrus.Fields{
		"org_id":       month.Org.ID,
		"archive_type": month.ArchiveType,
		"start_date":   month.StartDate,
		"period":       month.Period,
	})

	// figure out the first day in the month we'll archive
	startDate := month.StartDate
	if month.StartDate.Before(org.CreatedOn) {
		orgUTC := org.CreatedOn.In(time.UTC)
		startDate = time.Date(orgUTC.Year(), orgUTC.Month(), orgUTC.Day(), 0, 0, 0, 0, time.UTC)
	}

	// grab all the daily archives we need
	dailies := make([]*Archive, 0, 31)
	day := startDate.Day()
	for _, archive := range archives {
		if archive.StartDate.Year() == month.StartDate.Year() && archive.StartDate.Month() == month.StartDate.Month() {
			dailies = append(dailies, archive)

			if archive.StartDate.Day() != day {
				return fmt.Errorf("missing day: %d in monthly archive", day)
			}
			day++
		}
	}

	// figure out number of days in the month
	days := month.StartDate.AddDate(0, 1, 0).AddDate(0, 0, -1).Day() - (startDate.Day() - 1)
	if len(dailies) != days {
		return fmt.Errorf("missing daily archives, need %d, have %d", days, len(dailies))
	}

	// great, we have all the dailies we need, download them
	filename := fmt.Sprintf("%s_%d_%s_%d_%02d_", month.ArchiveType, month.Org.ID, month.Period, month.StartDate.Year(), month.StartDate.Month())
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

	// for each daily
	for _, daily := range dailies {
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

	month.ArchiveFile = file.Name()
	err = writer.Flush()
	if err != nil {
		return err
	}

	err = gzWriter.Close()
	if err != nil {
		return err
	}

	// calculate our size and hash
	month.Hash = hex.EncodeToString(writerHash.Sum(nil))
	stat, err := file.Stat()
	if err != nil {
		return err
	}
	month.Size = stat.Size()
	month.RecordCount = recordCount
	month.BuildTime = int(time.Now().Sub(start) / time.Millisecond)
	month.Dailies = dailies
	return nil
}

const lookupMsgs = `
select rec.visibility, row_to_json(rec) FROM (
	select
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
	from msgs_msg mm JOIN contacts_contacturn ccu ON mm.contact_urn_id = ccu.id JOIN orgs_org oo ON ccu.org_id = oo.id
	  JOIN LATERAL (select uuid, name from contacts_contact cc where cc.id = mm.contact_id) as contact ON True
	  JOIN LATERAL (select uuid, name from channels_channel ch where ch.id = mm.channel_id) as channel ON True
	  LEFT JOIN LATERAL (select coalesce(jsonb_agg(label_row), '[]'::jsonb) as data from (select uuid, name from msgs_label ml INNER JOIN msgs_msg_labels mml ON ml.id = mml.label_id AND mml.msg_id = mm.id) as label_row) as labels_agg ON True

	  WHERE mm.org_id = $1 AND mm.created_on >= $2 AND mm.created_on < $3
	order by created_on ASC, id ASC) rec; 
`

const lookupFlowRuns = `
select row_to_json(rec)
FROM (
   select
     fr.id,
     row_to_json(flow_struct) as flow,
     row_to_json(contact_struct) as contact,
     fr.responded,
     (select coalesce(jsonb_agg(path_data), '[]'::jsonb) from (
		select path_row ->> 'node_uuid'  as node, (path_row ->> 'arrived_on')::timestamptz as time
		from jsonb_array_elements(fr.path :: jsonb) as path_row) as path_data
     ) as path,
     (select coalesce(jsonb_agg(values_data.tmp_values), '{}'::jsonb) from (
		select json_build_object(key, jsonb_build_object('name', value -> 'name', 'time', (value -> 'created_on')::text::timestamptz, 'category', value -> 'category', 'node', value -> 'node_uuid')) as tmp_values
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
   
   WHERE fr.org_id = $2 AND fr.created_on >= $3 AND fr.created_on < $4
   ORDER BY fr.created_on ASC, id ASC
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
	start := time.Now()

	log := logrus.WithFields(logrus.Fields{
		"org_id":       archive.Org.ID,
		"archive_type": archive.ArchiveType,
		"start_date":   archive.StartDate,
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

	log.WithField("filename", file.Name()).Debug("creating new archive file")

	tx, err := db.BeginTxx(ctx, nil)
	if err != nil {
		return err
	}
	_, err = tx.Exec("SET TIME ZONE 'UTC'")
	if err != nil {
		return err
	}

	endDate := archive.StartDate.Add(time.Hour * 24)
	var rows *sqlx.Rows
	if archive.ArchiveType == MessageType {
		rows, err = tx.QueryxContext(ctx, lookupMsgs, archive.Org.ID, archive.StartDate, endDate)
	} else if archive.ArchiveType == RunType {
		rows, err = tx.QueryxContext(ctx, lookupFlowRuns, archive.Org.IsAnon, archive.Org.ID, archive.StartDate, endDate)
	}
	if err != nil {
		return err
	}
	defer rows.Close()
	defer tx.Rollback()

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
			log.WithField("filename", file.Name()).WithField("record_count", recordCount).WithField("elapsed", time.Now().Sub(start)).Debug("writing archive file")
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
	archive.BuildTime = int(time.Now().Sub(start) / time.Millisecond)

	log.WithFields(logrus.Fields{
		"record_count": recordCount,
		"filename":     file.Name(),
		"file_size":    archive.Size,
		"file_hash":    archive.Hash,
		"elapsed":      time.Now().Sub(start),
	}).Debug("completed writing archive file")
	return nil
}

// UploadArchive uploads the passed archive file to S3
func UploadArchive(ctx context.Context, s3Client s3iface.S3API, bucket string, archive *Archive) error {
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
		s3Client,
		bucket,
		archivePath,
		"application/json",
		"gzip",
		archive.ArchiveFile,
		hashBase64,
	)

	if err == nil {
		archive.URL = url
		logrus.WithFields(logrus.Fields{
			"org_id":       archive.Org.ID,
			"archive_type": archive.ArchiveType,
			"start_date":   archive.StartDate,
			"period":       archive.Period,
			"url":          archive.URL,
			"file_size":    archive.Size,
			"file_hash":    archive.Hash,
		}).Debug("completed uploading archive file")
	}
	return err
}

const insertArchive = `
INSERT INTO archives_archive(archive_type, org_id, created_on, start_date, period, record_count, size, hash, url, is_purged, build_time, rollup_id)
              VALUES(:archive_type, :org_id, :created_on, :start_date, :period, :record_count, :size, :hash, :url, :is_purged, :build_time, :rollup_id)
RETURNING id
`

const updateRollups = `
UPDATE archives_archive SET rollup_id = $1 WHERE ARRAY[id] <@ $2
`

// WriteArchiveToDB write an archive to the Database
func WriteArchiveToDB(ctx context.Context, db *sqlx.DB, archive *Archive) error {
	archive.OrgID = archive.Org.ID
	archive.CreatedOn = time.Now()
	archive.IsPurged = false

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

	log := logrus.WithFields(logrus.Fields{
		"org_id":        archive.Org.ID,
		"archive_type":  archive.ArchiveType,
		"start_date":    archive.StartDate,
		"periond":       archive.Period,
		"db_archive_id": archive.ID,
	})
	log.WithField("filename", archive.ArchiveFile).Debug("deleted temporary archive file")
	return nil
}

// BuildOrgDailyArchives builds all the montly archives for the passid in org
func BuildOrgDailyArchives(ctx context.Context, now time.Time, config *Config, db *sqlx.DB, s3Client s3iface.S3API, org Org, archiveType ArchiveType) ([]*Archive, error) {
	log := logrus.WithField("org", org.Name).WithField("org_id", org.ID)
	records := 0
	created := make([]*Archive, 0, 1)
	start := time.Now()

	existing, err := GetCurrentArchives(ctx, db, org, archiveType)
	if err != nil {
		return nil, fmt.Errorf("error getting current archives")
	}

	archives, err := GetMissingDayArchives(existing, now, org, archiveType)
	if err != nil {
		return nil, fmt.Errorf("error calculating tasks for type '%s'", archiveType)
	}

	for _, archive := range archives {
		log = log.WithField("start_date", archive.StartDate).WithField("period", archive.Period).WithField("archive_type", archive.ArchiveType)
		log.Info("starting archive")
		err := CreateArchiveFile(ctx, db, archive, config.TempDir)
		if err != nil {
			log.WithError(err).Error("error writing archive file")
			continue
		}

		if config.UploadToS3 {
			err = UploadArchive(ctx, s3Client, config.S3Bucket, archive)
			if err != nil {
				log.WithError(err).Error("error writing archive to s3")
				return nil, err
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

		log.WithField("id", archive.ID).WithField("record_count", archive.RecordCount).WithField("elapsed", archive.BuildTime).Info("archive complete")
		records += archive.RecordCount

		created = append(created, archive)
	}

	if len(archives) > 0 {
		elapsed := time.Now().Sub(start)
		rate := float32(records) / (float32(elapsed) / float32(time.Second))
		log.WithField("elapsed", elapsed).WithField("records_per_second", int(rate)).Info("completed archival for org")
	}

	return created, nil
}

// BuildOrgMonthlyArchives builds all the montly archives for the passid in org
func BuildOrgMonthlyArchives(ctx context.Context, now time.Time, config *Config, db *sqlx.DB, s3Client s3iface.S3API, org Org, archiveType ArchiveType) ([]*Archive, error) {
	log := logrus.WithField("org", org.Name).WithField("org_id", org.ID)
	records := 0
	created := make([]*Archive, 0, 1)
	start := time.Now()

	existing, err := GetCurrentArchives(ctx, db, org, archiveType)
	if err != nil {
		return nil, fmt.Errorf("error getting current archives")
	}

	// now build our monthlies
	archives, err := GetMissingMonthArchives(existing, now, org, archiveType)
	if err != nil {
		return nil, fmt.Errorf("error calculating missing monthly archives for type '%s'", archiveType)
	}

	for _, archive := range archives {
		err = BuildMonthlyArchive(ctx, config, s3Client, existing, archive, now, org, archiveType)
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

		log.WithField("id", archive.ID).WithField("record_count", archive.RecordCount).WithField("elapsed", archive.BuildTime).Info("rollup complete")
		created = append(created, archive)
	}

	if len(archives) > 0 {
		elapsed := time.Now().Sub(start)
		rate := float32(records) / (float32(elapsed) / float32(time.Second))
		log.WithField("elapsed", elapsed).WithField("records_per_second", int(rate)).Info("completed rollup for org")
	}

	return created, nil
}

// ArchiveOrg looks for any missing archives for the passed in org, creating and uploading them as necessary, returning the created archives
func ArchiveOrg(ctx context.Context, now time.Time, config *Config, db *sqlx.DB, s3Client s3iface.S3API, org Org, archiveType ArchiveType) ([]*Archive, error) {
	created, err := BuildOrgDailyArchives(ctx, now, config, db, s3Client, org, archiveType)
	if err != nil {
		return nil, err
	}

	monthlies, err := BuildOrgMonthlyArchives(ctx, now, config, db, s3Client, org, archiveType)
	if err != nil {
		return nil, err
	}

	for _, m := range monthlies {
		created = append(created, m)
	}

	return created, nil
}
