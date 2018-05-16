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

	"errors"
	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/rp-archiver/s3"
	"github.com/sirupsen/logrus"
	"os"
	"path/filepath"
)

type ArchiveType string

const (
	FlowRunType = ArchiveType("flowrun")
	MessageType = ArchiveType("message")
	SessionType = ArchiveType("session")
)

type Org struct {
	ID         int       `db:"id"`
	Name       string    `db:"name"`
	CreatedOn  time.Time `db:"created_on"`
	ActiveDays int
}

type ArchiveTask struct {
	ID          int         `db:"id"`
	ArchiveType ArchiveType `db:"archive_type"`

	OrgID           int       `db:"org_id"`
	CreatedOn       time.Time `db:"created_on"`
	ArchiveDuration int       `db:"archive_duration"`

	StartDate time.Time `db:"start_date"`
	EndDate   time.Time `db:"end_date"`

	RecordCount int    `db:"record_count"`
	ArchiveSize int    `db:"archive_size"`
	ArchiveHash string `db:"archive_hash"`
	ArchiveURL  string `db:"archive_url"`

	IsPurged  bool `db:"is_purged"`
	BuildTime int  `db:"build_time"`

	Org      Org
	Filename string

	BuildStart time.Time
}

func addMonth(t time.Time) time.Time {
	monthLater := t.Add(time.Hour * 24 * 31)
	return time.Date(monthLater.Year(), monthLater.Month(), 1, 0, 0, 0, 0, time.UTC)
}

const lookupActiveOrgs = `SELECT id, name, created_on FROM orgs_org WHERE is_active = TRUE`

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

const lookupLastArchive = `SELECT start_date, end_date FROM archives_archive WHERE org_id = $1 AND archive_type = $2 ORDER BY end_date DESC LIMIT 1`

func GetArchiveTasks(ctx context.Context, db *sqlx.DB, org Org, archiveType ArchiveType) ([]ArchiveTask, error) {
	archive := ArchiveTask{}
	err := db.GetContext(ctx, &archive, lookupLastArchive, org.ID, archiveType)
	if err != nil && err != sql.ErrNoRows {
		return nil, err
	}

	nextArchiveStart := time.Date(org.CreatedOn.Year(), org.CreatedOn.Month(), 1, 0, 0, 0, 0, time.UTC)
	if err != sql.ErrNoRows {
		nextArchiveStart = addMonth(archive.StartDate)
	}

	// while our end date of our latest archive is farther away than our active days, create a new task
	tasks := make([]ArchiveTask, 0, 1)
	for time.Now().Sub(nextArchiveStart) > time.Hour*time.Duration(24)*time.Duration(org.ActiveDays) {
		end := addMonth(nextArchiveStart)

		task := ArchiveTask{
			Org:         org,
			ArchiveType: archiveType,
			StartDate:   nextArchiveStart,
			EndDate:     end,
		}
		tasks = append(tasks, task)

		nextArchiveStart = end
	}
	return tasks, nil
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
	  (select coalesce(jsonb_agg(attach_row), '[]'::jsonb) FROM (select attach_data.attachment[1] as content_type, attach_data.attachment[2] as url FROM (select regexp_matches(unnest(attachments), '^(.*?);(.*)$') attachment) as attach_data) as attach_row) as attachments,
	  labels_agg.data as labels,
	  mm.created_on,
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
     (select jsonb_agg(path_data) from (
          select path_row ->> 'node_uuid'  as node, path_row ->> 'arrived_on' as time
          from jsonb_array_elements(fr.path :: jsonb) as path_row) as path_data
     ) as path,
     (select jsonb_agg(values_data.tmp_values) from (
          select json_build_object(key, jsonb_build_object('name', value -> 'name', 'time', value -> 'created_on', 'category', value -> 'category', 'node', value -> 'node_uuid')) as tmp_values
          FROM jsonb_each(fr.results :: jsonb)) as values_data
     ) as values,
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
   
   WHERE fr.org_id = $1 AND fr.created_on >= $2 AND fr.created_on < $3
   ORDER BY fr.created_on ASC, id ASC
) as rec;
`

func EnsureTempArchiveDirectory(ctx context.Context, path string) error {
	if len(path) == 0 {
		return errors.New("Path argument cannot be empty")
	}

	// check if path is a directory we can write to
	fileinfo, err := os.Stat(path)

	if os.IsNotExist(err) {
		// try to create the directory
		err := os.MkdirAll(path, 0700)

		if err != nil {
			return err
		}
		// created the directory
		return nil

	} else if err != nil {
		return err
	}

	// is path a directory
	if !fileinfo.IsDir() {
		return errors.New(fmt.Sprintf("Path '%s' is not a directory", path))
	}

	var test_file_path string = filepath.Join(path, ".test_file")
	test_file, err := os.Create(test_file_path)
	defer test_file.Close()

	if err != nil {
		return errors.New(fmt.Sprintf("Directory '%s' is not writable for the user", path))
	}

	err = os.Remove(test_file_path)

	if err != nil {
		return err
	}

	return nil
}

func generateArchiveFilename(task *ArchiveTask) string {
	filename := fmt.Sprintf("%s_%d_%d%02d_%d%02d_", task.ArchiveType, task.Org.ID, task.StartDate.Year(), task.StartDate.Month(), task.EndDate.Year(), task.EndDate.Month())

	return filename
}

func CreateMsgArchive(ctx context.Context, db *sqlx.DB, task *ArchiveTask, archive_path string) error {
	task.BuildStart = time.Now()

	log := logrus.WithFields(logrus.Fields{
		"org_id":       task.Org.ID,
		"archive_type": task.ArchiveType,
		"start_date":   task.StartDate,
		"end_date":     task.EndDate,
	})

	filename := generateArchiveFilename(task)
	file, err := ioutil.TempFile(archive_path, filename)
	if err != nil {
		return err
	}
	hash := md5.New()
	gzWriter := gzip.NewWriter(io.MultiWriter(file, hash))
	writer := bufio.NewWriter(gzWriter)
	defer file.Close()

	log.WithField("filename", file.Name()).Debug("creating new archive file")

	var rows *sqlx.Rows

	if task.ArchiveType == MessageType {
		rows, err = db.QueryxContext(ctx, lookupMsgs, task.Org.ID, task.StartDate, task.EndDate)
	} else if task.ArchiveType == FlowRunType {
		rows, err = db.QueryxContext(ctx, lookupFlowRuns, task.Org.ID, task.StartDate, task.EndDate)
	}
	if err != nil {
		return err
	}
	defer rows.Close()

	recordCount := 0
	var msg, visibility string
	for rows.Next() {
		if task.ArchiveType == MessageType {
			err = rows.Scan(&visibility, &msg)
			if err != nil {
				return err
			}

			// skip over deleted rows
			if visibility == "deleted" {
				continue
			}
		} else if task.ArchiveType == FlowRunType {
			err = rows.Scan(&msg)
			if err != nil {
				return err
			}
		}

		writer.WriteString(msg)
		writer.WriteString("\n")
		recordCount++

		if recordCount%100000 == 0 {
			log.WithField("filename", file.Name()).WithField("record_count", recordCount).WithField("elapsed", time.Now().Sub(task.BuildStart)).Debug("writing archive file")
		}
	}

	task.Filename = file.Name()
	err = writer.Flush()
	if err != nil {
		return err
	}

	err = gzWriter.Close()
	if err != nil {
		return err
	}

	// calculate our size and hash
	task.ArchiveHash = fmt.Sprintf("%x", hash.Sum(nil))
	stat, err := file.Stat()
	if err != nil {
		return err
	}
	task.ArchiveSize = int(stat.Size())
	task.RecordCount = recordCount

	log.WithFields(logrus.Fields{
		"record_count": recordCount,
		"filename":     file.Name(),
		"file_size":    task.ArchiveSize,
		"file_hash":    task.ArchiveHash,
		"elapsed":      time.Now().Sub(task.BuildStart),
	}).Debug("completed writing archive file")
	return nil
}

func UploadArchive(ctx context.Context, s3Client s3iface.S3API, bucket string, task *ArchiveTask) error {
	// s3 wants a base64 encoded hash instead of our hex encoded
	hashBytes, _ := hex.DecodeString(task.ArchiveHash)
	hashBase64 := base64.StdEncoding.EncodeToString(hashBytes)

	url, err := s3.PutS3File(
		s3Client,
		bucket,
		fmt.Sprintf("/%d/%s_%d_%02d_%s.jsonl.gz", task.Org.ID, task.ArchiveType, task.StartDate.Year(), task.StartDate.Month(), task.ArchiveHash),
		"application/json",
		"gzip",
		task.Filename,
		hashBase64,
	)

	if err == nil {
		task.ArchiveURL = url
		logrus.WithFields(logrus.Fields{
			"org_id":       task.Org.ID,
			"archive_type": task.ArchiveType,
			"start_date":   task.StartDate,
			"end_date":     task.EndDate,
			"url":          task.ArchiveURL,
			"file_size":    task.ArchiveSize,
			"file_hash":    task.ArchiveHash,
		}).Debug("completed uploading archive file")
	}
	return err
}

const insertArchive = `
INSERT INTO archives_archive(archive_type, org_id, created_on, start_date, end_date, record_count, archive_size, archive_hash, archive_url, is_purged, build_time)
              VALUES(:archive_type, :org_id, :created_on, :start_date, :end_date, :record_count, :archive_size, :archive_hash, :archive_url, :is_purged, :build_time)
RETURNING id
`

func WriteArchiveToDB(ctx context.Context, db *sqlx.DB, task *ArchiveTask) error {
	task.CreatedOn = time.Now()
	task.IsPurged = false
	task.BuildTime = int(time.Now().Sub(task.BuildStart) / time.Millisecond)
	task.OrgID = task.Org.ID

	rows, err := db.NamedQueryContext(ctx, insertArchive, task)
	if err != nil {
		return err
	}
	defer rows.Close()

	rows.Next()
	err = rows.Scan(&task.ID)
	if err != nil {
		return err
	}

	return nil
}

func DeleteTemporaryArchive(task *ArchiveTask) error {
	err := os.Remove(task.Filename)

	if err != nil {
		return err
	}

	log := logrus.WithFields(logrus.Fields{
		"org_id":        task.Org.ID,
		"archive_type":  task.ArchiveType,
		"start_date":    task.StartDate,
		"end_date":      task.EndDate,
		"db_archive_id": task.ID,
	})
	log.WithField("filename", task.Filename).Debug("Deleted temporary archive file")
	return nil
}

func ExecuteArchiving(ctx context.Context, config *Config, db *sqlx.DB, s3Client *aws_s3.S3, org Org, archiveType ArchiveType) ([]ArchiveTask, error) {
	log := logrus.WithField("org", org.Name).WithField("org_id", org.ID)

	orgRecords := 0
	orgStart := time.Now()

	tasks, err := GetArchiveTasks(ctx, db, org, archiveType)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("error calculating tasks for type '%s'", archiveType))
	}

	for _, task := range tasks {
		log = log.WithField("start_date", task.StartDate).WithField("end_date", task.EndDate).WithField("archive_type", task.ArchiveType)
		log.Info("starting archive")
		err := CreateMsgArchive(ctx, db, &task, config.TempDir)
		if err != nil {
			log.WithError(err).Error("error writing archive file")
			continue
		}

		if config.UploadToS3 {
			err = UploadArchive(ctx, s3Client, config.S3Bucket, &task)
			if err != nil {
				log.WithError(err).Error("error writing archive to s3")
				continue
			}
		}

		err = WriteArchiveToDB(ctx, db, &task)
		if err != nil {
			log.WithError(err).Error("error writing record to db")
			continue
		}

		if config.DeleteAfterUpload == true {
			err := DeleteTemporaryArchive(&task)
			if err != nil {
				log.WithError(err).Error("error deleting temporary file")
				continue
			}
		}

		log.WithField("id", task.ID).WithField("record_count", task.RecordCount).WithField("elapsed", time.Now().Sub(task.BuildStart)).Info("archive complete")
		orgRecords += task.RecordCount
	}

	if len(tasks) > 0 {
		elapsed := time.Now().Sub(orgStart)
		rate := float32(orgRecords) / (float32(elapsed) / float32(time.Second))
		log.WithField("elapsed", elapsed).WithField("records_per_second", int(rate)).Info("completed archival for org")
	}

	return tasks, nil
}
