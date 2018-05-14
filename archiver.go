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

	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/rp-archiver/s3"
	"github.com/sirupsen/logrus"
)

type ArchiveType string

const (
	FlowRunType = ArchiveType("flowrun")
	MessageType = ArchiveType("message")
	SessionType = ArchiveType("session")
)

type DBOrg struct {
	ID         int       `db:"id"`
	Name       string    `db:"name"`
	CreatedOn  time.Time `db:"created_on"`
	ActiveDays int
}

type DBArchive struct {
	ID              int       `db:"id"`
	ArchiveType     string    `db:"archive_type"`
	OrgID           int       `db:"org_id"`
	CreatedOn       time.Time `db:"created_on"`
	ArchiveDuration int       `db:"archive_duration"`

	StartDate time.Time `db:"start_date"`
	EndDate   time.Time `db:"end_date"`

	RecordCount int `db:"record_count"`

	ArchiveSize int    `db:"archive_size"`
	ArchiveHash string `db:"archive_hash"`
	ArchiveURL  string `db:"archive_url"`

	IsPurged  bool `db:"is_purged"`
	BuildTime int  `db:"build_time"`
}

type ArchiveTask struct {
	Org         DBOrg
	ArchiveType ArchiveType
	StartDate   time.Time
	EndDate     time.Time

	ID int

	RecordCount int
	Filename    string
	FileSize    int64
	FileHash    string
	URL         string

	BuildStart time.Time
}

func addMonth(t time.Time) time.Time {
	monthLater := t.Add(time.Hour * 24 * 31)
	return time.Date(monthLater.Year(), monthLater.Month(), 1, 0, 0, 0, 0, time.UTC)
}

const lookupActiveOrgs = `SELECT id, name, created_on FROM orgs_org WHERE is_active = TRUE`

func GetActiveOrgs(ctx context.Context, db *sqlx.DB) ([]DBOrg, error) {
	rows, err := db.QueryxContext(ctx, lookupActiveOrgs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	orgs := make([]DBOrg, 0, 10)
	for rows.Next() {
		org := DBOrg{ActiveDays: 90}
		err = rows.StructScan(&org)
		if err != nil {
			return nil, err
		}
		orgs = append(orgs, org)
	}

	return orgs, nil
}

const lookupLastArchive = `SELECT start_date, end_date FROM archives_archive WHERE org_id = $1 AND archive_type = $2 ORDER BY end_date DESC LIMIT 1`

func GetArchiveTasks(ctx context.Context, db *sqlx.DB, org DBOrg, archiveType ArchiveType) ([]ArchiveTask, error) {
	archive := DBArchive{}
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
	  ccu.identity as urn,
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
	  (select coalesce(jsonb_agg(attach_row), '[]'::jsonb) FROM (select attach_data.attachment[1] as content_type, attach_data.attachment[2] as url FROM (select regexp_matches(unnest(attachments), '^(.*?);(.*)$') attachment FROM msgs_msg where id = 8) as attach_data) as attach_row) as attachments,
	  labels_agg.data as labels,
	  created_on,
	  sent_on
	from msgs_msg mm JOIN contacts_contacturn ccu ON mm.contact_urn_id = ccu.id
	  JOIN LATERAL (select uuid, name from contacts_contact cc where cc.id = mm.contact_id) as contact ON True
	  JOIN LATERAL (select uuid, name from channels_channel ch where ch.id = mm.channel_id) as channel ON True
	  LEFT JOIN LATERAL (select coalesce(jsonb_agg(label_row), '[]'::jsonb) as data from (select uuid, name from msgs_label ml INNER JOIN msgs_msg_labels mml ON ml.id = mml.label_id AND mml.msg_id = mm.id) as label_row) as labels_agg ON True

	  WHERE mm.org_id = $1 AND mm.created_on >= $2 AND mm.created_on < $3
	order by created_on ASC, id ASC) rec; 
`

func CreateMsgArchive(ctx context.Context, db *sqlx.DB, task *ArchiveTask) error {
	task.BuildStart = time.Now()

	filename := fmt.Sprintf("%s_%d_%d%02d_%d%02d_", task.ArchiveType, task.Org.ID, task.StartDate.Year(), task.StartDate.Month(), task.EndDate.Year(), task.EndDate.Month())
	log := logrus.WithFields(logrus.Fields{
		"org_id":       task.Org.ID,
		"archive_type": task.ArchiveType,
		"start_date":   task.StartDate,
		"end_date":     task.EndDate,
	})

	file, err := ioutil.TempFile("/tmp/archiver", filename)
	if err != nil {
		return err
	}
	hash := md5.New()
	gzWriter := gzip.NewWriter(io.MultiWriter(file, hash))
	writer := bufio.NewWriter(gzWriter)
	defer file.Close()

	log.WithField("filename", file.Name()).Debug("creating new archive file")

	rows, err := db.QueryxContext(ctx, lookupMsgs, task.Org.ID, task.StartDate, task.EndDate)
	if err != nil {
		return err
	}
	defer rows.Close()

	recordCount := 0
	var msg, visibility string
	for rows.Next() {
		err = rows.Scan(&visibility, &msg)
		if err != nil {
			return err
		}

		// skip over deleted rows
		if visibility == "deleted" {
			continue
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
	task.FileHash = fmt.Sprintf("%x", hash.Sum(nil))
	stat, err := file.Stat()
	if err != nil {
		return err
	}
	task.FileSize = stat.Size()
	task.RecordCount = recordCount

	log.WithFields(logrus.Fields{
		"record_count": recordCount,
		"filename":     file.Name(),
		"file_size":    task.FileSize,
		"file_hash":    task.FileHash,
		"elapsed":      time.Now().Sub(task.BuildStart),
	}).Debug("completed writing archive file")
	return nil
}

func UploadArchive(ctx context.Context, s3Client s3iface.S3API, bucket string, task *ArchiveTask) error {
	// s3 wants a base64 encoded hash instead of our hex encoded
	hashBytes, _ := hex.DecodeString(task.FileHash)
	hashBase64 := base64.StdEncoding.EncodeToString(hashBytes)

	url, err := s3.PutS3File(
		s3Client,
		bucket,
		fmt.Sprintf("/%d/%s_%d_%02d_%s.jsonl.gz", task.Org.ID, task.ArchiveType, task.StartDate.Year(), task.StartDate.Month(), task.FileHash),
		"application/json",
		"gzip",
		task.Filename,
		hashBase64,
	)

	if err == nil {
		task.URL = url
		logrus.WithFields(logrus.Fields{
			"org_id":       task.Org.ID,
			"archive_type": task.ArchiveType,
			"start_date":   task.StartDate,
			"end_date":     task.EndDate,
			"url":          task.URL,
			"file_size":    task.FileSize,
			"file_hash":    task.FileHash,
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
	dbArchive := DBArchive{
		ArchiveType: string(task.ArchiveType),
		OrgID:       task.Org.ID,
		CreatedOn:   time.Now(),

		StartDate: task.StartDate,
		EndDate:   task.EndDate,

		RecordCount: task.RecordCount,

		ArchiveSize: int(task.FileSize),
		ArchiveHash: task.FileHash,
		ArchiveURL:  task.URL,

		IsPurged:  false,
		BuildTime: int(time.Now().Sub(task.BuildStart) / time.Millisecond),
	}

	rows, err := db.NamedQueryContext(ctx, insertArchive, dbArchive)
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
