package archives

import (
	"bufio"
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const lookupMsgs = `
SELECT rec.visibility, row_to_json(rec) FROM (
	SELECT
	  mm.id,
	  broadcast_id as broadcast,
	  row_to_json(contact) as contact,
	  CASE WHEN oo.is_anon = False THEN ccu.identity ELSE null END as urn,
	  row_to_json(channel) as channel,
	  row_to_json(flow) as flow,
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
		WHEN status = 'S' then 'sent'
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
	  sent_on,
	  mm.modified_on as modified_on
	FROM msgs_msg mm 
	  JOIN orgs_org oo ON mm.org_id = oo.id
	  JOIN LATERAL (select uuid, name from contacts_contact cc where cc.id = mm.contact_id) as contact ON True
	  LEFT JOIN contacts_contacturn ccu ON mm.contact_urn_id = ccu.id
	  LEFT JOIN LATERAL (select uuid, name from channels_channel ch where ch.id = mm.channel_id) as channel ON True
	  LEFT JOIN LATERAL (select uuid, name from flows_flow f where f.id = mm.flow_id) as flow ON True
	  LEFT JOIN LATERAL (select coalesce(jsonb_agg(label_row), '[]'::jsonb) as data from (select uuid, name from msgs_label ml INNER JOIN msgs_msg_labels mml ON ml.id = mml.label_id AND mml.msg_id = mm.id) as label_row) as labels_agg ON True

	  WHERE mm.org_id = $1 AND mm.created_on >= $2 AND mm.created_on < $3
	ORDER BY created_on ASC, id ASC) rec; 
`

// writeMessageRecords writes the messages in the archive's date range to the passed in writer
func writeMessageRecords(ctx context.Context, db *sqlx.DB, archive *Archive, writer *bufio.Writer) (int, error) {
	var rows *sqlx.Rows
	recordCount := 0

	// first write our normal records
	var record, visibility string

	rows, err := db.QueryxContext(ctx, lookupMsgs, archive.Org.ID, archive.StartDate, archive.endDate())
	if err != nil {
		return 0, errors.Wrapf(err, "error querying messages for org: %d", archive.Org.ID)
	}
	defer rows.Close()

	for rows.Next() {
		err = rows.Scan(&visibility, &record)
		if err != nil {
			return 0, errors.Wrapf(err, "error scanning message row for org: %d", archive.Org.ID)
		}

		if visibility == "deleted" {
			continue
		}
		writer.WriteString(record)
		writer.WriteString("\n")
		recordCount++
	}

	logrus.WithField("record_count", recordCount).Debug("Done Writing")
	return recordCount, nil
}

const selectOrgMessagesInRange = `
SELECT mm.id, mm.visibility
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

const deleteMessages = `
DELETE FROM msgs_msg 
WHERE id IN(?)
`

// DeleteArchivedMessages takes the passed in archive, verifies the S3 file is still present (and correct), then selects
// all the messages in the archive date range, and if equal or fewer than the number archived, deletes them 100 at a time
//
// Upon completion it updates the needs_deletion flag on the archive
func DeleteArchivedMessages(ctx context.Context, config *Config, db *sqlx.DB, s3Client s3iface.S3API, archive *Archive) error {
	outer, cancel := context.WithTimeout(ctx, time.Hour*3)
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
	msgIDs := make([]int64, 0, archive.RecordCount)
	for rows.Next() {
		err = rows.Scan(&msgID, &visibility)
		if err != nil {
			return err
		}
		msgIDs = append(msgIDs, msgID)

		// keep track of the number of visible messages, ie, not deleted
		if visibility != "D" {
			visibleCount++
		}
	}
	rows.Close()

	log.WithField("msg_count", len(msgIDs)).Debug("found messages")

	// verify we don't see more messages than there are in our archive (fewer is ok)
	if visibleCount > archive.RecordCount {
		return fmt.Errorf("more messages in the database: %d than in archive: %d", visibleCount, archive.RecordCount)
	}

	// ok, delete our messages in batches, we do this in transactions as it spans a few different queries
	for _, idBatch := range chunkIDs(msgIDs, deleteTransactionSize) {
		// no single batch should take more than a few minutes
		ctx, cancel := context.WithTimeout(ctx, time.Minute*15)
		defer cancel()

		start := time.Now()

		// start our transaction
		tx, err := db.BeginTxx(ctx, nil)
		if err != nil {
			return err
		}

		// first update our delete_reason
		err = executeInQuery(ctx, tx, setMessageDeleteReason, idBatch)
		if err != nil {
			return errors.Wrap(err, "error updating delete reason")
		}

		// now delete any channel logs
		err = executeInQuery(ctx, tx, deleteMessageLogs, idBatch)
		if err != nil {
			return errors.Wrap(err, "error removing channel logs")
		}

		// then any labels
		err = executeInQuery(ctx, tx, deleteMessageLabels, idBatch)
		if err != nil {
			return errors.Wrap(err, "error removing message labels")
		}

		// finally, delete our messages
		err = executeInQuery(ctx, tx, deleteMessages, idBatch)
		if err != nil {
			return errors.Wrap(err, "error deleting messages")
		}

		// commit our transaction
		err = tx.Commit()
		if err != nil {
			return errors.Wrap(err, "error committing message delete transaction")
		}

		log.WithField("elapsed", time.Since(start)).WithField("count", len(idBatch)).Debug("deleted batch of messages")

		cancel()
	}

	outer, cancel = context.WithTimeout(ctx, time.Minute)
	defer cancel()

	deletedOn := time.Now()

	// all went well! mark our archive as no longer needing deletion
	_, err = db.ExecContext(outer, setArchiveDeleted, archive.ID, deletedOn)
	if err != nil {
		return errors.Wrap(err, "error setting archive as deleted")
	}
	archive.NeedsDeletion = false
	archive.DeletedOn = &deletedOn

	logrus.WithField("elapsed", time.Since(start)).Info("completed deleting messages")

	return nil
}

const selectOldOrgBroadcasts = `
SELECT 
	id
FROM 
	msgs_broadcast
WHERE 
	org_id = $1 AND
	created_on < $2 AND
	schedule_id IS NULL
ORDER BY 
	created_on ASC,
	id ASC
LIMIT 1000000;
`

// DeleteBroadcasts deletes all broadcasts older than 90 days for the passed in org which have no active messages on them
func DeleteBroadcasts(ctx context.Context, now time.Time, config *Config, db *sqlx.DB, org Org) error {
	start := time.Now()
	threshhold := now.AddDate(0, 0, -org.RetentionPeriod)

	rows, err := db.QueryxContext(ctx, selectOldOrgBroadcasts, org.ID, threshhold)
	if err != nil {
		return err
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		if count == 0 {
			logrus.WithField("org_id", org.ID).Info("deleting broadcasts")
		}

		// been deleting this org more than an hour? thats enough for today, exit out
		if time.Since(start) > time.Hour {
			break
		}

		var broadcastID int64
		err := rows.Scan(&broadcastID)
		if err != nil {
			return errors.Wrap(err, "unable to get broadcast id")
		}

		// make sure we have no active messages
		var msgCount int64
		err = db.Get(&msgCount, `SELECT count(*) FROM msgs_msg WHERE broadcast_id = $1`, broadcastID)
		if err != nil {
			return errors.Wrapf(err, "unable to select number of msgs for broadcast: %d", broadcastID)
		}

		if msgCount != 0 {
			logrus.WithField("broadcast_id", broadcastID).WithField("org_id", org.ID).WithField("msg_count", msgCount).Warn("unable to delete broadcast, has messages still")
			continue
		}

		// we delete broadcasts in a transaction per broadcast
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return errors.Wrapf(err, "error starting transaction while deleting broadcast: %d", broadcastID)
		}

		// delete contacts M2M
		_, err = tx.Exec(`DELETE from msgs_broadcast_contacts WHERE broadcast_id = $1`, broadcastID)
		if err != nil {
			tx.Rollback()
			return errors.Wrapf(err, "error deleting related contacts for broadcast: %d", broadcastID)
		}

		// delete groups M2M
		_, err = tx.Exec(`DELETE from msgs_broadcast_groups WHERE broadcast_id = $1`, broadcastID)
		if err != nil {
			tx.Rollback()
			return errors.Wrapf(err, "error deleting related groups for broadcast: %d", broadcastID)
		}

		// delete URNs M2M
		_, err = tx.Exec(`DELETE from msgs_broadcast_urns WHERE broadcast_id = $1`, broadcastID)
		if err != nil {
			tx.Rollback()
			return errors.Wrapf(err, "error deleting related urns for broadcast: %d", broadcastID)
		}

		// delete counts associated with this broadcast
		_, err = tx.Exec(`DELETE from msgs_broadcastmsgcount WHERE broadcast_id = $1`, broadcastID)
		if err != nil {
			tx.Rollback()
			return errors.Wrapf(err, "error deleting counts for broadcast: %d", broadcastID)
		}

		// finally, delete our broadcast
		_, err = tx.Exec(`DELETE from msgs_broadcast WHERE id = $1`, broadcastID)
		if err != nil {
			tx.Rollback()
			return errors.Wrapf(err, "error deleting broadcast: %d", broadcastID)
		}

		err = tx.Commit()
		if err != nil {
			return errors.Wrapf(err, "error deleting broadcast: %d", broadcastID)
		}

		count++
	}

	if count > 0 {
		logrus.WithFields(logrus.Fields{
			"elapsed": time.Since(start),
			"count":   count,
			"org_id":  org.ID,
		}).Info("completed deleting broadcasts")
	}

	return nil
}
