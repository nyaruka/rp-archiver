package archives

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/gocommon/dates"
)

const (
	visibilityDeletedByUser   = "D"
	visibilityDeletedBySender = "X"
)

const sqlLookupMsgs = `
SELECT rec.visibility, row_to_json(rec) FROM (
	SELECT
		mm.id,
		broadcast_id as broadcast,
		row_to_json(contact) as contact,
		CASE WHEN oo.is_anon = FALSE THEN ccu.identity ELSE NULL END AS urn,
		row_to_json(channel) as channel,
		row_to_json(flow) as flow,
		CASE WHEN direction = 'I' THEN 'in' WHEN direction = 'O' THEN 'out' ELSE NULL END AS direction,
		CASE 
			WHEN msg_type = 'T' THEN 'text'
			WHEN msg_type = 'O' THEN 'optin'
			WHEN msg_type = 'V' THEN 'voice'
			ELSE NULL 
		END AS "type",
		CASE 
			WHEN status = 'I' THEN 'initializing'
			WHEN status = 'P' THEN 'queued'
			WHEN status = 'Q' THEN 'queued'
			WHEN status = 'W' THEN 'wired'
			WHEN status = 'D' THEN 'delivered'
			WHEN status = 'H' THEN 'handled'
			WHEN status = 'E' THEN 'errored'
			WHEN status = 'F' THEN 'failed'
			WHEN status = 'S' THEN 'sent'
			WHEN status = 'R' THEN 'read'
			ELSE NULL 
		END AS status,
		CASE WHEN visibility = 'V' THEN 'visible' WHEN visibility = 'A' THEN 'archived' WHEN visibility = 'D' THEN 'deleted' WHEN visibility = 'X' THEN 'deleted' ELSE NULL END as visibility,
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
ORDER BY created_on ASC, id ASC) rec;`

// writeMessageRecords writes the messages in the archive's date range to the passed in writer
func writeMessageRecords(ctx context.Context, db *sqlx.DB, archive *Archive, writer *bufio.Writer) (int, error) {
	var rows *sqlx.Rows
	recordCount := 0

	// first write our normal records
	var record, visibility string

	rows, err := db.QueryxContext(ctx, sqlLookupMsgs, archive.Org.ID, archive.StartDate, archive.endDate())
	if err != nil {
		return 0, fmt.Errorf("error querying messages for org: %d: %w", archive.Org.ID, err)
	}
	defer rows.Close()

	for rows.Next() {
		err = rows.Scan(&visibility, &record)
		if err != nil {
			return 0, fmt.Errorf("error scanning message row for org: %d: %w", archive.Org.ID, err)
		}

		if visibility == "deleted" {
			continue
		}
		writer.WriteString(record)
		writer.WriteString("\n")
		recordCount++
	}

	slog.Debug("Done Writing", "record_count", recordCount)
	return recordCount, nil
}

const sqlSelectOrgMessagesInRange = `
   SELECT mm.id, mm.visibility
     FROM msgs_msg mm
LEFT JOIN contacts_contact cc ON cc.id = mm.contact_id
    WHERE mm.org_id = $1 AND mm.created_on >= $2 AND mm.created_on < $3
 ORDER BY mm.created_on ASC, mm.id ASC`

const sqlDeleteMessageLabels = `
DELETE FROM msgs_msg_labels WHERE msg_id IN(?)`

const sqlDeleteMessages = `
DELETE FROM msgs_msg WHERE id IN(?)`

// DeleteArchivedMessages takes the passed in archive, verifies the S3 file is still present (and correct), then selects
// all the messages in the archive date range, and if equal or fewer than the number archived, deletes them 100 at a time
//
// Upon completion it updates the needs_deletion flag on the archive
func DeleteArchivedMessages(ctx context.Context, config *Config, db *sqlx.DB, s3Client s3iface.S3API, archive *Archive) error {
	outer, cancel := context.WithTimeout(ctx, time.Hour*3)
	defer cancel()

	start := dates.Now()
	log := slog.With(
		"id", archive.ID,
		"org_id", archive.OrgID,
		"start_date", archive.StartDate,
		"end_date", archive.endDate(),
		"archive_type", archive.ArchiveType,
		"total_count", archive.RecordCount,
	)
	log.Info("deleting messages")

	// first things first, make sure our file is correct on S3
	s3Size, s3Hash, err := GetS3FileInfo(outer, s3Client, archive.URL)
	if err != nil {
		return err
	}

	if s3Size != archive.Size {
		return fmt.Errorf("archive size: %d and s3 size: %d do not match", archive.Size, s3Size)
	}

	// if S3 hash is MD5 then check against archive hash
	if config.CheckS3Hashes && archive.Size <= maxSingleUploadBytes && s3Hash != archive.Hash {
		return fmt.Errorf("archive md5: %s and s3 etag: %s do not match", archive.Hash, s3Hash)
	}

	// ok, archive file looks good, let's build up our list of message ids, this may be big but we are int64s so shouldn't be too big
	rows, err := db.QueryxContext(outer, sqlSelectOrgMessagesInRange, archive.OrgID, archive.StartDate, archive.endDate())
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
		if visibility != visibilityDeletedByUser && visibility != visibilityDeletedBySender {
			visibleCount++
		}
	}
	rows.Close()

	log.Debug("found messages", "msg_count", len(msgIDs))

	// verify we don't see more messages than there are in our archive (fewer is ok)
	if visibleCount > archive.RecordCount {
		return fmt.Errorf("more messages in the database: %d than in archive: %d", visibleCount, archive.RecordCount)
	}

	// ok, delete our messages in batches, we do this in transactions as it spans a few different queries
	for _, idBatch := range chunkIDs(msgIDs, deleteTransactionSize) {
		// no single batch should take more than a few minutes
		ctx, cancel := context.WithTimeout(ctx, time.Minute*15)
		defer cancel()

		start := dates.Now()

		// start our transaction
		tx, err := db.BeginTxx(ctx, nil)
		if err != nil {
			return err
		}

		// first delete any labelings
		err = executeInQuery(ctx, tx, sqlDeleteMessageLabels, idBatch)
		if err != nil {
			return fmt.Errorf("error removing message labels: %w", err)
		}

		// then delete the messages themselves
		err = executeInQuery(ctx, tx, sqlDeleteMessages, idBatch)
		if err != nil {
			return fmt.Errorf("error deleting messages: %w", err)
		}

		// commit our transaction
		err = tx.Commit()
		if err != nil {
			return fmt.Errorf("error committing message delete transaction: %w", err)
		}

		log.Debug("deleted batch of messages", "elapsed", dates.Since(start), "count", len(idBatch))

		cancel()
	}

	outer, cancel = context.WithTimeout(ctx, time.Minute)
	defer cancel()

	deletedOn := dates.Now()

	// all went well! mark our archive as no longer needing deletion
	_, err = db.ExecContext(outer, sqlUpdateArchiveDeleted, archive.ID, deletedOn)
	if err != nil {
		return fmt.Errorf("error setting archive as deleted: %w", err)
	}
	archive.NeedsDeletion = false
	archive.DeletedOn = &deletedOn

	slog.Info("completed deleting messages", "elapsed", dates.Since(start))

	return nil
}

const sqlSelectOldOrgBroadcasts = `
SELECT id
  FROM msgs_broadcast b
 WHERE b.org_id = $1 AND b.created_on < $2 AND b.schedule_id IS NULL AND b.is_active AND NOT EXISTS (SELECT 1 FROM msgs_msg WHERE broadcast_id = b.id)
 LIMIT 1000000;`

// DeleteBroadcasts deletes all broadcasts older than 90 days for the passed in org which have no associated messages
func DeleteBroadcasts(ctx context.Context, now time.Time, config *Config, db *sqlx.DB, org Org) error {
	start := dates.Now()
	threshhold := now.AddDate(0, 0, -org.RetentionPeriod)

	rows, err := db.QueryxContext(ctx, sqlSelectOldOrgBroadcasts, org.ID, threshhold)
	if err != nil {
		return err
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		if count == 0 {
			slog.Info("deleting broadcasts", "org_id", org.ID)

		}

		// been deleting this org more than an hour? thats enough for today, exit out
		if dates.Since(start) > time.Hour {
			break
		}

		var broadcastID int64
		if err := rows.Scan(&broadcastID); err != nil {
			return fmt.Errorf("unable to get broadcast id: %w", err)
		}

		// we delete broadcasts in a transaction per broadcast
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("error starting transaction while deleting broadcast: %d: %w", broadcastID, err)
		}

		// delete contacts M2M
		_, err = tx.Exec(`DELETE from msgs_broadcast_contacts WHERE broadcast_id = $1`, broadcastID)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("error deleting related contacts for broadcast: %d: %w", broadcastID, err)
		}

		// delete groups M2M
		_, err = tx.Exec(`DELETE from msgs_broadcast_groups WHERE broadcast_id = $1`, broadcastID)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("error deleting related groups for broadcast: %d: %w", broadcastID, err)
		}

		// delete counts associated with this broadcast
		_, err = tx.Exec(`DELETE from msgs_broadcastmsgcount WHERE broadcast_id = $1`, broadcastID)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("error deleting counts for broadcast: %d: %w", broadcastID, err)
		}

		// finally, delete our broadcast
		_, err = tx.Exec(`DELETE from msgs_broadcast WHERE id = $1`, broadcastID)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("error deleting broadcast: %d: %w", broadcastID, err)
		}

		err = tx.Commit()
		if err != nil {
			return fmt.Errorf("error deleting broadcast: %d: %w", broadcastID, err)
		}

		count++
	}

	if count > 0 {
		slog.Info("completed deleting broadcasts", "elapsed", dates.Since(start), "count", count, "org_id", org.ID)
	}

	return nil
}
