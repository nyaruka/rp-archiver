package archives

import (
	"bufio"
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/jmoiron/sqlx"
	"github.com/nyaruka/gocommon/dates"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const (
	RunStatusActive      = "A"
	RunStatusWaiting     = "W"
	RunStatusCompleted   = "C"
	RunStatusExpired     = "X"
	RunStatusInterrupted = "I"
	RunStatusFailed      = "F"
)

const sqlLookupRuns = `
SELECT rec.uuid, rec.exited_on, row_to_json(rec)
FROM (
   SELECT
	 fr.id as id,
	 fr.uuid as uuid,
     row_to_json(flow_struct) AS flow,
     row_to_json(contact_struct) AS contact,
     fr.responded,
     (SELECT coalesce(jsonb_agg(path_data), '[]'::jsonb) from (
		SELECT path_row ->> 'node_uuid' AS node, (path_row ->> 'arrived_on')::timestamptz as time
		FROM jsonb_array_elements(fr.path::jsonb) AS path_row LIMIT 500) as path_data
     ) as path,
     (SELECT coalesce(jsonb_object_agg(values_data.key, values_data.value), '{}'::jsonb) from (
		SELECT key, jsonb_build_object('name', value -> 'name', 'value', value -> 'value', 'input', value -> 'input', 'time', (value -> 'created_on')::text::timestamptz, 'category', value -> 'category', 'node', value -> 'node_uuid') as value
		FROM jsonb_each(fr.results::jsonb)) AS values_data
	 ) as values,
     fr.created_on,
     fr.modified_on,
	 fr.exited_on,
     CASE
        WHEN status = 'C' THEN 'completed'
        WHEN status = 'I' THEN 'interrupted'
        WHEN status = 'X' THEN 'expired'
        WHEN status = 'F' THEN 'failed'
        ELSE NULL
	 END as exit_type,
 	 a.username as submitted_by

   FROM flows_flowrun fr
     LEFT JOIN auth_user a ON a.id = fr.submitted_by_id
     JOIN LATERAL (SELECT uuid, name FROM flows_flow WHERE flows_flow.id = fr.flow_id) AS flow_struct ON True
     JOIN LATERAL (SELECT uuid, name FROM contacts_contact cc WHERE cc.id = fr.contact_id) AS contact_struct ON True
   
   WHERE fr.org_id = $1 AND fr.modified_on >= $2 AND fr.modified_on < $3
   ORDER BY fr.modified_on ASC, id ASC
) as rec;`

// writeRunRecords writes the runs in the archive's date range to the passed in writer
func writeRunRecords(ctx context.Context, db *sqlx.DB, archive *Archive, writer *bufio.Writer) (int, error) {
	var rows *sqlx.Rows
	rows, err := db.QueryxContext(ctx, sqlLookupRuns, archive.Org.ID, archive.StartDate, archive.endDate())
	if err != nil {
		return 0, errors.Wrapf(err, "error querying run records for org: %d", archive.Org.ID)
	}
	defer rows.Close()

	recordCount := 0

	var runUUID string
	var runExitedOn *time.Time
	var record string

	for rows.Next() {
		err = rows.Scan(&runUUID, &runExitedOn, &record)

		if err != nil {
			return 0, errors.Wrapf(err, "error scanning run record for org: %d", archive.Org.ID)
		}

		// shouldn't be archiving an active run, that's an error
		if runExitedOn == nil {
			return 0, fmt.Errorf("run %s still active, cannot archive", runUUID)
		}

		writer.WriteString(record)
		writer.WriteString("\n")
		recordCount++
	}

	return recordCount, nil
}

const sqlSelectOrgRunsInRange = `
   SELECT fr.id, fr.status
     FROM flows_flowrun fr
LEFT JOIN contacts_contact cc ON cc.id = fr.contact_id
    WHERE fr.org_id = $1 AND fr.modified_on >= $2 AND fr.modified_on < $3
 ORDER BY fr.modified_on ASC, fr.id ASC`

const sqlDeleteRuns = `
DELETE FROM flows_flowrun WHERE id IN(?)`

// DeleteArchivedRuns takes the passed in archive, verifies the S3 file is still present (and correct), then selects
// all the runs in the archive date range, and if equal or fewer than the number archived, deletes them 100 at a time
//
// Upon completion it updates the needs_deletion flag on the archive
func DeleteArchivedRuns(ctx context.Context, config *Config, db *sqlx.DB, s3Client s3iface.S3API, archive *Archive) error {
	outer, cancel := context.WithTimeout(ctx, time.Hour*3)
	defer cancel()

	start := dates.Now()
	log := logrus.WithFields(logrus.Fields{
		"id":           archive.ID,
		"org_id":       archive.OrgID,
		"start_date":   archive.StartDate,
		"end_date":     archive.endDate(),
		"archive_type": archive.ArchiveType,
		"total_count":  archive.RecordCount,
	})
	log.Info("deleting runs")

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

	// ok, archive file looks good, let's build up our list of run ids, this may be big but we are int64s so shouldn't be too big
	rows, err := db.QueryxContext(outer, sqlSelectOrgRunsInRange, archive.OrgID, archive.StartDate, archive.endDate())
	if err != nil {
		return err
	}
	defer rows.Close()

	var runID int64
	var status string
	runCount := 0
	runIDs := make([]int64, 0, archive.RecordCount)
	for rows.Next() {
		err = rows.Scan(&runID, &status)
		if err != nil {
			return err
		}

		// if this run is still active, something has gone wrong, throw an error
		if status == RunStatusActive || status == RunStatusWaiting {
			return fmt.Errorf("run #%d in archive hadn't exited", runID)
		}

		// increment our count
		runCount++
		runIDs = append(runIDs, runID)
	}
	rows.Close()

	log.WithField("run_count", len(runIDs)).Debug("found runs")

	// verify we don't see more runs than there are in our archive (fewer is ok)
	if runCount > archive.RecordCount {
		return fmt.Errorf("more runs in the database: %d than in archive: %d", runCount, archive.RecordCount)
	}

	// ok, delete our runs in batches, we do this in transactions as it spans a few different queries
	for _, idBatch := range chunkIDs(runIDs, deleteTransactionSize) {
		// no single batch should take more than a few minutes
		ctx, cancel := context.WithTimeout(ctx, time.Minute*15)
		defer cancel()

		start := dates.Now()

		// start our transaction
		tx, err := db.BeginTxx(ctx, nil)
		if err != nil {
			return err
		}

		// delete our runs
		err = executeInQuery(ctx, tx, sqlDeleteRuns, idBatch)
		if err != nil {
			return errors.Wrap(err, "error deleting runs")
		}

		// commit our transaction
		err = tx.Commit()
		if err != nil {
			return errors.Wrap(err, "error committing run delete transaction")
		}

		log.WithField("elapsed", dates.Since(start)).WithField("count", len(idBatch)).Debug("deleted batch of runs")

		cancel()
	}

	outer, cancel = context.WithTimeout(ctx, time.Minute)
	defer cancel()

	deletedOn := dates.Now()

	// all went well! mark our archive as no longer needing deletion
	_, err = db.ExecContext(outer, sqlUpdateArchiveDeleted, archive.ID, deletedOn)
	if err != nil {
		return errors.Wrap(err, "error setting archive as deleted")
	}
	archive.NeedsDeletion = false
	archive.DeletedOn = &deletedOn

	logrus.WithField("elapsed", dates.Since(start)).Info("completed deleting runs")

	return nil
}

const selectOldOrgFlowStarts = `
 SELECT id
   FROM flows_flowstart s
  WHERE s.org_id = $1 AND s.created_on < $2 AND NOT EXISTS (SELECT 1 FROM flows_flowrun WHERE start_id = s.id)
  LIMIT 1000000;`

// DeleteFlowStarts deletes all starts older than 90 days for the passed in org which have no associated runs
func DeleteFlowStarts(ctx context.Context, now time.Time, config *Config, db *sqlx.DB, org Org) error {
	start := dates.Now()
	threshhold := now.AddDate(0, 0, -org.RetentionPeriod)

	rows, err := db.QueryxContext(ctx, selectOldOrgFlowStarts, org.ID, threshhold)
	if err != nil {
		return err
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		if count == 0 {
			logrus.WithField("org_id", org.ID).Info("deleting starts")
		}

		// been deleting this org more than an hour? thats enough for today, exit out
		if dates.Since(start) > time.Hour {
			break
		}

		var startID int64
		if err := rows.Scan(&startID); err != nil {
			return errors.Wrap(err, "unable to get start id")
		}

		// we delete starts in a transaction per start
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return errors.Wrapf(err, "error starting transaction while deleting start: %d", startID)
		}

		// delete contacts M2M
		_, err = tx.Exec(`DELETE from flows_flowstart_contacts WHERE flowstart_id = $1`, startID)
		if err != nil {
			tx.Rollback()
			return errors.Wrapf(err, "error deleting related contacts for start: %d", startID)
		}

		// delete groups M2M
		_, err = tx.Exec(`DELETE from flows_flowstart_groups WHERE flowstart_id = $1`, startID)
		if err != nil {
			tx.Rollback()
			return errors.Wrapf(err, "error deleting related groups for start: %d", startID)
		}

		// delete calls M2M
		_, err = tx.Exec(`DELETE from flows_flowstart_calls WHERE flowstart_id = $1`, startID)
		if err != nil {
			tx.Rollback()
			return errors.Wrapf(err, "error deleting related calls for start: %d", startID)
		}

		// delete counts
		_, err = tx.Exec(`DELETE from flows_flowstartcount WHERE start_id = $1`, startID)
		if err != nil {
			tx.Rollback()
			return errors.Wrapf(err, "error deleting counts for start: %d", startID)
		}

		// finally, delete our start
		_, err = tx.Exec(`DELETE from flows_flowstart WHERE id = $1`, startID)
		if err != nil {
			tx.Rollback()
			return errors.Wrapf(err, "error deleting start: %d", startID)
		}

		err = tx.Commit()
		if err != nil {
			return errors.Wrapf(err, "error deleting start: %d", startID)
		}

		count++
	}

	if count > 0 {
		logrus.WithFields(logrus.Fields{"elapsed": dates.Since(start), "count": count, "org_id": org.ID}).Info("completed deleting starts")
	}

	return nil
}
