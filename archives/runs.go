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

const (
	RunStatusActive      = "A"
	RunStatusWaiting     = "W"
	RunStatusCompleted   = "C"
	RunStatusExpired     = "X"
	RunStatusInterrupted = "I"
	RunStatusFailed      = "F"
)

const lookupFlowRuns = `
SELECT rec.exited_on, row_to_json(rec)
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
) as rec;
`

// writeRunRecords writes the runs in the archive's date range to the passed in writer
func writeRunRecords(ctx context.Context, db *sqlx.DB, archive *Archive, writer *bufio.Writer) (int, error) {
	var rows *sqlx.Rows
	rows, err := db.QueryxContext(ctx, lookupFlowRuns, archive.Org.ID, archive.StartDate, archive.endDate())
	if err != nil {
		return 0, errors.Wrapf(err, "error querying run records for org: %d", archive.Org.ID)
	}
	defer rows.Close()

	recordCount := 0
	var record string
	var exitedOn *time.Time
	for rows.Next() {
		err = rows.Scan(&exitedOn, &record)

		// shouldn't be archiving an active run, that's an error
		if exitedOn == nil {
			return 0, fmt.Errorf("run still active, cannot archive: %s", record)
		}

		if err != nil {
			return 0, errors.Wrapf(err, "error scanning run record for org: %d", archive.Org.ID)
		}

		writer.WriteString(record)
		writer.WriteString("\n")
		recordCount++
	}

	return recordCount, nil
}

const selectOrgRunsInRange = `
SELECT fr.id, fr.status
FROM flows_flowrun fr
LEFT JOIN contacts_contact cc ON cc.id = fr.contact_id
WHERE fr.org_id = $1 AND fr.modified_on >= $2 AND fr.modified_on < $3
ORDER BY fr.modified_on ASC, fr.id ASC
`

const setRunDeleteReason = `
UPDATE flows_flowrun
SET delete_reason = 'A' 
WHERE id IN(?)
`

const deleteRuns = `
DELETE FROM flows_flowrun
WHERE id IN(?)
`

// DeleteArchivedRuns takes the passed in archive, verifies the S3 file is still present (and correct), then selects
// all the runs in the archive date range, and if equal or fewer than the number archived, deletes them 100 at a time
//
// Upon completion it updates the needs_deletion flag on the archive
func DeleteArchivedRuns(ctx context.Context, config *Config, db *sqlx.DB, s3Client s3iface.S3API, archive *Archive) error {
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
	log.Info("deleting runs")

	// first things first, make sure our file is present on S3
	md5, err := GetS3FileETAG(outer, s3Client, archive.URL)
	if err != nil {
		return err
	}

	// if our etag and archive md5 don't match, that's an error, return
	if md5 != archive.Hash {
		return fmt.Errorf("archive md5: %s and s3 etag: %s do not match", archive.Hash, md5)
	}

	// ok, archive file looks good, let's build up our list of run ids, this may be big but we are int64s so shouldn't be too big
	rows, err := db.QueryxContext(outer, selectOrgRunsInRange, archive.OrgID, archive.StartDate, archive.endDate())
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

		start := time.Now()

		// start our transaction
		tx, err := db.BeginTxx(ctx, nil)
		if err != nil {
			return err
		}

		// first update our delete_reason
		err = executeInQuery(ctx, tx, setRunDeleteReason, idBatch)
		if err != nil {
			return errors.Wrap(err, "error updating delete reason")
		}

		// finally, delete our runs
		err = executeInQuery(ctx, tx, deleteRuns, idBatch)
		if err != nil {
			return errors.Wrap(err, "error deleting runs")
		}

		// commit our transaction
		err = tx.Commit()
		if err != nil {
			return errors.Wrap(err, "error committing run delete transaction")
		}

		log.WithField("elapsed", time.Since(start)).WithField("count", len(idBatch)).Debug("deleted batch of runs")

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

	logrus.WithField("elapsed", time.Since(start)).Info("completed deleting runs")

	return nil
}
