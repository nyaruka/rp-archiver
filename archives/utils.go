package archives

import (
	"context"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
)

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

// counts the records in the given archives
func countRecords(as []*Archive) int {
	n := 0
	for _, a := range as {
		n += a.RecordCount
	}
	return n
}

// removes duplicates from a slice of archives
func removeDuplicates(as []*Archive) []*Archive {
	unique := make([]*Archive, 0, len(as))
	seen := make(map[string]bool)

	for _, a := range as {
		key := fmt.Sprintf("%s:%s:%s", a.ArchiveType, a.Period, a.StartDate.Format(time.RFC3339))
		if !seen[key] {
			unique = append(unique, a)
			seen[key] = true
		}
	}
	return unique
}

// chunks a slice of in64 IDs
func chunkIDs(ids []int64, size int) [][]int64 {
	chunks := make([][]int64, 0, len(ids)/size+1)

	for i := 0; i < len(ids); i += size {
		end := i + size
		if end > len(ids) {
			end = len(ids)
		}
		chunks = append(chunks, ids[i:end])
	}
	return chunks
}
