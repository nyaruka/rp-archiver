package archives

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
