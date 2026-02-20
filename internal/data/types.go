package data

type InspectResult struct {
	RowCount      int64
	FileSizeBytes int64
	Columns       []ColumnInfo
	Preview       []map[string]any
}

type ColumnInfo struct {
	Name    string
	Type    string
	NullPct float64
}
