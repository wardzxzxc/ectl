package data

import (
	"database/sql"
	"fmt"
	"os"

	_ "github.com/marcboeker/go-duckdb"
)

func InspectParquet(filepath string) (*InspectResult, error) {
	f, err := os.Open(filepath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return nil, err
	}

	// Open in-memory DuckDB
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, err
	}
	defer db.Close()

	// Get row count
	var rowCount int64
	query := fmt.Sprintf("SELECT COUNT(*) FROM read_parquet('%s')", filepath)
	err = db.QueryRow(query).Scan(&rowCount)
	if err != nil {
		return nil, err
	}

	// Get columns
	query = fmt.Sprintf("DESCRIBE SELECT * FROM read_parquet('%s')", filepath)
	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []ColumnInfo
	for rows.Next() {
		var colName, colType string
		var nullStr, key, defaultVal, extra sql.NullString
		err := rows.Scan(&colName, &colType, &nullStr, &key, &defaultVal, &extra)
		if err != nil {
			return nil, err
		}
		columns = append(columns, ColumnInfo{
			Name: colName,
			Type: colType,
		})
	}

	// Get first 5 rows
	query = fmt.Sprintf("SELECT * FROM read_parquet('%s') LIMIT 5", filepath)
	rows, err = db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Get column names for mapping
	columnNames, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	preview := []map[string]any{}
	for rows.Next() {
		// Create slice to scan into
		values := make([]any, len(columnNames))
		valuePtrs := make([]any, len(columnNames))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		err := rows.Scan(valuePtrs...)
		if err != nil {
			return nil, err
		}

		// Convert to map
		row := make(map[string]any)
		for i, colName := range columnNames {
			val := values[i]
			row[colName] = val
		}
		preview = append(preview, row)
	}

	return &InspectResult{
		RowCount:      rowCount,
		FileSizeBytes: info.Size(),
		Columns:       columns,
		Preview:       preview,
	}, nil
}
