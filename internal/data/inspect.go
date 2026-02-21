package data

import (
	"database/sql"
	"fmt"
	"os"
	"strings"

	_ "github.com/marcboeker/go-duckdb"
)

func InspectParquet(filepath string) (*InspectResult, error) {
	return inspectFile(filepath, fmt.Sprintf("read_parquet('%s')", filepath))
}

func InspectCSV(filepath string) (*InspectResult, error) {
	return inspectFile(filepath, fmt.Sprintf("read_csv_auto('%s')", filepath))
}

func inspectFile(filepath, readerExpr string) (*InspectResult, error) {
	var fileSize int64
	isS3 := strings.HasPrefix(filepath, "s3://")

	if !isS3 {
		info, err := os.Stat(filepath)
		if err != nil {
			return nil, err
		}
		fileSize = info.Size()
	}

	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, err
	}
	defer db.Close()

	if isS3 {
		_, err = db.Exec("INSTALL httpfs;")
		if err != nil {
			return nil, err
		}
		_, err = db.Exec("LOAD httpfs;")
		if err != nil {
			return nil, err
		}
	}

	// Get row count
	var rowCount int64
	err = db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", readerExpr)).Scan(&rowCount)
	if err != nil {
		return nil, err
	}

	// Get columns
	rows, err := db.Query(fmt.Sprintf("DESCRIBE SELECT * FROM %s", readerExpr))
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
	rows, err = db.Query(fmt.Sprintf("SELECT * FROM %s LIMIT 5", readerExpr))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columnNames, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	preview := []map[string]any{}
	for rows.Next() {
		values := make([]any, len(columnNames))
		valuePtrs := make([]any, len(columnNames))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		err := rows.Scan(valuePtrs...)
		if err != nil {
			return nil, err
		}

		row := make(map[string]any)
		for i, colName := range columnNames {
			row[colName] = values[i]
		}
		preview = append(preview, row)
	}

	return &InspectResult{
		RowCount:      rowCount,
		FileSizeBytes: fileSize,
		Columns:       columns,
		Preview:       preview,
	}, nil
}
