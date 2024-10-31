package sql

import (
	"database/sql"
	"fmt"
)

func RowsToObjects(rows *sql.Rows) ([]map[string]any, error) {
	if rows == nil {
		return []map[string]any{}, nil
	}

	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var objects []map[string]any
	for rows.Next() {
		row := make([]any, len(columns))
		rowPointers := make([]any, len(columns))
		for i := range row {
			rowPointers[i] = &row[i]
		}

		if err = rows.Scan(rowPointers...); err != nil {
			return nil, err
		}

		object := make(map[string]any)
		for i, column := range columns {
			object[column] = row[i]
		}

		objects = append(objects, object)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate over rows: %w", err)
	}

	return objects, nil
}
