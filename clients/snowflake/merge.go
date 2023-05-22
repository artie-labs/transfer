package snowflake

import (
	"encoding/csv"
	"fmt"
	"os"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/typing"
)

// escapeCols will return the following arguments:
// 1) colsToUpdate - list of columns to update
// 2) list of columns to update (escaped).
func escapeCols(cols []typing.Column) (colsToUpdate []string, colsToUpdateEscaped []string) {
	for _, column := range cols {
		if column.KindDetails.Kind == typing.Invalid.Kind {
			// Don't update Snowflake
			continue
		}

		escapedCol := column.Name
		switch column.KindDetails.Kind {
		case typing.Struct.Kind, typing.Array.Kind:
			if column.ToastColumn {
				escapedCol = fmt.Sprintf("CASE WHEN %s = '%s' THEN {'key': '%s'} ELSE PARSE_JSON(%s) END %s",
					// Comparing the column against placeholder
					column.Name, constants.ToastUnavailableValuePlaceholder,
					// Casting placeholder as a JSON object
					constants.ToastUnavailableValuePlaceholder,
					// Regular parsing.
					column.Name, column.Name)
			} else {
				escapedCol = fmt.Sprintf("PARSE_JSON(%s) %s", column.Name, column.Name)
			}
		}

		colsToUpdate = append(colsToUpdate, column.Name)
		colsToUpdateEscaped = append(colsToUpdateEscaped, escapedCol)
	}

	return
}

// loadTemporaryTable will write the data into /tmp/newTableName.csv
// This way, another function can call this and then invoke a Snowflake PUT.
func (s *Store) loadTemporaryTable(tableData *optimization.TableData, newTableName string) error {
	// TODO - test
	file, err := os.Create(fmt.Sprintf("/tmp/%s.csv", newTableName))
	if err != nil {
		return err
	}

	defer file.Close()
	writer := csv.NewWriter(file)
	writer.Comma = '\t'
	for _, value := range tableData.RowsData() {
		var row []string
		for _, col := range tableData.ReadOnlyInMemoryCols().GetColumnsToUpdate() {
			row = append(row, fmt.Sprint(value[col]))
		}

		if err = writer.Write(row); err != nil {
			return fmt.Errorf("failed to write to csv, err: %v", err)
		}
	}

	writer.Flush()
	return writer.Error()
}

func (s *Store) deleteTemporaryFile(tableName string) error {
	// TODO - test
	return os.RemoveAll(fmt.Sprintf("/tmp/%s.csv", tableName))
}
