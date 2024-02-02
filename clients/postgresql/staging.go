package postgresql

import (
	"fmt"
	"strings"
	"time"

	"github.com/lib/pq"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/destination/ddl"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/typing"
)

func (s *Store) prepareTempTable(tableData *optimization.TableData, tableConfig *types.DwhTableConfig, tempTableName string) error {
	tempAlterTableArgs := ddl.AlterTableArgs{
		Dwh:               s,
		Tc:                tableConfig,
		FqTableName:       tempTableName,
		CreateTable:       true,
		TemporaryTable:    true,
		ColumnOp:          constants.Add,
		UppercaseEscNames: &s.config.SharedDestinationConfig.UppercaseEscapedNames,
	}

	if err := ddl.AlterTable(tempAlterTableArgs, tableData.ReadOnlyInMemoryCols().GetColumns()...); err != nil {
		return fmt.Errorf("failed to create temp table, err: %v", err)
	}

	expiryString := typing.ExpiresDate(time.Now().UTC().Add(ddl.TempTableTTL))
	// Now add a comment to the temporary table.
	if _, err := s.Exec(fmt.Sprintf(`COMMENT ON TABLE %s IS '%s';`, tempTableName, ddl.ExpiryComment(expiryString))); err != nil {
		return fmt.Errorf("failed to add comment to table, tableName: %v, err: %v", tempTableName, err)
	}

	return s.loadTemporaryTable(tableData, tempTableName)
}

func (s *Store) loadTemporaryTable(tableData *optimization.TableData, newTableName string) error {
	tx, err := s.Begin()
	if err != nil {
		return fmt.Errorf("failed to start tx, err: %w", err)
	}

	newTableNameParts := strings.Split(newTableName, ".")
	if len(newTableNameParts) != 2 {
		return fmt.Errorf("invalid table name, tableName: %v", newTableName)
	}

	columns := tableData.ReadOnlyInMemoryCols().GetColumnsToUpdate(s.config.SharedDestinationConfig.UppercaseEscapedNames, nil)
	stmt, err := tx.Prepare(pq.CopyInSchema(newTableNameParts[0], newTableNameParts[1], columns...))
	if err != nil {
		return fmt.Errorf("failed to prepare table, err: %w", err)
	}

	additionalDateFmts := s.config.SharedTransferConfig.TypingSettings.AdditionalDateFormats
	for _, value := range tableData.RowsData() {
		var row []any
		for _, col := range columns {
			colKind, _ := tableData.ReadOnlyInMemoryCols().GetColumn(col)
			castedValue, castErr := s.CastColValStaging(value[col], colKind, additionalDateFmts)
			if castErr != nil {
				return castErr
			}

			row = append(row, castedValue)
		}

		if _, err = stmt.Exec(row...); err != nil {
			return fmt.Errorf("failed to copy row, err: %w", err)
		}
	}

	// Close the statement to finish the COPY operation
	if _, err = stmt.Exec(); err != nil {
		return fmt.Errorf("failed to finalize COPY, err: %w", err)
	}

	// Commit the transaction
	return tx.Commit()
}
