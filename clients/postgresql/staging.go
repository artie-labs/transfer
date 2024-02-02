package redshift

import (
	"fmt"
	"strings"
	"time"

	"github.com/lib/pq"

	"github.com/artie-labs/transfer/lib/typing"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/destination/ddl"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/optimization"
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

	columns := tableData.ReadOnlyInMemoryCols().GetColumnsToUpdate(s.config.SharedDestinationConfig.UppercaseEscapedNames, nil)

	after, _ := strings.CutPrefix(newTableName, "public.")
	fmt.Println("after", after, "newTableName", newTableName)

	stmt, err := tx.Prepare(pq.CopyIn(strings.ToLower(after), columns...))
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
	_, err = stmt.Exec()
	if err != nil {
		return fmt.Errorf("failed to finalize COPY, err: %w", err)
	}

	// Commit the transaction
	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("failed to commit transaction, err: %w", err)
	}

	return nil
}
