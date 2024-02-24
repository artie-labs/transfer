package mssql

import (
	"fmt"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/destination/ddl"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/optimization"
	mssql "github.com/microsoft/go-mssqldb"
)

func (s *Store) PrepareTemporaryTable(tableData *optimization.TableData, tableConfig *types.DwhTableConfig, tempTableName string, additionalSettings types.AdditionalSettings) error {
	// Create the temporary table
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
		return fmt.Errorf("failed to create temp table: %w", err)
	}

	tx, err := s.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	columns := tableData.ReadOnlyInMemoryCols().GetColumnsToUpdate(s.config.SharedDestinationConfig.UppercaseEscapedNames, nil)
	fmt.Println("columns", columns)
	stmt, err := tx.Prepare(mssql.CopyIn(tempTableName, mssql.BulkOptions{}, columns...))
	if err != nil {
		return fmt.Errorf("failed to prepare bulk insert: %w", err)
	}

	defer stmt.Close()

	additionalDateFmts := s.config.SharedTransferConfig.TypingSettings.AdditionalDateFormats
	for _, value := range tableData.RowsData() {
		var row []any
		for _, col := range columns {
			colKind, _ := tableData.ReadOnlyInMemoryCols().GetColumn(col)
			castedValue, castErr := parseValue(value[col], colKind, additionalDateFmts)
			if castErr != nil {
				return castErr
			}

			row = append(row, castedValue)
		}

		if _, err = stmt.Exec(row...); err != nil {
			return fmt.Errorf("failed to copy row, err: %w", err)
		}
	}

	if _, err = stmt.Exec(); err != nil {
		return fmt.Errorf("failed to finalize bulk insert: %w", err)
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}
