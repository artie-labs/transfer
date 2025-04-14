package iceberg

import (
	"context"
	"fmt"
	"time"

	"github.com/artie-labs/transfer/clients/iceberg/dialect"
	"github.com/artie-labs/transfer/lib/awslib"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

func (s Store) describeTable(ctx context.Context, tableID dialect.TableIdentifier) ([]columns.Column, error) {
	out, err := s.s3TablesAPI.GetTable(ctx, tableID.Namespace(), tableID.Table())
	if err != nil {
		if awslib.IsNotFoundError(err) {
			return nil, nil
		}

		return nil, fmt.Errorf("failed to get table: %w", err)
	}

	metadata, err := s.s3TablesAPI.GetTableMetadata(ctx, *out.MetadataLocation)
	if err != nil {
		return nil, fmt.Errorf("failed to get table metadata: %w", err)
	}

	currentSchema, err := metadata.RetrieveCurrentSchema()
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve current schema: %w", err)
	}

	cols := make([]columns.Column, len(currentSchema.Fields))
	for i, field := range currentSchema.Fields {
		kind, err := s.Dialect().KindForDataType(field.Type, "notused")
		if err != nil {
			return nil, fmt.Errorf("failed to get kind for data type: %w", err)
		}

		cols[i] = columns.NewColumn(field.Name, kind)
	}

	return cols, nil
}

func (s Store) CreateTable(ctx context.Context, tableID sql.TableIdentifier, tableConfig *types.DestinationTableConfig, cols []columns.Column) error {
	var colParts []string
	for _, col := range cols {
		colPart := fmt.Sprintf("%s %s", col.Name(), s.Dialect().DataTypeForKind(col.KindDetails, col.PrimaryKey(), config.SharedDestinationColumnSettings{}))
		colParts = append(colParts, colPart)
	}

	query := s.Dialect().BuildCreateTableQuery(tableID, false, colParts)
	fmt.Println("query", query)
	if err := s.apacheLivyClient.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	// Now add this to our [tableConfig]
	tableConfig.MutateInMemoryColumns(constants.Add, cols...)
	return nil
}

func (s Store) AlterTableAddColumns(ctx context.Context, tableID sql.TableIdentifier, tableConfig *types.DestinationTableConfig, cols []columns.Column) error {
	colSQLParts := make([]string, len(cols))
	for i, col := range cols {
		colSQLParts[i] = fmt.Sprintf("%s %s", col.Name(), s.Dialect().DataTypeForKind(col.KindDetails, col.PrimaryKey(), config.SharedDestinationColumnSettings{}))
	}

	for _, part := range colSQLParts {
		if err := s.apacheLivyClient.ExecContext(ctx, s.Dialect().BuildAddColumnQuery(tableID, part)); err != nil {
			return fmt.Errorf("failed to alter table: %w", err)
		}
	}

	// Now add this to our [tableConfig]
	tableConfig.MutateInMemoryColumns(constants.Add, cols...)
	return nil
}

func (s Store) AlterTableDropColumns(ctx context.Context, tableID sql.TableIdentifier, tableConfig *types.DestinationTableConfig, cols []columns.Column, cdcTime time.Time, containOtherOperations bool) error {
	var colsToDrop []columns.Column
	for _, col := range cols {
		if tableConfig.ShouldDeleteColumn(col.Name(), cdcTime, containOtherOperations) {
			colsToDrop = append(colsToDrop, col)
		}
	}

	if len(colsToDrop) == 0 {
		return nil
	}

	for _, col := range colsToDrop {
		if err := s.apacheLivyClient.ExecContext(ctx, s.Dialect().BuildDropColumnQuery(tableID, col.Name())); err != nil {
			return fmt.Errorf("failed to drop column: %w", err)
		}
	}

	tableConfig.MutateInMemoryColumns(constants.Delete, colsToDrop...)
	return nil
}

func (s Store) DeleteTable(ctx context.Context, tableID sql.TableIdentifier) error {
	castedTableID, ok := tableID.(dialect.TableIdentifier)
	if !ok {
		return fmt.Errorf("failed to cast table ID to dialect.TableIdentifier")
	}

	if err := s.s3TablesAPI.DeleteTable(ctx, castedTableID.Namespace(), castedTableID.Table()); err != nil {
		return fmt.Errorf("failed to delete table: %w", err)
	}

	return nil
}
