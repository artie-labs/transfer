package dml

import (
	"errors"
	"fmt"

	bigQueryDialect "github.com/artie-labs/transfer/clients/bigquery/dialect"
	mssqlDialect "github.com/artie-labs/transfer/clients/mssql/dialect"
	redshiftDialect "github.com/artie-labs/transfer/clients/redshift/dialect"
	snowflakeDialect "github.com/artie-labs/transfer/clients/snowflake/dialect"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

type MergeArgument struct {
	TableID       sql.TableIdentifier
	SubQuery      string
	IdempotentKey string
	PrimaryKeys   []columns.Column

	// AdditionalEqualityStrings is used for handling BigQuery partitioned table merges
	AdditionalEqualityStrings []string

	// Columns will need to be escaped
	Columns []columns.Column

	SoftDelete bool
	// ContainsHardDeletes is only used for Redshift and MergeStatementParts,
	// where we do not issue a DELETE statement if there are no hard deletes in the batch
	ContainsHardDeletes *bool
	Dialect             sql.Dialect
}

func (m *MergeArgument) Valid() error {
	if m == nil {
		return fmt.Errorf("merge argument is nil")
	}

	if len(m.PrimaryKeys) == 0 {
		return fmt.Errorf("merge argument does not contain primary keys")
	}

	if len(m.Columns) == 0 {
		return fmt.Errorf("columns cannot be empty")
	}
	for _, column := range m.Columns {
		if column.ShouldSkip() {
			return fmt.Errorf("column %q is invalid and should be skipped", column.Name())
		}
	}

	if m.TableID == nil {
		return fmt.Errorf("tableID cannot be nil")
	}

	if m.SubQuery == "" {
		return fmt.Errorf("subQuery cannot be empty")
	}

	if m.Dialect == nil {
		return fmt.Errorf("dialect cannot be nil")
	}

	return nil
}

func (m *MergeArgument) buildRedshiftStatements() ([]string, error) {
	// ContainsHardDeletes is only used for Redshift, so we'll validate it now
	if m.ContainsHardDeletes == nil {
		return nil, fmt.Errorf("containsHardDeletes cannot be nil")
	}

	// We should not need idempotency key for DELETE
	// This is based on the assumption that the primary key would be atomically increasing or UUID based
	// With AI, the sequence will increment (never decrement). And UUID is there to prevent universal hash collision
	// However, there may be edge cases where folks end up restoring deleted rows (which will contain the same PK).

	if m.SoftDelete {
		return []string{
			redshiftDialect.RedshiftDialect{}.BuildMergeInsertQuery(m.TableID, m.SubQuery, m.PrimaryKeys, m.Columns),
			redshiftDialect.RedshiftDialect{}.BuildMergeUpdateQuery(m.TableID, m.SubQuery, m.PrimaryKeys, m.Columns, m.IdempotentKey, m.SoftDelete),
		}, nil
	}

	// We also need to remove __artie flags since it does not exist in the destination table
	columns, removed := columns.RemoveDeleteColumnMarker(m.Columns)
	if !removed {
		return nil, errors.New("artie delete flag doesn't exist")
	}

	parts := []string{
		redshiftDialect.RedshiftDialect{}.BuildMergeInsertQuery(m.TableID, m.SubQuery, m.PrimaryKeys, columns),
		redshiftDialect.RedshiftDialect{}.BuildMergeUpdateQuery(m.TableID, m.SubQuery, m.PrimaryKeys, columns, m.IdempotentKey, m.SoftDelete),
	}

	if *m.ContainsHardDeletes {
		parts = append(parts, redshiftDialect.RedshiftDialect{}.BuildMergeDeleteQuery(m.TableID, m.SubQuery, m.PrimaryKeys))
	}

	return parts, nil
}

func (m *MergeArgument) BuildStatements() ([]string, error) {
	if err := m.Valid(); err != nil {
		return nil, err
	}

	switch specificDialect := m.Dialect.(type) {
	case bigQueryDialect.BigQueryDialect:
		return specificDialect.BuildMergeQueries(
			m.TableID,
			m.SubQuery,
			m.IdempotentKey,
			m.PrimaryKeys,
			m.AdditionalEqualityStrings,
			m.Columns,
			m.SoftDelete,
			m.ContainsHardDeletes,
		)
	case redshiftDialect.RedshiftDialect:
		return m.buildRedshiftStatements()
	case mssqlDialect.MSSQLDialect:
		return specificDialect.BuildMergeQueries(
			m.TableID,
			m.SubQuery,
			m.IdempotentKey,
			m.PrimaryKeys,
			m.AdditionalEqualityStrings,
			m.Columns,
			m.SoftDelete,
			m.ContainsHardDeletes,
		)
	case snowflakeDialect.SnowflakeDialect:
		return specificDialect.BuildMergeQueries(
			m.TableID,
			m.SubQuery,
			m.IdempotentKey,
			m.PrimaryKeys,
			m.AdditionalEqualityStrings,
			m.Columns,
			m.SoftDelete,
			m.ContainsHardDeletes,
		)
	default:
		return nil, fmt.Errorf("not implemented for %T", m.Dialect)
	}
}
