package dml

import (
	"fmt"

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

func (m *MergeArgument) BuildStatements() ([]string, error) {
	if err := m.Valid(); err != nil {
		return nil, err
	}

	return m.Dialect.BuildMergeQueries(
		m.TableID,
		m.SubQuery,
		m.IdempotentKey,
		m.PrimaryKeys,
		m.AdditionalEqualityStrings,
		m.Columns,
		m.SoftDelete,
		m.ContainsHardDeletes,
	)
}
