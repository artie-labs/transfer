package dml

import (
	"errors"
	"fmt"
	"strings"

	bigQueryDialect "github.com/artie-labs/transfer/clients/bigquery/dialect"
	mssqlDialect "github.com/artie-labs/transfer/clients/mssql/dialect"
	redshiftDialect "github.com/artie-labs/transfer/clients/redshift/dialect"
	snowflakeDialect "github.com/artie-labs/transfer/clients/snowflake/dialect"
	"github.com/artie-labs/transfer/lib/array"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing"
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

func (m *MergeArgument) buildRedshiftUpdateQuery(cols []columns.Column) string {
	clauses := redshiftDialect.RedshiftDialect{}.EqualitySQLParts(m.PrimaryKeys)

	if m.IdempotentKey != "" {
		clauses = append(clauses, fmt.Sprintf("cc.%s >= c.%s", m.IdempotentKey, m.IdempotentKey))
	}

	if !m.SoftDelete {
		clauses = append(clauses, fmt.Sprintf("COALESCE(cc.%s, false) = false", m.Dialect.QuoteIdentifier(constants.DeleteColumnMarker)))
	}

	return fmt.Sprintf(`UPDATE %s AS c SET %s FROM %s AS cc WHERE %s;`,
		// UPDATE table set col1 = cc. col1
		m.TableID.FullyQualifiedName(), columns.BuildColumnsUpdateFragment(cols, m.Dialect),
		// FROM staging WHERE join on PK(s)
		m.SubQuery, strings.Join(clauses, " AND "),
	)
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
			m.buildRedshiftUpdateQuery(m.Columns),
		}, nil
	}

	// We also need to remove __artie flags since it does not exist in the destination table
	columns, removed := columns.RemoveDeleteColumnMarker(m.Columns)
	if !removed {
		return nil, errors.New("artie delete flag doesn't exist")
	}

	parts := []string{
		redshiftDialect.RedshiftDialect{}.BuildMergeInsertQuery(m.TableID, m.SubQuery, m.PrimaryKeys, columns),
		m.buildRedshiftUpdateQuery(columns),
	}

	if *m.ContainsHardDeletes {
		parts = append(parts, redshiftDialect.RedshiftDialect{}.BuildMergeDeleteQuery(m.TableID, m.SubQuery, m.PrimaryKeys))
	}

	return parts, nil
}

func (m *MergeArgument) buildDefaultStatements() ([]string, error) {
	// We should not need idempotency key for DELETE
	// This is based on the assumption that the primary key would be atomically increasing or UUID based
	// With AI, the sequence will increment (never decrement). And UUID is there to prevent universal hash collision
	// However, there may be edge cases where folks end up restoring deleted rows (which will contain the same PK).

	// We also need to do staged table's idempotency key is GTE target table's idempotency key
	// This is because Snowflake does not respect NS granularity.
	var idempotentClause string
	if m.IdempotentKey != "" {
		idempotentClause = fmt.Sprintf("AND cc.%s >= c.%s ", m.IdempotentKey, m.IdempotentKey)
	}

	_, isBigQuery := m.Dialect.(bigQueryDialect.BigQueryDialect)

	var equalitySQLParts []string
	for _, primaryKey := range m.PrimaryKeys {
		// We'll need to escape the primary key as well.
		quotedPrimaryKey := m.Dialect.QuoteIdentifier(primaryKey.Name())

		equalitySQL := fmt.Sprintf("c.%s = cc.%s", quotedPrimaryKey, quotedPrimaryKey)

		if isBigQuery && primaryKey.KindDetails.Kind == typing.Struct.Kind {
			// BigQuery requires special casting to compare two JSON objects.
			equalitySQL = fmt.Sprintf("TO_JSON_STRING(c.%s) = TO_JSON_STRING(cc.%s)", quotedPrimaryKey, quotedPrimaryKey)
		}

		equalitySQLParts = append(equalitySQLParts, equalitySQL)
	}

	subQuery := fmt.Sprintf("( %s )", m.SubQuery)
	if isBigQuery {
		subQuery = m.SubQuery
	}

	if len(m.AdditionalEqualityStrings) > 0 {
		equalitySQLParts = append(equalitySQLParts, m.AdditionalEqualityStrings...)
	}

	if m.SoftDelete {
		return []string{fmt.Sprintf(`
MERGE INTO %s c USING %s AS cc ON %s
WHEN MATCHED %sTHEN UPDATE SET %s
WHEN NOT MATCHED AND IFNULL(cc.%s, false) = false THEN INSERT (%s) VALUES (%s);`,
			m.TableID.FullyQualifiedName(), subQuery, strings.Join(equalitySQLParts, " and "),
			// Update + Soft Deletion
			idempotentClause, columns.BuildColumnsUpdateFragment(m.Columns, m.Dialect),
			// Insert
			m.Dialect.QuoteIdentifier(constants.DeleteColumnMarker), strings.Join(columns.QuoteColumns(m.Columns, m.Dialect), ","),
			array.StringsJoinAddPrefix(array.StringsJoinAddPrefixArgs{
				Vals:      columns.QuoteColumns(m.Columns, m.Dialect),
				Separator: ",",
				Prefix:    "cc.",
			}))}, nil
	}

	// We also need to remove __artie flags since it does not exist in the destination table
	cols, removed := columns.RemoveDeleteColumnMarker(m.Columns)
	if !removed {
		return []string{}, errors.New("artie delete flag doesn't exist")
	}

	return []string{fmt.Sprintf(`
MERGE INTO %s c USING %s AS cc ON %s
WHEN MATCHED AND cc.%s THEN DELETE
WHEN MATCHED AND IFNULL(cc.%s, false) = false %sTHEN UPDATE SET %s
WHEN NOT MATCHED AND IFNULL(cc.%s, false) = false THEN INSERT (%s) VALUES (%s);`,
		m.TableID.FullyQualifiedName(), subQuery, strings.Join(equalitySQLParts, " and "),
		// Delete
		m.Dialect.QuoteIdentifier(constants.DeleteColumnMarker),
		// Update
		m.Dialect.QuoteIdentifier(constants.DeleteColumnMarker), idempotentClause, columns.BuildColumnsUpdateFragment(cols, m.Dialect),
		// Insert
		m.Dialect.QuoteIdentifier(constants.DeleteColumnMarker), strings.Join(columns.QuoteColumns(cols, m.Dialect), ","),
		array.StringsJoinAddPrefix(array.StringsJoinAddPrefixArgs{
			Vals:      columns.QuoteColumns(cols, m.Dialect),
			Separator: ",",
			Prefix:    "cc.",
		}))}, nil
}

func (m *MergeArgument) BuildStatements() ([]string, error) {
	if err := m.Valid(); err != nil {
		return nil, err
	}

	switch specificDialect := m.Dialect.(type) {
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
		return m.buildDefaultStatements()
	}
}
