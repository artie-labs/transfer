package dml

import (
	"errors"
	"fmt"
	"strings"

	"github.com/artie-labs/transfer/lib/array"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

type MergeArgument struct {
	TableID       types.TableIdentifier
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

func (m *MergeArgument) redshiftEqualitySQLParts() []string {
	var equalitySQLParts []string
	for _, primaryKey := range m.PrimaryKeys {
		// We'll need to escape the primary key as well.
		quotedPrimaryKey := m.Dialect.QuoteIdentifier(primaryKey.Name())
		equalitySQL := fmt.Sprintf("c.%s = cc.%s", quotedPrimaryKey, quotedPrimaryKey)
		equalitySQLParts = append(equalitySQLParts, equalitySQL)
	}
	return equalitySQLParts
}

func (m *MergeArgument) buildRedshiftInsertQuery(columns []columns.Column) string {
	return fmt.Sprintf(`INSERT INTO %s (%s) SELECT %s FROM %s AS cc LEFT JOIN %s AS c ON %s WHERE c.%s IS NULL;`,
		// insert into target (col1, col2, col3)
		m.TableID.FullyQualifiedName(), strings.Join(quoteColumns(columns, m.Dialect), ","),
		// SELECT cc.col1, cc.col2, ... FROM staging as CC
		array.StringsJoinAddPrefix(array.StringsJoinAddPrefixArgs{
			Vals:      quoteColumns(columns, m.Dialect),
			Separator: ",",
			Prefix:    "cc.",
		}), m.SubQuery,
		// LEFT JOIN table on pk(s)
		m.TableID.FullyQualifiedName(), strings.Join(m.redshiftEqualitySQLParts(), " AND "),
		// Where PK is NULL (we only need to specify one primary key since it's covered with equalitySQL parts)
		m.Dialect.QuoteIdentifier(m.PrimaryKeys[0].Name()),
	)
}

func (m *MergeArgument) buildRedshiftUpdateQuery(columns []columns.Column) string {
	clauses := m.redshiftEqualitySQLParts()

	if m.IdempotentKey != "" {
		clauses = append(clauses, fmt.Sprintf("cc.%s >= c.%s", m.IdempotentKey, m.IdempotentKey))
	}

	if !m.SoftDelete {
		clauses = append(clauses, fmt.Sprintf("COALESCE(cc.%s, false) = false", m.Dialect.QuoteIdentifier(constants.DeleteColumnMarker)))
	}

	return fmt.Sprintf(`UPDATE %s AS c SET %s FROM %s AS cc WHERE %s;`,
		// UPDATE table set col1 = cc. col1
		m.TableID.FullyQualifiedName(), buildColumnsUpdateFragment(columns, m.Dialect),
		// FROM staging WHERE join on PK(s)
		m.SubQuery, strings.Join(clauses, " AND "),
	)
}

func (m *MergeArgument) buildRedshiftDeleteQuery() string {
	return fmt.Sprintf(`DELETE FROM %s WHERE (%s) IN (SELECT %s FROM %s AS cc WHERE cc.%s = true);`,
		// DELETE from table where (pk_1, pk_2)
		m.TableID.FullyQualifiedName(), strings.Join(quoteColumns(m.PrimaryKeys, m.Dialect), ","),
		// IN (cc.pk_1, cc.pk_2) FROM staging
		array.StringsJoinAddPrefix(array.StringsJoinAddPrefixArgs{
			Vals:      quoteColumns(m.PrimaryKeys, m.Dialect),
			Separator: ",",
			Prefix:    "cc.",
		}), m.SubQuery, m.Dialect.QuoteIdentifier(constants.DeleteColumnMarker),
	)
}

func (m *MergeArgument) GetRedshiftStatements() ([]string, error) {
	if err := m.Valid(); err != nil {
		return nil, err
	}

	if _, ok := m.Dialect.(sql.RedshiftDialect); !ok {
		return nil, fmt.Errorf("this is meant for Redshift only")
	}

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
			m.buildRedshiftInsertQuery(m.Columns),
			m.buildRedshiftUpdateQuery(m.Columns),
		}, nil
	}

	// We also need to remove __artie flags since it does not exist in the destination table
	columns, removed := removeDeleteColumnMarker(m.Columns)
	if !removed {
		return nil, errors.New("artie delete flag doesn't exist")
	}

	parts := []string{
		m.buildRedshiftInsertQuery(columns),
		m.buildRedshiftUpdateQuery(columns),
	}

	if *m.ContainsHardDeletes {
		parts = append(parts, m.buildRedshiftDeleteQuery())
	}

	return parts, nil
}

func (m *MergeArgument) GetStatement() (string, error) {
	if err := m.Valid(); err != nil {
		return "", err
	}

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

	_, isBigQuery := m.Dialect.(sql.BigQueryDialect)

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
		return fmt.Sprintf(`
MERGE INTO %s c USING %s AS cc ON %s
WHEN MATCHED %sTHEN UPDATE SET %s
WHEN NOT MATCHED AND IFNULL(cc.%s, false) = false THEN INSERT (%s) VALUES (%s);`,
			m.TableID.FullyQualifiedName(), subQuery, strings.Join(equalitySQLParts, " and "),
			// Update + Soft Deletion
			idempotentClause, buildColumnsUpdateFragment(m.Columns, m.Dialect),
			// Insert
			m.Dialect.QuoteIdentifier(constants.DeleteColumnMarker), strings.Join(quoteColumns(m.Columns, m.Dialect), ","),
			array.StringsJoinAddPrefix(array.StringsJoinAddPrefixArgs{
				Vals:      quoteColumns(m.Columns, m.Dialect),
				Separator: ",",
				Prefix:    "cc.",
			})), nil
	}

	// We also need to remove __artie flags since it does not exist in the destination table
	columns, removed := removeDeleteColumnMarker(m.Columns)
	if !removed {
		return "", errors.New("artie delete flag doesn't exist")
	}

	return fmt.Sprintf(`
MERGE INTO %s c USING %s AS cc ON %s
WHEN MATCHED AND cc.%s THEN DELETE
WHEN MATCHED AND IFNULL(cc.%s, false) = false %sTHEN UPDATE SET %s
WHEN NOT MATCHED AND IFNULL(cc.%s, false) = false THEN INSERT (%s) VALUES (%s);`,
		m.TableID.FullyQualifiedName(), subQuery, strings.Join(equalitySQLParts, " and "),
		// Delete
		m.Dialect.QuoteIdentifier(constants.DeleteColumnMarker),
		// Update
		m.Dialect.QuoteIdentifier(constants.DeleteColumnMarker), idempotentClause, buildColumnsUpdateFragment(columns, m.Dialect),
		// Insert
		m.Dialect.QuoteIdentifier(constants.DeleteColumnMarker), strings.Join(quoteColumns(columns, m.Dialect), ","),
		array.StringsJoinAddPrefix(array.StringsJoinAddPrefixArgs{
			Vals:      quoteColumns(columns, m.Dialect),
			Separator: ",",
			Prefix:    "cc.",
		})), nil
}

func (m *MergeArgument) GetMSSQLStatement() (string, error) {
	if err := m.Valid(); err != nil {
		return "", err
	}

	var idempotentClause string
	if m.IdempotentKey != "" {
		idempotentClause = fmt.Sprintf("AND cc.%s >= c.%s ", m.IdempotentKey, m.IdempotentKey)
	}

	var equalitySQLParts []string
	for _, primaryKey := range m.PrimaryKeys {
		// We'll need to escape the primary key as well.
		quotedPrimaryKey := m.Dialect.QuoteIdentifier(primaryKey.Name())
		equalitySQL := fmt.Sprintf("c.%s = cc.%s", quotedPrimaryKey, quotedPrimaryKey)
		equalitySQLParts = append(equalitySQLParts, equalitySQL)
	}

	if m.SoftDelete {
		return fmt.Sprintf(`
MERGE INTO %s c
USING %s AS cc ON %s
WHEN MATCHED %sTHEN UPDATE SET %s
WHEN NOT MATCHED AND COALESCE(cc.%s, 0) = 0 THEN INSERT (%s) VALUES (%s);`,
			m.TableID.FullyQualifiedName(), m.SubQuery, strings.Join(equalitySQLParts, " and "),
			// Update + Soft Deletion
			idempotentClause, buildColumnsUpdateFragment(m.Columns, m.Dialect),
			// Insert
			m.Dialect.QuoteIdentifier(constants.DeleteColumnMarker), strings.Join(quoteColumns(m.Columns, m.Dialect), ","),
			array.StringsJoinAddPrefix(array.StringsJoinAddPrefixArgs{
				Vals:      quoteColumns(m.Columns, m.Dialect),
				Separator: ",",
				Prefix:    "cc.",
			})), nil
	}

	// We also need to remove __artie flags since it does not exist in the destination table
	columns, removed := removeDeleteColumnMarker(m.Columns)
	if !removed {
		return "", errors.New("artie delete flag doesn't exist")
	}

	return fmt.Sprintf(`
MERGE INTO %s c
USING %s AS cc ON %s
WHEN MATCHED AND cc.%s = 1 THEN DELETE
WHEN MATCHED AND COALESCE(cc.%s, 0) = 0 %sTHEN UPDATE SET %s
WHEN NOT MATCHED AND COALESCE(cc.%s, 1) = 0 THEN INSERT (%s) VALUES (%s);`,
		m.TableID.FullyQualifiedName(), m.SubQuery, strings.Join(equalitySQLParts, " and "),
		// Delete
		m.Dialect.QuoteIdentifier(constants.DeleteColumnMarker),
		// Update
		m.Dialect.QuoteIdentifier(constants.DeleteColumnMarker), idempotentClause, buildColumnsUpdateFragment(columns, m.Dialect),
		// Insert
		m.Dialect.QuoteIdentifier(constants.DeleteColumnMarker), strings.Join(quoteColumns(columns, m.Dialect), ","),
		array.StringsJoinAddPrefix(array.StringsJoinAddPrefixArgs{
			Vals:      quoteColumns(columns, m.Dialect),
			Separator: ",",
			Prefix:    "cc.",
		})), nil
}
