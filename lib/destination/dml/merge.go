package dml

import (
	"errors"
	"fmt"
	"slices"
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
	Columns *columns.Columns

	DestKind   constants.DestinationKind
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

	if len(m.Columns.GetColumns()) == 0 {
		return fmt.Errorf("columns cannot be empty")
	}

	if m.TableID == nil {
		return fmt.Errorf("tableID cannot be nil")
	}

	if m.SubQuery == "" {
		return fmt.Errorf("subQuery cannot be empty")
	}

	if !constants.IsValidDestination(m.DestKind) {
		return fmt.Errorf("invalid destination: %s", m.DestKind)
	}

	if m.Dialect == nil {
		return fmt.Errorf("dialect cannot be nil")
	}

	return nil
}

func removeDeleteColumnMarker(columns []string) ([]string, bool) {
	origLength := len(columns)
	columns = slices.DeleteFunc(columns, func(col string) bool { return col == constants.DeleteColumnMarker })
	return columns, len(columns) != origLength
}

func (m *MergeArgument) GetParts() ([]string, error) {
	if err := m.Valid(); err != nil {
		return nil, err
	}

	if m.DestKind != constants.Redshift {
		return nil, fmt.Errorf("err - this is meant for redshift only")
	}

	// ContainsHardDeletes is only used for Redshift, so we'll validate it now
	if m.ContainsHardDeletes == nil {
		return nil, fmt.Errorf("containsHardDeletes cannot be nil")
	}

	// We should not need idempotency key for DELETE
	// This is based on the assumption that the primary key would be atomically increasing or UUID based
	// With AI, the sequence will increment (never decrement). And UUID is there to prevent universal hash collision
	// However, there may be edge cases where folks end up restoring deleted rows (which will contain the same PK).

	// We also need to do staged table's idempotency key is GTE target table's idempotency key
	// This is because Snowflake does not respect NS granularity.
	var idempotentClause string
	if m.IdempotentKey != "" {
		idempotentClause = fmt.Sprintf(" AND cc.%s >= c.%s", m.IdempotentKey, m.IdempotentKey)
	}

	var equalitySQLParts []string
	for _, primaryKey := range m.PrimaryKeys {
		// We'll need to escape the primary key as well.
		quotedPrimaryKey := m.Dialect.QuoteIdentifier(primaryKey.Name())
		equalitySQL := fmt.Sprintf("c.%s = cc.%s", quotedPrimaryKey, quotedPrimaryKey)
		equalitySQLParts = append(equalitySQLParts, equalitySQL)
	}

	columns := m.Columns.GetColumnsToUpdate()

	if m.SoftDelete {
		return []string{
			// INSERT
			fmt.Sprintf(`INSERT INTO %s (%s) SELECT %s FROM %s as cc LEFT JOIN %s as c on %s WHERE c.%s IS NULL;`,
				// insert into target (col1, col2, col3)
				m.TableID.FullyQualifiedName(), strings.Join(sql.QuoteIdentifiers(columns, m.Dialect), ","),
				// SELECT cc.col1, cc.col2, ... FROM staging as CC
				array.StringsJoinAddPrefix(array.StringsJoinAddPrefixArgs{
					Vals:      sql.QuoteIdentifiers(columns, m.Dialect),
					Separator: ",",
					Prefix:    "cc.",
				}), m.SubQuery,
				// LEFT JOIN table on pk(s)
				m.TableID.FullyQualifiedName(), strings.Join(equalitySQLParts, " and "),
				// Where PK is NULL (we only need to specify one primary key since it's covered with equalitySQL parts)
				m.Dialect.QuoteIdentifier(m.PrimaryKeys[0].Name()),
			),
			// UPDATE
			fmt.Sprintf(`UPDATE %s as c SET %s FROM %s as cc WHERE %s%s;`,
				// UPDATE table set col1 = cc. col1
				m.TableID.FullyQualifiedName(), buildColumnsUpdateFragment(m.Columns, m.Dialect, false),
				// FROM table (temp) WHERE join on PK(s)
				m.SubQuery, strings.Join(equalitySQLParts, " and "), idempotentClause,
			),
		}, nil
	}

	// We also need to remove __artie flags since it does not exist in the destination table
	var removed bool
	columns, removed = removeDeleteColumnMarker(columns)
	if !removed {
		return nil, errors.New("artie delete flag doesn't exist")
	}

	var pks []string
	for _, pk := range m.PrimaryKeys {
		pks = append(pks, m.Dialect.QuoteIdentifier(pk.Name()))
	}

	parts := []string{
		// INSERT
		fmt.Sprintf(`INSERT INTO %s (%s) SELECT %s FROM %s as cc LEFT JOIN %s as c on %s WHERE c.%s IS NULL;`,
			// insert into target (col1, col2, col3)
			m.TableID.FullyQualifiedName(), strings.Join(sql.QuoteIdentifiers(columns, m.Dialect), ","),
			// SELECT cc.col1, cc.col2, ... FROM staging as CC
			array.StringsJoinAddPrefix(array.StringsJoinAddPrefixArgs{
				Vals:      sql.QuoteIdentifiers(columns, m.Dialect),
				Separator: ",",
				Prefix:    "cc.",
			}), m.SubQuery,
			// LEFT JOIN table on pk(s)
			m.TableID.FullyQualifiedName(), strings.Join(equalitySQLParts, " and "),
			// Where PK is NULL (we only need to specify one primary key since it's covered with equalitySQL parts)
			m.Dialect.QuoteIdentifier(m.PrimaryKeys[0].Name()),
		),
		// UPDATE
		fmt.Sprintf(`UPDATE %s as c SET %s FROM %s as cc WHERE %s%s AND COALESCE(cc.%s, false) = false;`,
			// UPDATE table set col1 = cc. col1
			m.TableID.FullyQualifiedName(), buildColumnsUpdateFragment(m.Columns, m.Dialect, true),
			// FROM staging WHERE join on PK(s)
			m.SubQuery, strings.Join(equalitySQLParts, " and "), idempotentClause, m.Dialect.QuoteIdentifier(constants.DeleteColumnMarker),
		),
	}

	if *m.ContainsHardDeletes {
		parts = append(parts,
			// DELETE
			fmt.Sprintf(`DELETE FROM %s WHERE (%s) IN (SELECT %s FROM %s as cc WHERE cc.%s = true);`,
				// DELETE from table where (pk_1, pk_2)
				m.TableID.FullyQualifiedName(), strings.Join(pks, ","),
				// IN (cc.pk_1, cc.pk_2) FROM staging
				array.StringsJoinAddPrefix(array.StringsJoinAddPrefixArgs{
					Vals:      pks,
					Separator: ",",
					Prefix:    "cc.",
				}), m.SubQuery, m.Dialect.QuoteIdentifier(constants.DeleteColumnMarker),
			))
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

	var equalitySQLParts []string
	for _, primaryKey := range m.PrimaryKeys {
		// We'll need to escape the primary key as well.
		quotedPrimaryKey := m.Dialect.QuoteIdentifier(primaryKey.Name())

		equalitySQL := fmt.Sprintf("c.%s = cc.%s", quotedPrimaryKey, quotedPrimaryKey)
		pkCol, isOk := m.Columns.GetColumn(primaryKey.Name())
		if !isOk {
			return "", fmt.Errorf("column: %s does not exist in columnToType: %v", primaryKey.Name(), m.Columns)
		}

		if m.DestKind == constants.BigQuery && pkCol.KindDetails.Kind == typing.Struct.Kind {
			// BigQuery requires special casting to compare two JSON objects.
			equalitySQL = fmt.Sprintf("TO_JSON_STRING(c.%s) = TO_JSON_STRING(cc.%s)", quotedPrimaryKey, quotedPrimaryKey)
		}

		equalitySQLParts = append(equalitySQLParts, equalitySQL)
	}

	subQuery := fmt.Sprintf("( %s )", m.SubQuery)
	if m.DestKind == constants.BigQuery {
		subQuery = m.SubQuery
	}

	if len(m.AdditionalEqualityStrings) > 0 {
		equalitySQLParts = append(equalitySQLParts, m.AdditionalEqualityStrings...)
	}

	columns := m.Columns.GetColumnsToUpdate()

	if m.SoftDelete {
		return fmt.Sprintf(`
MERGE INTO %s c USING %s AS cc ON %s
WHEN MATCHED %sTHEN UPDATE SET %s
WHEN NOT MATCHED AND IFNULL(cc.%s, false) = false THEN INSERT (%s) VALUES (%s);`,
			m.TableID.FullyQualifiedName(), subQuery, strings.Join(equalitySQLParts, " and "),
			// Update + Soft Deletion
			idempotentClause, buildColumnsUpdateFragment(m.Columns, m.Dialect, false),
			// Insert
			m.Dialect.QuoteIdentifier(constants.DeleteColumnMarker), strings.Join(sql.QuoteIdentifiers(columns, m.Dialect), ","),
			array.StringsJoinAddPrefix(array.StringsJoinAddPrefixArgs{
				Vals:      sql.QuoteIdentifiers(columns, m.Dialect),
				Separator: ",",
				Prefix:    "cc.",
			})), nil
	}

	// We also need to remove __artie flags since it does not exist in the destination table
	var removed bool
	columns, removed = removeDeleteColumnMarker(columns)
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
		m.Dialect.QuoteIdentifier(constants.DeleteColumnMarker), idempotentClause, buildColumnsUpdateFragment(m.Columns, m.Dialect, true),
		// Insert
		m.Dialect.QuoteIdentifier(constants.DeleteColumnMarker), strings.Join(sql.QuoteIdentifiers(columns, m.Dialect), ","),
		array.StringsJoinAddPrefix(array.StringsJoinAddPrefixArgs{
			Vals:      sql.QuoteIdentifiers(columns, m.Dialect),
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

	columns := m.Columns.GetColumnsToUpdate()

	if m.SoftDelete {
		return fmt.Sprintf(`
MERGE INTO %s c
USING %s AS cc ON %s
WHEN MATCHED %sTHEN UPDATE SET %s
WHEN NOT MATCHED AND COALESCE(cc.%s, 0) = 0 THEN INSERT (%s) VALUES (%s);`,
			m.TableID.FullyQualifiedName(), m.SubQuery, strings.Join(equalitySQLParts, " and "),
			// Update + Soft Deletion
			idempotentClause, buildColumnsUpdateFragment(m.Columns, m.Dialect, false),
			// Insert
			m.Dialect.QuoteIdentifier(constants.DeleteColumnMarker), strings.Join(sql.QuoteIdentifiers(columns, m.Dialect), ","),
			array.StringsJoinAddPrefix(array.StringsJoinAddPrefixArgs{
				Vals:      sql.QuoteIdentifiers(columns, m.Dialect),
				Separator: ",",
				Prefix:    "cc.",
			})), nil
	}

	// We also need to remove __artie flags since it does not exist in the destination table
	var removed bool
	columns, removed = removeDeleteColumnMarker(columns)
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
		m.Dialect.QuoteIdentifier(constants.DeleteColumnMarker), idempotentClause, buildColumnsUpdateFragment(m.Columns, m.Dialect, true),
		// Insert
		m.Dialect.QuoteIdentifier(constants.DeleteColumnMarker), strings.Join(sql.QuoteIdentifiers(columns, m.Dialect), ","),
		array.StringsJoinAddPrefix(array.StringsJoinAddPrefixArgs{
			Vals:      sql.QuoteIdentifiers(columns, m.Dialect),
			Separator: ",",
			Prefix:    "cc.",
		})), nil
}
