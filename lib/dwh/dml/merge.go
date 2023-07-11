package dml

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/artie-labs/transfer/lib/sql"

	"github.com/artie-labs/transfer/lib/stringutil"

	"github.com/artie-labs/transfer/lib/typing/columns"

	"github.com/artie-labs/transfer/lib/typing"

	"github.com/artie-labs/transfer/lib/array"
	"github.com/artie-labs/transfer/lib/config/constants"
)

type MergeArgument struct {
	FqTableName   string
	SubQuery      string
	IdempotentKey string
	PrimaryKeys   []columns.Wrapper

	// ColumnsToTypes also needs to be escaped.
	ColumnsToTypes columns.Columns

	/*
		DestKind is used needed because:
		- BigQuery is used to:
			1) escape JSON columns
			2) merge temp table vs. subquery
		- Redshift is used to:
			1) Using as part of the MergeStatementIndividual
	*/
	DestKind   constants.DestinationKind
	SoftDelete bool
}

func (m *MergeArgument) Valid() error {
	if m == nil {
		return fmt.Errorf("merge argument is nil")
	}

	if len(m.PrimaryKeys) == 0 {
		return fmt.Errorf("merge argument does not contain primary keys")
	}

	if len(m.ColumnsToTypes.GetColumns()) == 0 {
		return fmt.Errorf("columnToTypes cannot be empty")
	}

	if stringutil.Empty(m.FqTableName, m.SubQuery) {
		return fmt.Errorf("one of these arguments is empty: fqTableName, subQuery")
	}

	return nil
}

func MergeStatementParts(ctx context.Context, m *MergeArgument) ([]string, error) {
	if err := m.Valid(); err != nil {
		return nil, err
	}

	if m.DestKind != constants.Redshift {
		return nil, fmt.Errorf("err - this is meant for redshift only")
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
		equalitySQL := fmt.Sprintf("c.%s = cc.%s", primaryKey.EscapedName(), primaryKey.EscapedName())
		equalitySQLParts = append(equalitySQLParts, equalitySQL)
	}

	cols := m.ColumnsToTypes.GetColumnsToUpdate(ctx, &sql.NameArgs{
		Escape:   true,
		DestKind: m.DestKind,
	})

	if m.SoftDelete {
		return []string{
			// INSERT
			fmt.Sprintf(`INSERT INTO %s (%s) SELECT %s FROM %s as cc LEFT JOIN %s as c on %s WHERE c.%s IS NULL;`,
				// insert into target (col1, col2, col3)
				m.FqTableName, strings.Join(cols, ","),
				// SELECT cc.col1, cc.col2, ... FROM staging as CC
				array.StringsJoinAddPrefix(array.StringsJoinAddPrefixArgs{
					Vals:      cols,
					Separator: ",",
					Prefix:    "cc.",
				}), m.SubQuery,
				// LEFT JOIN table on pk(s)
				m.FqTableName, strings.Join(equalitySQLParts, " and "),
				// Where PK is NULL (we only need to specify one primary key since it's covered with equalitySQL parts)
				m.PrimaryKeys[0].EscapedName()),
			// UPDATE
			fmt.Sprintf(`UPDATE %s as c SET %s FROM %s as cc WHERE %s%s;`,
				// UPDATE table set col1 = cc. col1
				m.FqTableName, columns.ColumnsUpdateQuery(ctx, cols, m.ColumnsToTypes, m.DestKind),
				// FROM table (temp) WHERE join on PK(s)
				m.SubQuery, strings.Join(equalitySQLParts, " and "), idempotentClause,
			),
		}, nil
	}

	// We also need to remove __artie flags since it does not exist in the destination table
	var removed bool
	for idx, col := range cols {
		if col == constants.DeleteColumnMarker {
			cols = append(cols[:idx], cols[idx+1:]...)
			removed = true
			break
		}
	}

	if !removed {
		return nil, errors.New("artie delete flag doesn't exist")
	}

	var pks []string
	for _, pk := range m.PrimaryKeys {
		pks = append(pks, pk.EscapedName())
	}

	return []string{
		// INSERT
		fmt.Sprintf(`INSERT INTO %s (%s) SELECT %s FROM %s as cc LEFT JOIN %s as c on %s WHERE c.%s IS NULL;`,
			// insert into target (col1, col2, col3)
			m.FqTableName, strings.Join(cols, ","),
			// SELECT cc.col1, cc.col2, ... FROM staging as CC
			array.StringsJoinAddPrefix(array.StringsJoinAddPrefixArgs{
				Vals:      cols,
				Separator: ",",
				Prefix:    "cc.",
			}), m.SubQuery,
			// LEFT JOIN table on pk(s)
			m.FqTableName, strings.Join(equalitySQLParts, " and "),
			// Where PK is NULL (we only need to specify one primary key since it's covered with equalitySQL parts)
			m.PrimaryKeys[0].EscapedName()),
		// UPDATE
		fmt.Sprintf(`UPDATE %s as c SET %s FROM %s as cc WHERE %s%s AND COALESCE(cc.%s, false) = false;`,
			// UPDATE table set col1 = cc. col1
			m.FqTableName, columns.ColumnsUpdateQuery(ctx, cols, m.ColumnsToTypes, m.DestKind),
			// FROM staging WHERE join on PK(s)
			m.SubQuery, strings.Join(equalitySQLParts, " and "), idempotentClause, constants.DeleteColumnMarker,
		),
		// DELETE
		fmt.Sprintf(`DELETE FROM %s WHERE (%s) IN (SELECT %s FROM %s as cc WHERE cc.%s = true);`,
			// DELETE from table where (pk_1, pk_2)
			m.FqTableName, strings.Join(pks, ","),
			// IN (cc.pk_1, cc.pk_2) FROM staging
			array.StringsJoinAddPrefix(array.StringsJoinAddPrefixArgs{
				Vals:      pks,
				Separator: ",",
				Prefix:    "cc.",
			}), m.SubQuery, constants.DeleteColumnMarker,
		),
	}, nil
}

func MergeStatement(ctx context.Context, m *MergeArgument) (string, error) {
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
		equalitySQL := fmt.Sprintf("c.%s = cc.%s", primaryKey.EscapedName(), primaryKey.EscapedName())
		pkCol, isOk := m.ColumnsToTypes.GetColumn(primaryKey.RawName())
		if !isOk {
			return "", fmt.Errorf("error: column: %s does not exist in columnToType: %v", primaryKey.RawName(), m.ColumnsToTypes)
		}

		if m.DestKind == constants.BigQuery && pkCol.KindDetails.Kind == typing.Struct.Kind {
			// BigQuery requires special casting to compare two JSON objects.
			equalitySQL = fmt.Sprintf("TO_JSON_STRING(c.%s) = TO_JSON_STRING(cc.%s)", primaryKey.EscapedName(), primaryKey.EscapedName())
		}

		equalitySQLParts = append(equalitySQLParts, equalitySQL)
	}

	subQuery := fmt.Sprintf("( %s )", m.SubQuery)
	if m.DestKind == constants.BigQuery {
		subQuery = m.SubQuery
	}

	cols := m.ColumnsToTypes.GetColumnsToUpdate(ctx, &sql.NameArgs{
		Escape:   true,
		DestKind: m.DestKind,
	})

	if m.SoftDelete {
		return fmt.Sprintf(`
			MERGE INTO %s c using %s as cc on %s
				when matched %sthen UPDATE
					SET %s
				when not matched AND IFNULL(cc.%s, false) = false then INSERT
					(
						%s
					)
					VALUES
					(
						%s
					);
		`, m.FqTableName, subQuery, strings.Join(equalitySQLParts, " and "),
			// Update + Soft Deletion
			idempotentClause, columns.ColumnsUpdateQuery(ctx, cols, m.ColumnsToTypes, m.DestKind),
			// Insert
			constants.DeleteColumnMarker, strings.Join(cols, ","),
			array.StringsJoinAddPrefix(array.StringsJoinAddPrefixArgs{
				Vals:      cols,
				Separator: ",",
				Prefix:    "cc.",
			})), nil
	}

	// We also need to remove __artie flags since it does not exist in the destination table
	var removed bool
	for idx, col := range cols {
		if col == constants.DeleteColumnMarker {
			cols = append(cols[:idx], cols[idx+1:]...)
			removed = true
			break
		}
	}

	if !removed {
		return "", errors.New("artie delete flag doesn't exist")
	}

	return fmt.Sprintf(`
			MERGE INTO %s c using %s as cc on %s
				when matched AND cc.%s then DELETE
				when matched AND IFNULL(cc.%s, false) = false %sthen UPDATE
					SET %s
				when not matched AND IFNULL(cc.%s, false) = false then INSERT
					(
						%s
					)
					VALUES
					(
						%s
					);
		`, m.FqTableName, subQuery, strings.Join(equalitySQLParts, " and "),
		// Delete
		constants.DeleteColumnMarker,
		// Update
		constants.DeleteColumnMarker, idempotentClause, columns.ColumnsUpdateQuery(ctx, cols, m.ColumnsToTypes, m.DestKind),
		// Insert
		constants.DeleteColumnMarker, strings.Join(cols, ","),
		array.StringsJoinAddPrefix(array.StringsJoinAddPrefixArgs{
			Vals:      cols,
			Separator: ",",
			Prefix:    "cc.",
		})), nil
}
