package dialect

import (
	"fmt"
	"strings"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

type RedshiftDialect struct{}

func (RedshiftDialect) ReservedColumnNames() map[string]bool {
	return nil
}

func (rd RedshiftDialect) QuoteIdentifier(identifier string) string {
	// Preserve the existing behavior of Redshift identifiers being lowercased due to not being quoted.
	return fmt.Sprintf(`"%s"`, strings.ToLower(strings.ReplaceAll(identifier, `"`, ``)))
}

func (RedshiftDialect) EscapeStruct(value string) string {
	return fmt.Sprintf("JSON_PARSE(%s)", sql.QuoteLiteral(value))
}

func (RedshiftDialect) IsColumnAlreadyExistsErr(err error) bool {
	// Redshift's error: ERROR: column "foo" of relation "statement" already exists
	return strings.Contains(err.Error(), "already exists")
}

func (RedshiftDialect) IsTableDoesNotExistErr(err error) bool {
	if err == nil {
		return false
	}

	// 42P01 is the SQLSTATE code for table does not exist.
	if strings.Contains(err.Error(), "does not exist (SQLSTATE 42P01)") {
		return true
	}

	return false
}

func (rd RedshiftDialect) BuildIsNotToastValueExpression(tableAlias constants.TableAlias, column columns.Column) string {
	toastedValue := "%" + constants.ToastUnavailableValuePlaceholder + "%"
	colName := sql.QuoteTableAliasColumn(tableAlias, column, rd)

	switch column.KindDetails {
	case typing.Struct, typing.Array:
		// We need to use JSON_SIZE to check if the column can be serialized into a VARCHAR
		// If the value is greater than 500 characters, it's likely not going to be toasted, so we can skip the check.
		return fmt.Sprintf(`
COALESCE(
    CASE
        WHEN JSON_SIZE(%s) < 500 THEN JSON_SERIALIZE(%s) NOT LIKE '%s'
    ELSE
        TRUE
    END,
    TRUE
)`, colName, colName, toastedValue)
	default:
		return fmt.Sprintf(`COALESCE(%s NOT LIKE '%s', TRUE)`, colName, toastedValue)
	}
}

func (rd RedshiftDialect) BuildDedupeQueries(tableID, stagingTableID sql.TableIdentifier, primaryKeys []string, includeArtieUpdatedAt bool) []string {
	primaryKeysEscaped := sql.QuoteIdentifiers(primaryKeys, rd)

	orderColsToIterate := primaryKeysEscaped
	if includeArtieUpdatedAt {
		orderColsToIterate = append(orderColsToIterate, rd.QuoteIdentifier(constants.UpdateColumnMarker))
	}

	var orderByCols []string
	for _, orderByCol := range orderColsToIterate {
		orderByCols = append(orderByCols, fmt.Sprintf("%s ASC", orderByCol))
	}

	var parts []string
	parts = append(parts,
		// It looks funny, but we do need a WHERE clause to make the query valid.
		fmt.Sprintf("CREATE TEMPORARY TABLE %s AS (SELECT * FROM %s WHERE true QUALIFY ROW_NUMBER() OVER (PARTITION BY %s ORDER BY %s) = 2)",
			// Temporary tables may not specify a schema name
			stagingTableID.EscapedTable(),
			tableID.FullyQualifiedName(),
			strings.Join(primaryKeysEscaped, ", "),
			strings.Join(orderByCols, ", "),
		),
	)

	var whereClauses []string
	for _, primaryKeyEscaped := range primaryKeysEscaped {
		// Redshift does not support table aliasing for deletes.
		whereClauses = append(whereClauses, fmt.Sprintf("%s.%s = t2.%s", tableID.EscapedTable(), primaryKeyEscaped, primaryKeyEscaped))
	}

	// Delete duplicates in the main table based on matches with the staging table
	parts = append(parts,
		fmt.Sprintf("DELETE FROM %s USING %s t2 WHERE %s",
			tableID.FullyQualifiedName(),
			stagingTableID.EscapedTable(),
			strings.Join(whereClauses, " AND "),
		),
	)

	// Insert deduplicated data back into the main table from the staging table
	parts = append(parts,
		fmt.Sprintf("INSERT INTO %s SELECT * FROM %s",
			tableID.FullyQualifiedName(),
			stagingTableID.EscapedTable(),
		),
	)

	return parts
}

// DedupeStageRowIDColumn is the IDENTITY tiebreaker appended to the dedupe
// staging table built by BuildDedupeQueriesFixed. Having it lets the winner
// QUALIFY reference only primary keys + __artie_updated_at + this BIGINT,
// so wide VARCHAR and SUPER columns never flow through a window operator.
const DedupeStageRowIDColumn = "_artie_dedupe_rn"

// BuildDedupeQueriesFixed is a SUPER-safe variant of BuildDedupeQueries.
//
// Background: two distinct Redshift behaviors trip the `Invalid input (8001):
// String value exceeds the max size of 65535 bytes` error on any table with a
// SUPER (or VARCHAR > 64KB) column:
//
//  1. Window / PartiQL operators implicitly serialize SUPER to VARCHAR(65535).
//     The legacy `SELECT * ... QUALIFY ROW_NUMBER()` hits this on every wide
//     row because every column is routed through the window.
//  2. Cross-slice row redistribution also serializes SUPER to the same
//     VARCHAR(65535) wire form. Any query shape that forces the outer
//     (SUPER-bearing) rows to be redistributed — `WHERE (pk) IN (<subquery>)`
//     against the same large source being the canonical offender — hits the
//     identical error without any window in sight.
//
// This variant dodges both by (a) never routing SUPER through a window and
// (b) pinning SUPER-bearing rows to their home slice by joining only against
// small `DISTSTYLE ALL` helper temp tables. The five statements are:
//
//  1. CREATE the main stage: mirrors the source plus a BIGINT IDENTITY.
//  2. CREATE a `_pks` temp table with DISTSTYLE ALL holding every duplicated
//     PK tuple (GROUP BY PK, HAVING COUNT(*) > 1). Small, replicated to every
//     slice.
//  3. Populate the main stage via `source INNER JOIN _pks` on the PKs. With
//     _pks on every slice the planner can do a local colocated join; source
//     rows (including SUPER) never leave their slice.
//  4. DELETE duplicated-PK rows from source using _pks (not the main stage),
//     so the DELETE plan never touches SUPER columns.
//  5. Reinsert one winner per PK from the main stage. The inner QUALIFY
//     projects only rn and references only PKs + __artie_updated_at + rn,
//     so SUPER never enters the window.
//
// `columns` must list every source column in ordinal order (the caller pulls
// them from information_schema.columns). The IDENTITY column is auto-populated
// by Redshift and is intentionally omitted from `columns` and from the
// explicit INSERT column lists.
//
// Ordering and row selection exactly mirror the original: ORDER BY … ASC
// with `= 2`, so the produced row per duplicated PK is the same one the
// legacy query would have kept. The IDENTITY is appended as a deterministic
// final tiebreaker — the legacy query had no tiebreaker at all.
func (rd RedshiftDialect) BuildDedupeQueriesFixed(tableID, stagingTableID sql.TableIdentifier, primaryKeys []string, includeArtieUpdatedAt bool, columns []string) []string {
	primaryKeysEscaped := sql.QuoteIdentifiers(primaryKeys, rd)
	pkTuple := strings.Join(primaryKeysEscaped, ", ")
	escapedColumns := sql.QuoteIdentifiers(columns, rd)
	colList := strings.Join(escapedColumns, ", ")
	rnCol := rd.QuoteIdentifier(DedupeStageRowIDColumn)

	dupPKsID := stagingTableID.WithTable(stagingTableID.Table() + "_pks")

	// 1. Stage table mirrors the source plus a BIGINT IDENTITY tiebreaker.
	createStage := fmt.Sprintf(
		"CREATE TEMPORARY TABLE %s (LIKE %s, %s BIGINT IDENTITY(1,1))",
		stagingTableID.EscapedTable(),
		tableID.FullyQualifiedName(),
		rnCol,
	)

	// 2. Duplicated-PK list as a DISTSTYLE ALL temp table. Replicated to every
	//    slice so the step-3 join is colocated and source rows (with SUPER)
	//    stay put.
	createDupPKs := fmt.Sprintf(
		"CREATE TEMPORARY TABLE %s DISTSTYLE ALL AS SELECT %s FROM %s GROUP BY %s HAVING COUNT(*) > 1",
		dupPKsID.EscapedTable(),
		pkTuple,
		tableID.FullyQualifiedName(),
		pkTuple,
	)

	// 3. Populate the main stage via INNER JOIN against dup_pks. Plain
	//    projection, no window, no subquery — SUPER flows at its native ~1MB
	//    cap.
	var joinPreds []string
	for _, pk := range primaryKeysEscaped {
		joinPreds = append(joinPreds, fmt.Sprintf("s.%s = d.%s", pk, pk))
	}
	sColumnList := make([]string, 0, len(escapedColumns))
	for _, col := range escapedColumns {
		sColumnList = append(sColumnList, fmt.Sprintf("s.%s", col))
	}
	populate := fmt.Sprintf(
		"INSERT INTO %s (%s) SELECT %s FROM %s s INNER JOIN %s d ON %s",
		stagingTableID.EscapedTable(),
		colList,
		strings.Join(sColumnList, ", "),
		tableID.FullyQualifiedName(),
		dupPKsID.EscapedTable(),
		strings.Join(joinPreds, " AND "),
	)

	// 4. Remove every duplicated-PK row from the main table. Using dup_pks
	//    (no SUPER columns at all) keeps the DELETE plan narrow.
	var deletePreds []string
	for _, pk := range primaryKeysEscaped {
		deletePreds = append(deletePreds, fmt.Sprintf("%s.%s = d.%s", tableID.EscapedTable(), pk, pk))
	}
	deleteDupes := fmt.Sprintf("DELETE FROM %s USING %s d WHERE %s",
		tableID.FullyQualifiedName(),
		dupPKsID.EscapedTable(),
		strings.Join(deletePreds, " AND "),
	)

	// 5. Reinsert one winner per PK. The inner subquery projects only rn and
	//    the window references only PKs + __artie_updated_at + rn, so SUPER
	//    never touches the window. The IN list is a set of BIGINTs (small);
	//    broadcast is cheap and the outer rows stay colocated with stage.
	var orderCols []string
	for _, pk := range primaryKeysEscaped {
		orderCols = append(orderCols, fmt.Sprintf("%s ASC", pk))
	}
	if includeArtieUpdatedAt {
		orderCols = append(orderCols, fmt.Sprintf("%s ASC", rd.QuoteIdentifier(constants.UpdateColumnMarker)))
	}
	orderCols = append(orderCols, fmt.Sprintf("%s ASC", rnCol))

	// `WHERE true` is required before QUALIFY for Redshift.
	insertWinners := fmt.Sprintf(
		"INSERT INTO %s (%s) SELECT %s FROM %s WHERE %s IN (SELECT %s FROM %s WHERE true QUALIFY ROW_NUMBER() OVER (PARTITION BY %s ORDER BY %s) = 2)",
		tableID.FullyQualifiedName(),
		colList,
		colList,
		stagingTableID.EscapedTable(),
		rnCol,
		rnCol,
		stagingTableID.EscapedTable(),
		pkTuple,
		strings.Join(orderCols, ", "),
	)

	return []string{createStage, createDupPKs, populate, deleteDupes, insertWinners}
}

func (rd RedshiftDialect) buildMergeInsertQuery(
	tableID sql.TableIdentifier,
	subQuery string,
	primaryKeys []columns.Column,
	cols []columns.Column,
	softDelete bool,
) string {
	// Only reference the first primary key here since the ON clause (equalitySQL) already covers all PKs.
	whereClause := fmt.Sprintf("%s IS NULL", sql.QuoteTableAliasColumn(constants.TargetAlias, primaryKeys[0], rd))
	if !softDelete {
		whereClause += fmt.Sprintf(" AND COALESCE(%s, false) = false", sql.QuotedDeleteColumnMarker(constants.StagingAlias, rd))
	}

	return fmt.Sprintf(`INSERT INTO %s (%s) SELECT %s FROM %s AS %s LEFT JOIN %s AS %s ON %s WHERE %s;`,
		// INSERT INTO %s (%s)
		tableID.FullyQualifiedName(), strings.Join(sql.QuoteColumns(cols, rd), ","),
		// SELECT %s FROM %s AS %s
		strings.Join(sql.QuoteTableAliasColumns(constants.StagingAlias, cols, rd), ","), subQuery, constants.StagingAlias,
		// LEFT JOIN %s AS %s ON %s
		tableID.FullyQualifiedName(), constants.TargetAlias, strings.Join(sql.BuildColumnComparisons(primaryKeys, constants.TargetAlias, constants.StagingAlias, sql.Equal, rd), " AND "),
		// WHERE %s
		whereClause,
	)
}

func (rd RedshiftDialect) buildMergeUpdateQueries(
	tableID sql.TableIdentifier,
	subQuery string,
	primaryKeys []columns.Column,
	cols []columns.Column,
	softDelete bool,
) []string {
	clauses := sql.BuildColumnComparisons(primaryKeys, constants.TargetAlias, constants.StagingAlias, sql.Equal, rd)

	if !softDelete {
		clauses = append(clauses, fmt.Sprintf("COALESCE(%s, false) = false", sql.QuotedDeleteColumnMarker(constants.StagingAlias, rd)))
		return []string{fmt.Sprintf(`UPDATE %s AS %s SET %s FROM %s AS %s WHERE %s;`,
			// UPDATE table set col1 = stg.col1, col2 = stg.col2...
			tableID.FullyQualifiedName(), constants.TargetAlias, sql.BuildColumnsUpdateFragment(cols, constants.StagingAlias, constants.TargetAlias, rd),
			// FROM staging WHERE tgt.pk = stg.pk
			subQuery, constants.StagingAlias, strings.Join(clauses, " AND "),
		)}
	}

	// If soft delete is enabled, issue two updates; one to update rows where all columns should be updated,
	// and one to update rows where only the __artie_delete column should be updated.
	return []string{
		fmt.Sprintf(`UPDATE %s AS %s SET %s FROM %s AS %s WHERE %s AND COALESCE(%s, false) = false;`,
			// UPDATE table set [all columns]
			tableID.FullyQualifiedName(), constants.TargetAlias, sql.BuildColumnsUpdateFragment(cols, constants.StagingAlias, constants.TargetAlias, rd),
			// FROM staging WHERE tgt.pk = stg.pk and __artie_only_set_delete = false
			subQuery, constants.StagingAlias, strings.Join(clauses, " AND "), sql.GetQuotedOnlySetDeleteColumnMarker(constants.StagingAlias, rd),
		),
		fmt.Sprintf(`UPDATE %s AS %s SET %s FROM %s AS %s WHERE %s AND COALESCE(%s, false) = true;`,
			// UPDATE table set __artie_delete = stg.__artie_delete
			tableID.FullyQualifiedName(), constants.TargetAlias, sql.BuildColumnsUpdateFragment([]columns.Column{columns.NewColumn(constants.DeleteColumnMarker, typing.Boolean)}, constants.StagingAlias, constants.TargetAlias, rd),
			// FROM staging WHERE tgt.pk = stg.pk and __artie_only_set_delete = true
			subQuery, constants.StagingAlias, strings.Join(clauses, " AND "), sql.GetQuotedOnlySetDeleteColumnMarker(constants.StagingAlias, rd),
		),
	}
}

func (rd RedshiftDialect) buildMergeDeleteQuery(tableID sql.TableIdentifier, subQuery string, primaryKeys []columns.Column) string {
	return fmt.Sprintf(`DELETE FROM %s WHERE (%s) IN (SELECT %s FROM %s AS %s WHERE %s = true);`,
		// DELETE FROM %s WHERE (%s)
		tableID.FullyQualifiedName(), strings.Join(sql.QuoteColumns(primaryKeys, rd), ","),
		// IN (SELECT %s FROM %s AS %s
		strings.Join(sql.QuoteTableAliasColumns(constants.StagingAlias, primaryKeys, rd), ","), subQuery, constants.StagingAlias,
		// WHERE %s = true);
		sql.QuotedDeleteColumnMarker(constants.StagingAlias, rd),
	)
}

func (rd RedshiftDialect) BuildMergeQueries(
	tableID sql.TableIdentifier,
	subQuery string,
	primaryKeys []columns.Column,
	_ []string,
	cols []columns.Column,
	softDelete bool,
	containsHardDeletes bool,
) ([]string, error) {
	cols, err := columns.RemoveOnlySetDeleteColumnMarker(cols)
	if err != nil {
		return []string{}, err
	}

	if !softDelete {
		// We also need to remove __artie flags since it does not exist in the destination table
		cols, err = columns.RemoveDeleteColumnMarker(cols)
		if err != nil {
			return nil, err
		}
	}

	// We want to issue the update first, then the insert, then the delete.
	// This order is important for us to avoid no-ops, where rows get inserted and then immediately updated.
	parts := rd.buildMergeUpdateQueries(tableID, subQuery, primaryKeys, cols, softDelete)
	parts = append(parts, rd.buildMergeInsertQuery(tableID, subQuery, primaryKeys, cols, softDelete))
	if !softDelete && containsHardDeletes {
		parts = append(parts, rd.buildMergeDeleteQuery(tableID, subQuery, primaryKeys))
	}

	return parts, nil
}

func (rd RedshiftDialect) BuildIncreaseStringPrecisionQuery(tableID sql.TableIdentifier, columnName string, newPrecision int32) string {
	return fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s TYPE VARCHAR(%d)", tableID.FullyQualifiedName(), rd.QuoteIdentifier(columnName), newPrecision)
}

func (RedshiftDialect) BuildSweepQuery(_, schemaName string) (string, []any) {
	// `relkind` will filter for only ordinary tables and exclude sequences, views, etc.
	return `
SELECT
    n.nspname, c.relname
FROM
    PG_CATALOG.PG_CLASS c
JOIN
    PG_CATALOG.PG_NAMESPACE n ON n.oid = c.relnamespace
WHERE
    n.nspname = $1 AND c.relname ILIKE $2 AND c.relkind = 'r';`, []any{schemaName, "%" + constants.ArtiePrefix + "%"}
}

func (rd RedshiftDialect) BuildCopyStatement(tableID sql.TableIdentifier, cols []string, s3URI, credentialsClause string) string {
	quotedColumns := make([]string, len(cols))
	for i, col := range cols {
		quotedColumns[i] = rd.QuoteIdentifier(col)
	}

	return fmt.Sprintf(`COPY %s (%s) FROM %s DELIMITER '\t' NULL AS %s GZIP FORMAT CSV %s dateformat 'auto' timeformat 'auto';`,
		// COPY
		tableID.FullyQualifiedName(), strings.Join(quotedColumns, ","),
		// Filepath
		sql.QuoteLiteral(s3URI),
		// CSV option and credential clause
		sql.QuoteLiteral(constants.NullValuePlaceholder), credentialsClause,
	)
}

func (RedshiftDialect) BuildMergeQueryIntoStagingTable(tableID sql.TableIdentifier, subQuery string, primaryKeys []columns.Column, additionalEqualityStrings []string, cols []columns.Column) []string {
	panic("not implemented")
}
