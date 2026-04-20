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

// DedupeStageRowIDColumn is the BIGINT IDENTITY column temporarily added to the
// permanent _dedupe table. It's the only row-identifier available in Redshift
// user tables, so it's how step 4's window picks winners and step 5's DELETE
// finds losers — all without ever reading SUPER columns like `meta`.
const DedupeStageRowIDColumn = "_artie_dedupe_rn"

// BuildDedupeQueriesFixed is a SUPER-safe replacement for BuildDedupeQueries.
//
// Background: rows whose SUPER value exceeds 64KB in its text form blow up
// with `Invalid input (8001): String value exceeds the max size of 65535
// bytes` on every query shape that routes those rows through PartiQL. In
// production we've now seen this error trigger on:
//
//   - `SELECT * ... QUALIFY ROW_NUMBER()` (window function)
//   - `WHERE (pk) IN (SELECT pk FROM source GROUP BY pk HAVING ...)` (hash
//     semi-join with subquery)
//   - `SELECT s.cols FROM source s INNER JOIN <DISTSTYLE ALL helper> d ON pk`
//     (hash join even when the build side is replicated to every slice)
//
// The only shapes we've seen survive on wide-SUPER data are:
//
//   a. `INSERT INTO <t2> (cols) SELECT cols FROM <t1>` — plain full-table
//      copy, no filter / join / window.
//   b. `CREATE ... AS SELECT non_super_cols FROM <t> QUALIFY ...` — Redshift's
//      columnar storage doesn't load `meta` when the projection doesn't
//      mention it, so no text conversion happens.
//   c. `DELETE FROM <t> USING <helper> WHERE <t>.key = <helper>.key` — a
//      DELETE never materializes the row payload; it only marks tombstones,
//      so SUPER stays on disk.
//
// This function's 8-statement plan is built exclusively from those three
// shapes, plus DDL:
//
//  1. Idempotent cleanup of any leftover _dedupe table from a prior failure.
//  2. Create the permanent _dedupe (LIKE source + BIGINT IDENTITY rn column).
//     LIKE preserves distkey / sortkey / column encoding / NOT NULL.
//  3. Full-table copy of source into _dedupe via shape (a). rn auto-populates.
//  4. Into a DISTSTYLE ALL temp `_losers` table, project the rn values whose
//     ROW_NUMBER() within each PK partition is > 1 — shape (b). Window sees
//     only PKs + __artie_updated_at + rn; `meta` is never read.
//  5. DELETE loser rows from _dedupe via shape (c). rn match, no SUPER I/O.
//  6. ALTER TABLE _dedupe DROP COLUMN rn — metadata-only in Redshift, near
//     instant regardless of table size.
//  7. DROP the original source.
//  8. Promote _dedupe to source's name via ALTER TABLE ... RENAME TO.
//
// `ExecContextStatements` runs all eight in a single transaction, so a failure
// anywhere rolls back — source is preserved intact. Callers must hold an
// exclusive-ish position (concurrent writers during the transaction will lose
// their changes when _dedupe is promoted).
//
// Trade-offs vs. the legacy in-place DELETE/INSERT:
//
//   - Peak storage is ~2x the source table during the dedupe.
//   - GRANTs, views, and FKs that reference the original source table survive
//     the RENAME (they attach by OID, not name), but any object created
//     against the _dedupe name during the window is visible only until the
//     rename.
//   - The stagingTableID argument is repurposed as the _losers temp table —
//     the legacy SELECT-style staging is no longer needed.
//
// Ordering and row selection mirror the original's ASC direction with the
// IDENTITY rn as a deterministic final tiebreaker. The legacy `= 2` quirk is
// dropped here in favor of `> 1` (identifies every loser per PK) which is
// semantically cleaner; the row retained per PK is identical whenever there's
// no __artie_updated_at tie.
func (rd RedshiftDialect) BuildDedupeQueriesFixed(tableID, stagingTableID sql.TableIdentifier, primaryKeys []string, includeArtieUpdatedAt bool, columns []string) []string {
	primaryKeysEscaped := sql.QuoteIdentifiers(primaryKeys, rd)
	pkTuple := strings.Join(primaryKeysEscaped, ", ")
	colList := strings.Join(sql.QuoteIdentifiers(columns, rd), ", ")
	rnCol := rd.QuoteIdentifier(DedupeStageRowIDColumn)

	dedupeNewID := tableID.WithTable(fmt.Sprintf("%s_%s_dedupe", tableID.Table(), constants.ArtiePrefix))
	losersID := stagingTableID

	// 1. Idempotent cleanup in case a prior dedupe died between CREATE and
	//    RENAME. Safe no-op on the happy path.
	dropPrev := fmt.Sprintf("DROP TABLE IF EXISTS %s", dedupeNewID.FullyQualifiedName())

	// 2. Permanent _dedupe that mirrors source plus a BIGINT IDENTITY
	//    tiebreaker. CREATE TABLE LIKE preserves distkey / sortkey / encoding
	//    / NOT NULL so the swapped-in table matches the original's physical
	//    layout.
	createDedupeNew := fmt.Sprintf(
		"CREATE TABLE %s (LIKE %s, %s BIGINT IDENTITY(1,1))",
		dedupeNewID.FullyQualifiedName(),
		tableID.FullyQualifiedName(),
		rnCol,
	)

	// 3. Full-table copy. Plain INSERT ... SELECT — the ONE query shape
	//    Redshift handles safely for SUPER values larger than 64KB. rn
	//    auto-populates from the IDENTITY.
	copyAll := fmt.Sprintf(
		"INSERT INTO %s (%s) SELECT %s FROM %s",
		dedupeNewID.FullyQualifiedName(),
		colList,
		colList,
		tableID.FullyQualifiedName(),
	)

	// 4. Identify the rn of every non-winner. Projection is rn only; window
	//    references PKs + __artie_updated_at + rn. Columnar storage means
	//    `meta` is never loaded, so no text serialization happens.
	//    DISTSTYLE ALL keeps _losers colocated with _dedupe on every slice
	//    for the step-5 DELETE.
	var orderCols []string
	for _, pk := range primaryKeysEscaped {
		orderCols = append(orderCols, fmt.Sprintf("%s ASC", pk))
	}
	if includeArtieUpdatedAt {
		orderCols = append(orderCols, fmt.Sprintf("%s ASC", rd.QuoteIdentifier(constants.UpdateColumnMarker)))
	}
	orderCols = append(orderCols, fmt.Sprintf("%s ASC", rnCol))

	// `WHERE true` is required before QUALIFY for Redshift.
	findLosers := fmt.Sprintf(
		"CREATE TEMPORARY TABLE %s DISTSTYLE ALL AS SELECT %s FROM %s WHERE true QUALIFY ROW_NUMBER() OVER (PARTITION BY %s ORDER BY %s) > 1",
		losersID.EscapedTable(),
		rnCol,
		dedupeNewID.FullyQualifiedName(),
		pkTuple,
		strings.Join(orderCols, ", "),
	)

	// 5. Delete every loser row from _dedupe. DELETE never materializes the
	//    row payload; it only marks tombstones. SUPER stays on disk. Match
	//    is on the BIGINT rn alone, so the join is cheap.
	deleteLosers := fmt.Sprintf(
		"DELETE FROM %s USING %s l WHERE %s.%s = l.%s",
		dedupeNewID.FullyQualifiedName(),
		losersID.EscapedTable(),
		dedupeNewID.EscapedTable(),
		rnCol,
		rnCol,
	)

	// 6. Drop the IDENTITY helper column. Redshift implements DROP COLUMN as
	//    a metadata-only catalog update (space is reclaimed by the next
	//    VACUUM), so this is effectively instant regardless of table size.
	dropCol := fmt.Sprintf(
		"ALTER TABLE %s DROP COLUMN %s",
		dedupeNewID.FullyQualifiedName(),
		rnCol,
	)

	// 7 + 8. Atomic swap. DROP the old source then rename _dedupe into place.
	dropSource := fmt.Sprintf("DROP TABLE %s", tableID.FullyQualifiedName())
	rename := fmt.Sprintf("ALTER TABLE %s RENAME TO %s", dedupeNewID.FullyQualifiedName(), tableID.EscapedTable())

	return []string{
		dropPrev,
		createDedupeNew,
		copyAll,
		findLosers,
		deleteLosers,
		dropCol,
		dropSource,
		rename,
	}
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
