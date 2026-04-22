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

// DedupeStageRowIDColumn is the BIGINT IDENTITY helper column temporarily added
// to the _dedupe table. Redshift user tables have no stable row identifier, so
// this is how step 3's GROUP BY picks winners and step 4's DELETE targets
// losers — all without ever reading SUPER columns like `meta`.
const DedupeStageRowIDColumn = "_artie_dedupe_rn"

// RedshiftDedupePlan is the three-phase output of [RedshiftDialect.BuildDedupeQueriesFixed].
// See that function's comment for the full rationale; the TL;DR is that Redshift
// forbids ALTER TABLE APPEND inside a transaction block, so the plan is split
// into three groups that the caller must dispatch separately:
//
//   - Prep (single statement): CREATE the empty _dedupe table. Will fail fast
//     if _dedupe already exists — that's deliberate, see the function comment.
//   - Append (single statement, auto-commits, NOT rollback-able): moves data
//     blocks from source into _dedupe via ALTER TABLE APPEND FILLTARGET.
//   - Swap (transactional, rollback-safe): dedupe within _dedupe, DROP source,
//     RENAME _dedupe into place.
//
// Passing each field to [ExecContextStatements] in order does the right thing,
// because ExecContextStatements wraps 2+-statement slices in BEGIN/END and runs
// a single statement bare.
type RedshiftDedupePlan struct {
	Prep   []string
	Append string
	Swap   []string
}

// BuildDedupeQueriesFixed is a SUPER-safe replacement for BuildDedupeQueries.
//
// Background: rows whose SUPER value exceeds 64KB in its text form blow up
// with `Invalid input (8001): String value exceeds the max size of 65535
// bytes` on every query shape that routes those rows through PartiQL. In
// production we've seen this error trigger on:
//
//   - `SELECT * ... QUALIFY ROW_NUMBER()` (window function over `SELECT *`)
//   - `WHERE (pk) IN (SELECT pk FROM source GROUP BY pk HAVING ...)` (hash
//     semi-join whose outer scan includes SUPER)
//   - `SELECT s.cols FROM source s INNER JOIN <DISTSTYLE ALL helper> d ON pk`
//     (hash join even when the build side is replicated to every slice)
//
// The query shapes we've confirmed survive on wide-SUPER data are:
//
//	a. `ALTER TABLE <t2> APPEND FROM <t1> FILLTARGET` — moves data blocks at
//	   the storage layer without ever re-reading row payloads. SUPER is
//	   never serialized. This is also dramatically faster than INSERT ...
//	   SELECT and uses ~0 extra peak storage (blocks move, they aren't
//	   duplicated).
//	b. `CREATE ... AS SELECT non_super_cols FROM <t> ...` — Redshift's
//	   columnar storage doesn't load `meta` when the projection doesn't
//	   mention it, so no text conversion happens.
//	c. `DELETE FROM <t> USING <helper> WHERE <t>.key = <helper>.key` — a
//	   DELETE never materializes the row payload; it only marks tombstones,
//	   so SUPER stays on disk.
//
// The plan assembled from those three shapes plus DDL:
//
//  1. Create the permanent _dedupe (LIKE source INCLUDING DEFAULTS + BIGINT
//     IDENTITY rn column). LIKE preserves distkey / sortkey / column encoding
//     / NOT NULL, and INCLUDING DEFAULTS carries over column DEFAULT
//     expressions so the swapped-in table is behaviourally identical to the
//     original for future INSERTs. We deliberately do NOT `DROP TABLE IF
//     EXISTS` first — see "Failure modes" below.
//  2. Move data from source into _dedupe via shape (a). FILLTARGET lets
//     Redshift populate the target-only rn column via its IDENTITY clause
//     (seed=1, step=1). After this, source is empty and _dedupe has every row.
//  3. Into a DISTSTYLE ALL temp `_losers` table, project the rn values that
//     are NOT the MAX(rn) within their PK group — shape (b). Projection is
//     rn only; the subquery aggregates rn grouped by PKs. `meta` is never
//     read.
//  4. DELETE loser rows from _dedupe via shape (c). rn match, no SUPER I/O.
//  5. ALTER TABLE _dedupe DROP COLUMN rn — metadata-only in Redshift, near
//     instant regardless of table size.
//  6. DROP the original (now-empty) source.
//  7. Promote _dedupe to source's name via ALTER TABLE ... RENAME TO.
//
// Why three phases instead of one? AWS: "An ALTER TABLE APPEND command
// automatically commits immediately upon completion of the operation. It can't
// be rolled back. You can't run ALTER TABLE APPEND within a transaction block
// (BEGIN ... END)." So step 2 MUST be issued outside any BEGIN/END. Steps 3-7
// are grouped into one transaction so the final rename is atomic with the
// dedupe work (rollback leaves _dedupe intact and source empty but recoverable
// by hand).
//
// Failure modes & recovery:
//
//   - Fails in Prep (CREATE errored): source untouched. Safe to retry — the
//     re-CREATE will succeed if _dedupe wasn't actually created, or will fail
//     fast with "already exists" if it was (operator intervention required).
//   - Fails at APPEND itself: per AWS docs APPEND is all-or-nothing on failure,
//     so no blocks moved. source intact, _dedupe exists but empty. Retry will
//     fail fast at Prep's CREATE ("already exists"); operator must DROP the
//     empty _dedupe then re-run.
//   - Fails anywhere in Swap: the Swap transaction rolls back. _dedupe still
//     holds every source row; source is empty. Retry will fail at Prep's
//     CREATE; operator must either re-run just the Swap SQL by hand, or DROP
//     the empty source and RENAME _dedupe into place manually.
//
// The fail-fast-on-existing-_dedupe behavior is intentional: blindly dropping
// a pre-existing _dedupe is unsafe because in the post-APPEND failure state
// that table is the ONLY copy of the data. We'd rather refuse to proceed and
// surface the state for human inspection than risk silent data loss.
//
// Why MAX(rn) GROUP BY pk instead of ROW_NUMBER() ordered by
// __artie_updated_at? This function only runs during initial snapshot
// dedupe. CDC streaming starts *after* snapshot completes successfully, so
// any duplicates present at dedupe time represent either (a) the same
// snapshot row emitted twice by the source connector, or (b) rows that the
// subsequent CDC stream will overwrite on its first event for that PK.
// Either way, which duplicate survives is immaterial — MAX(rn) is simpler
// and avoids needing the __artie_updated_at column in the query.
//
// Other trade-offs vs. the legacy in-place DELETE/INSERT:
//
//   - GRANTs, views, and FKs that reference the original source table do
//     NOT survive the DROP + RENAME (they bind by OID, and the OID
//     changes). If any such objects exist, they must be re-created by the
//     caller after dedupe.
func (rd RedshiftDialect) BuildDedupeQueriesFixed(tableID, losersID sql.TableIdentifier, primaryKeys []string) RedshiftDedupePlan {
	primaryKeysEscaped := sql.QuoteIdentifiers(primaryKeys, rd)
	pkTuple := strings.Join(primaryKeysEscaped, ", ")
	rnCol := rd.QuoteIdentifier(DedupeStageRowIDColumn)

	dedupeNewID := tableID.WithTable(fmt.Sprintf("%s_%s_dedupe", tableID.Table(), constants.ArtiePrefix))

	// 1. Permanent _dedupe that mirrors source plus a BIGINT IDENTITY
	//    tiebreaker. CREATE TABLE LIKE preserves distkey / sortkey / encoding
	//    / NOT NULL; INCLUDING DEFAULTS carries over column DEFAULT expressions
	//    too, so the swapped-in table is behaviourally identical to the
	//    original for future INSERTs. No `IF NOT EXISTS` on purpose: if a
	//    prior dedupe left _dedupe behind, we want this to error out rather
	//    than silently proceed (see the function comment for why).
	createDedupeNew := fmt.Sprintf(
		"CREATE TABLE %s (LIKE %s INCLUDING DEFAULTS, %s BIGINT IDENTITY(1,1))",
		dedupeNewID.FullyQualifiedName(),
		tableID.FullyQualifiedName(),
		rnCol,
	)

	// 2. Block-level move. ALTER TABLE APPEND re-points source's storage
	//    blocks into _dedupe without rewriting rows, so SUPER is never
	//    serialized. FILLTARGET tells Redshift to populate target-only
	//    columns (our IDENTITY rn) per their DEFAULT/IDENTITY rules.
	//    MUST run outside BEGIN/END — caller handles this via a dedicated
	//    single-statement ExecContextStatements call.
	appendData := fmt.Sprintf(
		"ALTER TABLE %s APPEND FROM %s FILLTARGET",
		dedupeNewID.FullyQualifiedName(),
		tableID.FullyQualifiedName(),
	)

	// 3. Identify the rn of every non-winner. Projection is rn only; the
	//    subquery aggregates rn grouped by PKs. Columnar storage means
	//    `meta` is never loaded, so no text serialization happens.
	//    DISTSTYLE ALL keeps _losers colocated with _dedupe on every slice
	//    for the step-4 DELETE. rn is NOT NULL (IDENTITY columns default to
	//    NOT NULL), so NOT IN is free of the usual null-pitfall.
	findLosers := fmt.Sprintf(
		"CREATE TEMPORARY TABLE %s DISTSTYLE ALL AS SELECT %s FROM %s WHERE %s NOT IN (SELECT MAX(%s) FROM %s GROUP BY %s)",
		losersID.EscapedTable(),
		rnCol,
		dedupeNewID.FullyQualifiedName(),
		rnCol,
		rnCol,
		dedupeNewID.FullyQualifiedName(),
		pkTuple,
	)

	// 4. Delete every loser row from _dedupe. DELETE never materializes the
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

	// 5. Drop the IDENTITY helper column. Redshift implements DROP COLUMN as
	//    a metadata-only catalog update (space is reclaimed by the next
	//    VACUUM), so this is effectively instant regardless of table size.
	dropCol := fmt.Sprintf(
		"ALTER TABLE %s DROP COLUMN %s",
		dedupeNewID.FullyQualifiedName(),
		rnCol,
	)

	// 6 + 7. Swap. DROP the (already-empty) source then rename _dedupe into
	//    place. Both inside the Swap transaction, so rollback on failure
	//    leaves _dedupe standing with the data still inside.
	dropSource := fmt.Sprintf("DROP TABLE %s", tableID.FullyQualifiedName())
	rename := fmt.Sprintf("ALTER TABLE %s RENAME TO %s", dedupeNewID.FullyQualifiedName(), tableID.EscapedTable())

	return RedshiftDedupePlan{
		Prep:   []string{createDedupeNew},
		Append: appendData,
		Swap:   []string{findLosers, deleteLosers, dropCol, dropSource, rename},
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
