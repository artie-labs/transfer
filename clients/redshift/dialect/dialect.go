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

	switch column.KindDetails.Kind {
	case typing.Struct.Kind, typing.Array.Kind:
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
// to the _dedupe table so we can pick winners (MAX(rn) per PK) and DELETE
// losers without ever reading SUPER columns.
const DedupeStageRowIDColumn = "_artie_dedupe_rn"

// RedshiftDedupePlan is the output of [RedshiftDialect.BuildDedupeQueriesAlterTableAppend].
//
// ALTER TABLE APPEND cannot run inside a transaction block, so the plan is
// split into five groups the caller dispatches separately. Slices go through
// [destination.ExecContextStatements] (which wraps 2+ statements in BEGIN/END);
// the two string fields go through a bare ExecContext so APPEND auto-commits.
//
//   - Prep: CREATE _dedupe. Fails fast if _dedupe already exists — deliberate.
//   - AppendIn: source → _dedupe via APPEND ... FILLTARGET (auto-commit).
//   - Dedupe (txn): build _losers temp + DELETE dupes from _dedupe.
//   - AppendOut: _dedupe → source via APPEND ... IGNOREEXTRA, which drops the
//     rn column along the way (auto-commit).
//   - Cleanup: DROP the now-empty _dedupe.
type RedshiftDedupePlan struct {
	Prep      []string
	AppendIn  string
	Dedupe    []string
	AppendOut string
	Cleanup   []string
}

// BuildDedupeQueriesAlterTableAppend is a SUPER-safe replacement for BuildDedupeQueries.
//
// Rows whose SUPER value exceeds 64KB in text form blow up with `Invalid input
// (8001): String value exceeds the max size of 65535 bytes` on any query shape
// that routes them through PartiQL — including `SELECT * ... QUALIFY
// ROW_NUMBER()`, IN-subquery semi-joins, and hash joins even against a
// DISTSTYLE ALL helper. The shapes that survive on wide-SUPER data are:
//
//	a. ALTER TABLE APPEND [FILLTARGET | IGNOREEXTRA] — storage-level block
//	   move, never re-reads row payloads. Also ~free on peak storage.
//	b. CREATE ... AS SELECT non_super_cols — columnar storage doesn't load
//	   columns absent from the projection.
//	c. DELETE ... USING — marks tombstones; never materializes rows.
//
// The plan, built from those shapes plus DDL:
//
//  1. CREATE _dedupe (LIKE source INCLUDING DEFAULTS, rn BIGINT IDENTITY). LIKE
//     preserves distkey / sortkey / encoding / NOT NULL; INCLUDING DEFAULTS
//     carries over column DEFAULT expressions.
//  2. source → _dedupe via (a) FILLTARGET, which populates rn from its
//     IDENTITY clause.
//  3. Into a DISTSTYLE ALL temp _losers, project the rn values that are NOT
//     MAX(rn) within their PK group — shape (b).
//  4. DELETE losers from _dedupe by rn — shape (c).
//  5. _dedupe → source via (a) IGNOREEXTRA, which discards rn. Source's OID
//     (and therefore its GRANTs / views / FKs) stays intact.
//  6. DROP the now-empty _dedupe.
//
// Why five phases? Per AWS, ALTER TABLE APPEND auto-commits and can't run in a
// BEGIN/END block, so steps 2 and 5 have to be issued bare. Steps 3-4 are
// grouped into one transaction so the temp and its DELETE succeed or fail
// together. See https://docs.aws.amazon.com/redshift/latest/dg/r_ALTER_TABLE_APPEND.html
//
// Failure modes & recovery:
//
//   - Fails in Prep (or at AppendIn before any blocks move): source intact,
//     _dedupe may exist but is empty. Operator must DROP _dedupe and retry.
//   - Fails in Dedupe: _dedupe holds all source data (with dupes); source is
//     empty. Operator re-runs Dedupe + AppendOut + Cleanup by hand.
//   - Fails at AppendOut: _dedupe holds deduped data; source empty. Operator
//     re-runs AppendOut + Cleanup by hand.
//   - Fails in Cleanup: source has deduped data; _dedupe is empty. Operator
//     drops _dedupe. No data at risk.
//
// We deliberately do NOT `DROP TABLE IF EXISTS` _dedupe in Prep: between
// AppendIn and AppendOut, _dedupe is the only copy of the data, so blind drops
// are dangerous. Fail-fast and surface the state for human inspection instead.
//
// Why MAX(rn) instead of ROW_NUMBER() ordered by __artie_updated_at? This
// only runs during initial snapshot dedupe; CDC streaming starts after
// snapshot completes, so duplicates are either double-emits of the same
// snapshot row or rows the CDC stream will overwrite. Which one wins doesn't
// matter — MAX(rn) is simpler.
func (rd RedshiftDialect) BuildDedupeQueriesAlterTableAppend(tableID, losersID sql.TableIdentifier, primaryKeys []string) RedshiftDedupePlan {
	primaryKeysEscaped := sql.QuoteIdentifiers(primaryKeys, rd)
	pkTuple := strings.Join(primaryKeysEscaped, ", ")
	rnCol := rd.QuoteIdentifier(DedupeStageRowIDColumn)

	dedupeNewID := tableID.WithTable(fmt.Sprintf("%s_%s_dedupe", tableID.Table(), constants.ArtiePrefix))

	// 1. _dedupe mirrors source plus a BIGINT IDENTITY tiebreaker. No
	//    `IF NOT EXISTS` — see function comment.
	createDedupeNew := fmt.Sprintf(
		"CREATE TABLE %s (LIKE %s INCLUDING DEFAULTS, %s BIGINT IDENTITY(1,1))",
		dedupeNewID.FullyQualifiedName(),
		tableID.FullyQualifiedName(),
		rnCol,
	)

	// 2. source → _dedupe. FILLTARGET fills the target-only rn column.
	//    MUST run outside BEGIN/END.
	appendIn := fmt.Sprintf(
		"ALTER TABLE %s APPEND FROM %s FILLTARGET",
		dedupeNewID.FullyQualifiedName(),
		tableID.FullyQualifiedName(),
	)

	// 3. Project the rn of every non-winner into _losers. DISTSTYLE ALL
	//    colocates it with _dedupe for the step-4 DELETE. rn is NOT NULL
	//    (IDENTITY), so NOT IN is null-safe.
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

	// 4. DELETE losers from _dedupe by rn.
	deleteLosers := fmt.Sprintf(
		"DELETE FROM %s USING %s l WHERE %s.%s = l.%s",
		dedupeNewID.FullyQualifiedName(),
		losersID.EscapedTable(),
		dedupeNewID.EscapedTable(),
		rnCol,
		rnCol,
	)

	// 5. _dedupe → source. IGNOREEXTRA drops rn so source's schema is
	//    unchanged by the round-trip. MUST run outside BEGIN/END.
	appendOut := fmt.Sprintf(
		"ALTER TABLE %s APPEND FROM %s IGNOREEXTRA",
		tableID.FullyQualifiedName(),
		dedupeNewID.FullyQualifiedName(),
	)

	// 6. _dedupe is empty after step 5, so DROP is near-instant.
	dropDedupe := fmt.Sprintf("DROP TABLE %s", dedupeNewID.FullyQualifiedName())

	return RedshiftDedupePlan{
		Prep:      []string{createDedupeNew},
		AppendIn:  appendIn,
		Dedupe:    []string{findLosers, deleteLosers},
		AppendOut: appendOut,
		Cleanup:   []string{dropDedupe},
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

func (RedshiftDialect) BuildMergeQueryIntoStagingTable(tableID sql.TableIdentifier, subQuery string, primaryKeys []columns.Column, additionalEqualityStrings []string, cols []columns.Column) ([]string, error) {
	return nil, fmt.Errorf("not implemented")
}
