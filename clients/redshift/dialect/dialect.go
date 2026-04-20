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

// BuildDedupeBoundaryQuery returns a single-row query whose columns are the
// chunk boundaries for the leading primary key: MIN, approximate percentiles at
// 1/numChunks, 2/numChunks, ..., (numChunks-1)/numChunks, and MAX. The resulting
// numChunks+1 values define numChunks half-open ranges (the last range is
// inclusive on the upper bound) used by BuildDedupeStagePopulateRangeQuery.
//
// APPROXIMATE PERCENTILE_DISC uses sketches, so this runs in a single pass over
// the table without a global sort.
func (rd RedshiftDialect) BuildDedupeBoundaryQuery(tableID sql.TableIdentifier, boundaryKey string, numChunks int) string {
	pk := rd.QuoteIdentifier(boundaryKey)

	selectParts := []string{fmt.Sprintf("MIN(%s)", pk)}
	for i := 1; i < numChunks; i++ {
		pct := float64(i) / float64(numChunks)
		selectParts = append(selectParts, fmt.Sprintf("APPROXIMATE PERCENTILE_DISC(%g) WITHIN GROUP (ORDER BY %s)", pct, pk))
	}
	selectParts = append(selectParts, fmt.Sprintf("MAX(%s)", pk))

	return fmt.Sprintf("SELECT %s FROM %s", strings.Join(selectParts, ", "), tableID.FullyQualifiedName())
}

// DedupeStageRowIDColumn is the IDENTITY column we append to the dedupe
// stage table. Using its value as the per-partition tiebreaker lets us run
// the winner-selection QUALIFY over only PK columns, __artie_updated_at, and
// this BIGINT - never through wide/SUPER columns.
//
// Why this matters: Redshift implicitly converts SUPER and wide VARCHAR
// values to VARCHAR(65535) when routing them through window/PartiQL
// operators. Any row whose serialized value exceeds that limit dies with
// "Invalid input (8001)". The original SELECT * ... QUALIFY path hit this
// whenever a table had a SUPER column > 64KB; the two-step stage + winners
// shape avoids it entirely.
const DedupeStageRowIDColumn = "_artie_dedupe_rn"

// BuildDedupeStageCreateQuery returns a CREATE TABLE mirroring the
// source schema plus an IDENTITY tiebreaker column.
func (rd RedshiftDialect) BuildDedupeStageCreateQuery(stageID, sourceID sql.TableIdentifier) string {
	return fmt.Sprintf(
		"CREATE TABLE %s (LIKE %s, %s BIGINT IDENTITY(1,1))",
		stageID.EscapedTable(),
		sourceID.FullyQualifiedName(),
		rd.QuoteIdentifier(DedupeStageRowIDColumn),
	)
}

// BuildDedupeStageDropQuery returns a DROP TABLE IF EXISTS for the stage.
func (RedshiftDialect) BuildDedupeStageDropQuery(stageID sql.TableIdentifier) string {
	return fmt.Sprintf("DROP TABLE IF EXISTS %s", stageID.EscapedTable())
}

// BuildDedupeStageTruncateQuery returns a TRUNCATE for the stage. Used
// between chunks to recycle a single stage table instead of dropping and
// recreating it.
func (RedshiftDialect) BuildDedupeStageTruncateQuery(stageID sql.TableIdentifier) string {
	return fmt.Sprintf("TRUNCATE TABLE %s", stageID.EscapedTable())
}

// BuildDedupeStagePopulateRangeQuery copies rows bounded by placeholders $1
// and $2 on boundaryKey from sourceID into stageID. columns must list every
// source column in ordinal order; the IDENTITY column is auto-populated.
// Upper bound is exclusive unless inclusiveUpper is true (used for the final
// chunk so the MAX value is included).
//
// Plain INSERT ... SELECT (no window) means SUPER values up to their natural
// 1MB cap flow through directly, without the 64KB PartiQL serialization cap.
func (rd RedshiftDialect) BuildDedupeStagePopulateRangeQuery(stageID, sourceID sql.TableIdentifier, columns []string, boundaryKey string, inclusiveUpper bool) string {
	colList := strings.Join(sql.QuoteIdentifiers(columns, rd), ", ")
	boundaryCol := rd.QuoteIdentifier(boundaryKey)
	upperOp := "<"
	if inclusiveUpper {
		upperOp = "<="
	}

	return fmt.Sprintf(
		"INSERT INTO %s (%s) SELECT %s FROM %s WHERE %s >= $1 AND %s %s $2",
		stageID.EscapedTable(),
		colList,
		colList,
		sourceID.FullyQualifiedName(),
		boundaryCol, boundaryCol, upperOp,
	)
}

// BuildDedupeStagePopulateNullQuery copies rows where boundaryKey IS NULL
// into the stage table. Range-chunk queries only catch non-NULL boundary
// values, so these rows would otherwise be silently dropped on swap.
func (rd RedshiftDialect) BuildDedupeStagePopulateNullQuery(stageID, sourceID sql.TableIdentifier, columns []string, boundaryKey string) string {
	colList := strings.Join(sql.QuoteIdentifiers(columns, rd), ", ")

	return fmt.Sprintf(
		"INSERT INTO %s (%s) SELECT %s FROM %s WHERE %s IS NULL",
		stageID.EscapedTable(),
		colList,
		colList,
		sourceID.FullyQualifiedName(),
		rd.QuoteIdentifier(boundaryKey),
	)
}

// BuildDedupeStageWinnersInsertQuery inserts one row per primary key from
// the stage table into targetID. Winner per PK is the row with the greatest
// __artie_updated_at (when includeArtieUpdatedAt) breaking ties by lowest
// IDENTITY value; otherwise simply the lowest IDENTITY value.
//
// The inner QUALIFY references only primary key columns, __artie_updated_at,
// and the IDENTITY column, so wide VARCHAR and SUPER columns are never
// routed through a window operator. They ride along only in the outer
// SELECT, which is a plain projection.
func (rd RedshiftDialect) BuildDedupeStageWinnersInsertQuery(targetID, stageID sql.TableIdentifier, columns []string, primaryKeys []string, includeArtieUpdatedAt bool) string {
	colList := strings.Join(sql.QuoteIdentifiers(columns, rd), ", ")
	pks := strings.Join(sql.QuoteIdentifiers(primaryKeys, rd), ", ")
	rnCol := rd.QuoteIdentifier(DedupeStageRowIDColumn)

	var orderCols []string
	if includeArtieUpdatedAt {
		orderCols = append(orderCols, fmt.Sprintf("%s DESC", rd.QuoteIdentifier(constants.UpdateColumnMarker)))
	}
	orderCols = append(orderCols, fmt.Sprintf("%s ASC", rnCol))

	return fmt.Sprintf(
		"INSERT INTO %s (%s) SELECT %s FROM %s WHERE %s IN (SELECT %s FROM %s QUALIFY ROW_NUMBER() OVER (PARTITION BY %s ORDER BY %s) = 1)",
		targetID.FullyQualifiedName(),
		colList,
		colList,
		stageID.EscapedTable(),
		rnCol,
		rnCol,
		stageID.EscapedTable(),
		pks,
		strings.Join(orderCols, ", "),
	)
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
