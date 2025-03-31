package sql

import (
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

type DefaultValueStrategy int

const (
	// Backfill - use backfill strategy for default values
	Backfill DefaultValueStrategy = iota
	// Native - set default values directly into the destination
	Native
)

type TableIdentifier interface {
	EscapedTable() string
	Table() string
	WithTable(table string) TableIdentifier
	FullyQualifiedName() string
	WithDisableDropProtection(disableDropProtection bool) TableIdentifier
	AllowToDrop() bool
}

type Dialect interface {
	QuoteIdentifier(identifier string) string
	EscapeStruct(value string) string
	DataTypeForKind(kd typing.KindDetails, isPk bool, settings config.SharedDestinationColumnSettings) string
	KindForDataType(_type string, stringPrecision string) (typing.KindDetails, error)
	IsColumnAlreadyExistsErr(err error) bool
	IsTableDoesNotExistErr(err error) bool
	BuildCreateTableQuery(tableID TableIdentifier, temporary bool, colSQLParts []string) string
	BuildDropTableQuery(tableID TableIdentifier) string
	BuildTruncateTableQuery(tableID TableIdentifier) string
	BuildDedupeQueries(tableID, stagingTableID TableIdentifier, primaryKeys []string, includeArtieUpdatedAt bool) []string
	BuildDedupeTableQuery(tableID TableIdentifier, primaryKeys []string) string
	BuildDescribeTableQuery(tableID TableIdentifier) (string, []any, error)
	BuildIsNotToastValueExpression(tableAlias constants.TableAlias, column columns.Column) string
	BuildMergeQueries(
		tableID TableIdentifier,
		subQuery string,
		primaryKeys []columns.Column,
		// additionalEqualityStrings is used for handling BigQuery & Snowflake partitioned table merges
		additionalEqualityStrings []string,
		cols []columns.Column,
		softDelete bool,
		// containsHardDeletes is only used for Redshift where we do not issue a DELETE statement if there are no hard deletes in the batch
		containsHardDeletes bool,
	) ([]string, error)

	// DDL queries
	BuildAddColumnQuery(tableID TableIdentifier, sqlPart string) string
	BuildDropColumnQuery(tableID TableIdentifier, colName string) string

	// Default values
	GetDefaultValueStrategy() DefaultValueStrategy
}
