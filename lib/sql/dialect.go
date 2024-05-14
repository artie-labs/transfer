package sql

import (
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

type TableIdentifier interface {
	EscapedTable() string
	Table() string
	WithTable(table string) TableIdentifier
	FullyQualifiedName() string
}

type Dialect interface {
	QuoteIdentifier(identifier string) string
	EscapeStruct(value string) string
	DataTypeForKind(kd typing.KindDetails, isPk bool) string
	KindForDataType(_type string, stringPrecision string) (typing.KindDetails, error)
	IsColumnAlreadyExistsErr(err error) bool
	IsTableDoesNotExistErr(err error) bool
	BuildCreateTableQuery(tableID TableIdentifier, temporary bool, colSQLParts []string) string
	BuildAlterColumnQuery(tableID TableIdentifier, columnOp constants.ColumnOperation, colSQLPart string) string
	BuildProcessToastColExpression(column columns.Column) string
	BuildDedupeQueries(tableID, stagingTableID TableIdentifier, primaryKeys []string, topicConfig kafkalib.TopicConfig) []string
	BuildMergeQueries(
		tableID TableIdentifier,
		subQuery string,
		idempotentKey string,
		primaryKeys []columns.Column,
		// additionalEqualityStrings is used for handling BigQuery & Snowflake partitioned table merges
		additionalEqualityStrings []string,
		cols []columns.Column,
		softDelete bool,
		// containsHardDeletes is only used for Redshift where we do not issue a DELETE statement if there are no hard deletes in the batch
		containsHardDeletes bool,
	) ([]string, error)
}
