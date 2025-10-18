package shared

import (
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/kafkalib/partition"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

func BuildAdditionalEqualityStrings(dialect sql.Dialect, predicates []partition.MergePredicates) ([]string, error) {
	if len(predicates) == 0 {
		return []string{}, nil
	}

	var cols []columns.Column
	for _, predicate := range predicates {
		cols = append(cols, columns.NewColumn(predicate.PartitionField, typing.Invalid))
	}

	return sql.BuildColumnComparisons(cols, constants.TargetAlias, constants.StagingAlias, sql.Equal, dialect), nil
}
