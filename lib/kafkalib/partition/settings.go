package partition

import (
	"fmt"
	"slices"

	"github.com/artie-labs/transfer/lib/array"
	"github.com/artie-labs/transfer/lib/config/constants"
)

var ValidPartitionTypes = []string{
	"time",
}

// TODO: We should be able to support different partition by fields in the future.
// https://cloud.google.com/bigquery/docs/partitioned-tables#partition_decorators
var ValidPartitionBy = []string{
	"daily",
}

// We need the JSON annotations here for our dashboard to import the settings correctly.

type MergePredicates struct {
	PartitionField string `yaml:"partitionField" json:"partitionField"`
}

type BigQuerySettings struct {
	PartitionType  string `yaml:"partitionType" json:"partitionType"`
	PartitionField string `yaml:"partitionField" json:"partitionField"`
	PartitionBy    string `yaml:"partitionBy" json:"partitionBy"`
}

// GenerateMergeString this is used as an equality string for the MERGE statement.
func (b *BigQuerySettings) GenerateMergeString(values []string) (string, error) {
	if err := b.Valid(); err != nil {
		return "", fmt.Errorf("failed to validate bigQuerySettings: %w", err)
	}

	if len(values) == 0 {
		return "", fmt.Errorf("values cannot be empty")
	}

	switch b.PartitionType {
	case "time":
		switch b.PartitionBy {
		case "daily":
			return fmt.Sprintf(`DATE(%s.%s) IN (%s)`, constants.TargetAlias, b.PartitionField, array.StringsJoinAddSingleQuotes(values)), nil
		}
	}

	return "", fmt.Errorf("unexpected partitionType: %s and/or partitionBy: %s", b.PartitionType, b.PartitionBy)
}

func (b *BigQuerySettings) Valid() error {
	if b == nil {
		return fmt.Errorf("bigQuerySettings is nil")
	}

	if b.PartitionType == "" {
		return fmt.Errorf("partitionTypes cannot be empty")
	}

	if b.PartitionField == "" {
		return fmt.Errorf("partitionField cannot be empty")
	}

	if b.PartitionBy == "" {
		return fmt.Errorf("partitionBy cannot be empty")
	}

	if !slices.Contains(ValidPartitionTypes, b.PartitionType) {
		return fmt.Errorf("partitionType must be one of: %v", ValidPartitionTypes)
	}

	if !slices.Contains(ValidPartitionBy, b.PartitionBy) {
		return fmt.Errorf("partitionBy must be one of: %v", ValidPartitionBy)
	}

	return nil
}
