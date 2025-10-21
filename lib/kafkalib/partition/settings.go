package partition

import (
	"fmt"
	"slices"
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
	// TODO: - Flip to start using this.
	PartitionBy   string `yaml:"partitionBy" json:"partitionBy"`
	PartitionType string `yaml:"partitionType" json:"partitionType"`
}

type BigQuerySettings struct {
	PartitionType  string `yaml:"partitionType" json:"partitionType"`
	PartitionField string `yaml:"partitionField" json:"partitionField"`
	PartitionBy    string `yaml:"partitionBy" json:"partitionBy"`
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
