package partition

import (
	"fmt"

	"github.com/artie-labs/transfer/lib/array"
)

var ValidPartitionTypes = []string{
	"time",
}

// TODO: We should be able to support different partition by fields in the future.
// https://cloud.google.com/bigquery/docs/partitioned-tables#partition_decorators
var ValidPartitionBy = []string{
	"daily",
}

type BigQuerySettings struct {
	PartitionType  string `yaml:"partitionType"`
	PartitionField string `yaml:"partitionField"`
	PartitionBy    string `yaml:"partitionBy"`
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

	if array.StringContains(ValidPartitionTypes, b.PartitionType) == false {
		return fmt.Errorf("partitionType must be one of: %v", ValidPartitionTypes)
	}

	if array.StringContains(ValidPartitionBy, b.PartitionBy) == false {
		return fmt.Errorf("partitionBy must be one of: %v", ValidPartitionBy)
	}

	return nil
}
