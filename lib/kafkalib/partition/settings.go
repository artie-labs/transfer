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

// GenerateMergeString this is used as an equality string for the MERGE statement.
func (b *BigQuerySettings) GenerateMergeString() (string, error) {
	if err := b.Valid(); err != nil {
		return "", fmt.Errorf("failed to validate bigQuerySettings, err: %v", err)
	}

	switch b.PartitionType {
	case "time":
		switch b.PartitionBy {
		case "daily":
			return fmt.Sprintf(`DATE(c.%s) = DATE(cc.%s)`, b.PartitionField, b.PartitionField), nil
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

	if array.StringContains(ValidPartitionTypes, b.PartitionType) == false {
		return fmt.Errorf("partitionType must be one of: %v", ValidPartitionTypes)
	}

	if array.StringContains(ValidPartitionBy, b.PartitionBy) == false {
		return fmt.Errorf("partitionBy must be one of: %v", ValidPartitionBy)
	}

	return nil
}
