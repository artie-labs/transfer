package partition

import (
	"fmt"
	"slices"
)

var ValidPartitionTypes = []string{
	"time",
}

type ByGranularity string

const (
	Daily   ByGranularity = "daily"
	Monthly ByGranularity = "monthly"
	Yearly  ByGranularity = "yearly"
)

func (b ByGranularity) Part() (string, error) {
	// https://cloud.google.com/bigquery/docs/reference/standard-sql/timestamp_functions#extract
	switch b {
	case Daily:
		return "DATE", nil
	case Monthly:
		return "MONTH", nil
	case Yearly:
		return "YEAR", nil
	default:
		return "", fmt.Errorf("unexpected partition by: %q", b)
	}
}

var ValidPartitionBy = []ByGranularity{
	Daily,
	Monthly,
	Yearly,
}

// We need the JSON annotations here for our dashboard to import the settings correctly.

type MergePredicates struct {
	PartitionField string `yaml:"partitionField" json:"partitionField"`
}

type BigQuerySettings struct {
	PartitionType  string        `yaml:"partitionType" json:"partitionType"`
	PartitionField string        `yaml:"partitionField" json:"partitionField"`
	PartitionBy    ByGranularity `yaml:"partitionBy" json:"partitionBy"`
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
