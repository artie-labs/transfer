package partition

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBigQuerySettings_Valid(t *testing.T) {
	{
		// Nil
		var settings *BigQuerySettings
		assert.ErrorContains(t, settings.Valid(), "bigQuerySettings is nil")
	}
	{
		// Empty partition type
		settings := &BigQuerySettings{}
		assert.ErrorContains(t, settings.Valid(), "partitionTypes cannot be empty")
	}
	{
		// Empty partition field
		settings := &BigQuerySettings{PartitionType: "time"}
		assert.ErrorContains(t, settings.Valid(), "partitionField cannot be empty")
	}
	{
		// Empty partition by
		settings := &BigQuerySettings{PartitionType: "time", PartitionField: "created_at"}
		assert.ErrorContains(t, settings.Valid(), "partitionBy cannot be empty")
	}
	{
		// Invalid partition type
		settings := &BigQuerySettings{PartitionType: "invalid", PartitionField: "created_at", PartitionBy: "daily"}
		assert.ErrorContains(t, settings.Valid(), "partitionType must be one of:")
	}
	{
		// Invalid partition by
		settings := &BigQuerySettings{PartitionType: "time", PartitionField: "created_at", PartitionBy: "invalid"}
		assert.ErrorContains(t, settings.Valid(), "partitionBy must be one of:")
	}
	{
		// Valid
		settings := &BigQuerySettings{PartitionType: "time", PartitionField: "created_at", PartitionBy: "daily"}
		assert.NoError(t, settings.Valid())
	}
}
