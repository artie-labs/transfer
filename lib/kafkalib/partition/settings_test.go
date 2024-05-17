package partition

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBigQuerySettings_Valid(t *testing.T) {
	testCases := []struct {
		name             string
		bigQuerySettings *BigQuerySettings
		expectedErr      string
	}{
		{
			name:        "nil",
			expectedErr: "bigQuerySettings is nil",
		},
		{
			name:             "empty partitionType",
			bigQuerySettings: &BigQuerySettings{},
			expectedErr:      "partitionTypes cannot be empty",
		},
		{
			name: "empty partitionField",
			bigQuerySettings: &BigQuerySettings{
				PartitionType: "time",
			},
			expectedErr: "partitionField cannot be empty",
		},
		{
			name: "empty partitionBy",
			bigQuerySettings: &BigQuerySettings{
				PartitionType:  "time",
				PartitionField: "created_at",
			},
			expectedErr: "partitionBy cannot be empty",
		},
		{
			name: "invalid partitionType",
			bigQuerySettings: &BigQuerySettings{
				PartitionType:  "invalid",
				PartitionField: "created_at",
				PartitionBy:    "daily",
			},
			expectedErr: "partitionType must be one of:",
		},
		{
			name: "invalid partitionBy",
			bigQuerySettings: &BigQuerySettings{
				PartitionType:  "time",
				PartitionField: "created_at",
				PartitionBy:    "invalid",
			},
			expectedErr: "partitionBy must be one of:",
		},
		{
			name: "valid",
			bigQuerySettings: &BigQuerySettings{
				PartitionType:  "time",
				PartitionField: "created_at",
				PartitionBy:    "daily",
			},
		},
	}

	for _, testCase := range testCases {
		actualErr := testCase.bigQuerySettings.Valid()
		if testCase.expectedErr != "" {
			assert.ErrorContains(t, actualErr, testCase.expectedErr, testCase.name)
		} else {
			assert.NoError(t, actualErr, testCase.name)
		}
	}
}
