package partition

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBigQuerySettings_Valid(t *testing.T) {
	type _testCase struct {
		name             string
		bigQuerySettings *BigQuerySettings
		expectError      bool
	}

	testCases := []_testCase{
		{
			name:        "nil",
			expectError: true,
		},
		{
			name:             "empty partitionType",
			bigQuerySettings: &BigQuerySettings{},
			expectError:      true,
		},
		{
			name: "empty partitionField",
			bigQuerySettings: &BigQuerySettings{
				PartitionType: "time",
			},
			expectError: true,
		},
		{
			name: "empty partitionBy",
			bigQuerySettings: &BigQuerySettings{
				PartitionType:  "time",
				PartitionField: "created_at",
			},
			expectError: true,
		},
		{
			name: "invalid partitionType",
			bigQuerySettings: &BigQuerySettings{
				PartitionType:  "invalid",
				PartitionField: "created_at",
				PartitionBy:    "daily",
			},
			expectError: true,
		},
		{
			name: "invalid partitionBy",
			bigQuerySettings: &BigQuerySettings{
				PartitionType:  "time",
				PartitionField: "created_at",
				PartitionBy:    "invalid",
			},
			expectError: true,
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
		if testCase.expectError {
			assert.Error(t, actualErr, testCase.name)
		} else {
			assert.NoError(t, actualErr, testCase.name)
		}
	}
}

func TestBigQuerySettings_GenerateMergeString(t *testing.T) {
	type _testCase struct {
		name           string
		values         []string
		expectError    bool
		expectedString string
	}

	testCases := []_testCase{
		{
			name:        "nil",
			expectError: true,
		},
		{
			name:        "empty values",
			values:      []string{},
			expectError: true,
		},
		{
			name:           "valid",
			values:         []string{"2020-01-01"},
			expectedString: "DATE(c.created_at) IN ('2020-01-01')",
		},
		{
			name:           "valid multiple values",
			values:         []string{"2020-01-01", "2020-01-02"},
			expectedString: `DATE(c.created_at) IN ('2020-01-01','2020-01-02')`,
		},
	}

	for _, testCase := range testCases {
		bigquery := &BigQuerySettings{
			PartitionType:  "time",
			PartitionField: "created_at",
			PartitionBy:    "daily",
		}

		actualValue, actualErr := bigquery.GenerateMergeString(testCase.values)
		if testCase.expectError {
			assert.Error(t, actualErr, testCase.name)
		} else {
			assert.NoError(t, actualErr, testCase.name)
			assert.Equal(t, testCase.expectedString, actualValue, testCase.name)
		}
	}
}
