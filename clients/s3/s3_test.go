package s3

import (
	"testing"
	"time"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/config"

	"github.com/artie-labs/transfer/lib/optimization"
)

func TestObjectPrefix(t *testing.T) {
	td := optimization.NewTableData(nil, config.Replication, nil, kafkalib.TopicConfig{
		Database:  "db",
		TableName: "table",
		Schema:    "public",
	}, "table")

	td.LatestCDCTs = time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	testCases := []struct {
		name      string
		tableData *optimization.TableData
		config    *config.S3Settings

		expectedErr    string
		expectedFormat string
	}{
		{
			name:        "nil",
			expectedErr: "failed to validate settings: s3 settings are nil",
		},
		{
			name:      "valid #1 (no folder)",
			tableData: td,
			config: &config.S3Settings{
				Bucket:             "bucket",
				AwsSecretAccessKey: "foo",
				AwsAccessKeyID:     "bar",
				OutputFormat:       constants.ParquetFormat,
			},
			expectedFormat: "db.public.table/2020-01-01",
		},
		{
			name:      "valid #2 w/ folder",
			tableData: td,
			config: &config.S3Settings{
				Bucket:             "bucket",
				AwsSecretAccessKey: "foo",
				AwsAccessKeyID:     "bar",
				OutputFormat:       constants.ParquetFormat,
				FolderName:         "foo",
			},
			expectedFormat: "foo/db.public.table/2020-01-01",
		},
	}

	for _, tc := range testCases {
		store, err := LoadStore(config.Config{S3: tc.config})
		if tc.expectedErr != "" {
			assert.ErrorContains(t, err, tc.expectedErr, tc.name)
		} else {
			assert.NoError(t, err, tc.name)
			actualObjectPrefix := store.ObjectPrefix(tc.tableData)
			assert.Equal(t, tc.expectedFormat, actualObjectPrefix, tc.name)
		}
	}
}
