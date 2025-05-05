package s3

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/optimization"
)

func TestBuildTemporaryFilePath(t *testing.T) {
	ts := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	fp := buildTemporaryFilePath(&optimization.TableData{LatestCDCTs: ts})
	assert.True(t, strings.HasPrefix(fp, "/tmp/1577836800000_"), fp)
	assert.True(t, strings.HasSuffix(fp, ".parquet"), fp)
}

func TestObjectPrefix(t *testing.T) {
	td := optimization.NewTableData(nil, config.Replication, nil, kafkalib.TopicConfig{
		Database:  "db",
		TableName: "table",
		Schema:    "public",
	}, "table")

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
			expectedFormat: fmt.Sprintf("db.public.table/date=%s", time.Now().Format(time.DateOnly)),
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
			expectedFormat: fmt.Sprintf("foo/db.public.table/date=%s", time.Now().Format(time.DateOnly)),
		},
	}

	for _, tc := range testCases {
		store, err := LoadStore(t.Context(), config.Config{S3: tc.config})
		if tc.expectedErr != "" {
			assert.ErrorContains(t, err, tc.expectedErr, tc.name)
		} else {
			assert.NoError(t, err, tc.name)
			actualObjectPrefix := store.ObjectPrefix(tc.tableData)
			assert.Equal(t, tc.expectedFormat, actualObjectPrefix, tc.name)
		}
	}
}
