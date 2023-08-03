package s3

import (
	"time"

	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/config"

	"github.com/artie-labs/transfer/lib/optimization"
)

func (s *S3TestSuite) TestObjectPrefix() {
	type _testCase struct {
		name      string
		tableData *optimization.TableData
		config    *config.S3Settings

		expectError    bool
		expectedFormat string
	}

	td := optimization.NewTableData(nil, nil, kafkalib.TopicConfig{
		Database:  "db",
		TableName: "table",
		Schema:    "public",
	}, "")

	td.LatestCDCTs = time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	testCases := []_testCase{
		{
			name:        "nil",
			expectError: true,
		},
		{
			name:      "valid #1 (no prefix)",
			tableData: td,
			config: &config.S3Settings{
				Bucket:            "bucket",
				CredentialsClause: "credentials",
				OutputFormat:      constants.ParquetFormat,
			},
			expectedFormat: "db.public.table/2020-01-01",
		},
		{
			name:      "valid #2 w/ prefix",
			tableData: td,
			config: &config.S3Settings{
				Bucket:            "bucket",
				CredentialsClause: "credentials",
				OutputFormat:      constants.ParquetFormat,
				OptionalPrefix:    "foo",
			},
			expectedFormat: "foo/db.public.table/2020-01-01",
		},
	}

	for _, tc := range testCases {
		store, err := LoadStore(s.ctx, tc.config)
		if tc.expectError {
			assert.Error(s.T(), err, tc.name)
		} else {
			assert.NoError(s.T(), err, tc.name)
			actualObjectPrefix := store.ObjectPrefix(s.ctx, tc.tableData)
			assert.Equal(s.T(), tc.expectedFormat, actualObjectPrefix, tc.name)
		}
	}
}
