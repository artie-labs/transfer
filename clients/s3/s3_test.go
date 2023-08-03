package s3

import (
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/config"

	"github.com/artie-labs/transfer/lib/kafkalib"
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

	testCases := []_testCase{
		{
			name: "valid #1 (no prefix)",
			tableData: &optimization.TableData{
				TopicConfig: kafkalib.TopicConfig{
					Database:  "db",
					TableName: "table",
					Schema:    "public",
				},
				LatestCDCTs: time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
			},
			expectedFormat: "db.public.table/2020-01-01",
			expectError:    true,
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
