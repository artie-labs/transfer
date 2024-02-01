package kafkalib

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetUniqueDatabaseAndSchema(t *testing.T) {
	type _testCase struct {
		name          string
		tcs           []*TopicConfig
		expectedPairs []DatabaseSchemaPair
	}

	testCases := []_testCase{
		{
			name: "happy path",
			tcs: []*TopicConfig{
				{
					Database: "db",
					Schema:   "schema",
				},
			},
			expectedPairs: []DatabaseSchemaPair{
				{
					Database: "db",
					Schema:   "schema",
				},
			},
		},
		{
			name: "1 database and 2 schemas",
			tcs: []*TopicConfig{
				{
					Database: "db",
					Schema:   "schema_uno",
				},
				{
					Database: "db",
					Schema:   "schema_deux",
				},
			},
			expectedPairs: []DatabaseSchemaPair{
				{
					Database: "db",
					Schema:   "schema_uno",
				},
				{
					Database: "db",
					Schema:   "schema_deux",
				},
			},
		},
		{
			name: "multiple topic configs with same db",
			tcs: []*TopicConfig{
				{
					Database:  "db",
					Schema:    "schema",
					TableName: "foo",
				},
				{
					Database:  "db",
					Schema:    "schema",
					TableName: "bar",
				},
				{
					Database:  "db",
					Schema:    "schema",
					TableName: "dusty",
				},
			},
			expectedPairs: []DatabaseSchemaPair{
				{
					Database: "db",
					Schema:   "schema",
				},
			},
		},
	}

	for _, testCase := range testCases {
		actualPairs := GetUniqueDatabaseAndSchema(testCase.tcs)
		assert.Equal(t, len(testCase.expectedPairs), len(actualPairs), testCase.name)
		for _, actualPair := range actualPairs {
			var found bool
			for _, expectedPair := range testCase.expectedPairs {
				if found = actualPair == expectedPair; found {
					break
				}
			}
			assert.True(t, found, fmt.Sprintf("missingPair=%s, testName=%s", actualPair, testCase.name))
		}
	}
}

func TestTopicConfig_String(t *testing.T) {
	tc := TopicConfig{
		Database:      "aaa",
		TableName:     "bbb",
		Schema:        "ccc",
		Topic:         "d",
		IdempotentKey: "e",
		CDCFormat:     "f",
	}

	assert.True(t, strings.Contains(tc.String(), fmt.Sprintf("tableNameOverride=%s", tc.TableName)), tc.String())
	assert.True(t, strings.Contains(tc.String(), fmt.Sprintf("db=%s", tc.Database)), tc.String())
	assert.True(t, strings.Contains(tc.String(), fmt.Sprintf("schema=%s", tc.Schema)), tc.String())
	assert.True(t, strings.Contains(tc.String(), fmt.Sprintf("topic=%s", tc.Topic)), tc.String())
	assert.True(t, strings.Contains(tc.String(), fmt.Sprintf("idempotentKey=%s", tc.IdempotentKey)), tc.String())
	assert.True(t, strings.Contains(tc.String(), fmt.Sprintf("cdcFormat=%s", tc.CDCFormat)), tc.String())
}

func TestTopicConfig_Validate(t *testing.T) {
	var tc TopicConfig
	assert.ErrorContains(t, tc.Validate(), "database, schema, topic or cdc format is empty", tc.String())

	tc = TopicConfig{
		Database:     "12",
		TableName:    "34",
		Schema:       "56",
		Topic:        "78",
		CDCFormat:    "aa",
		CDCKeyFormat: JSONKeyFmt,
	}

	assert.ErrorContains(t, tc.Validate(), "opsToSkipMap is nil, call Load() first")

	tc.Load()
	assert.NoError(t, tc.Validate(), tc.String())

	tc.CDCKeyFormat = "non_existent"
	assert.ErrorContains(t, tc.Validate(), "invalid cdc key format: non_existent", tc.String())

	for _, validKeyFormat := range validKeyFormats {
		tc.CDCKeyFormat = validKeyFormat
		assert.NoError(t, tc.Validate(), tc.String())
	}
}

func TestTopicConfig_Load_ShouldSkip(t *testing.T) {
	{
		// Test backwards compat
		tc := TopicConfig{
			SkipDelete: true,
		}

		tc.Load()
		assert.True(t, tc.ShouldSkip("d"), tc.String())
		for _, op := range []string{"c", "r", "u"} {
			assert.False(t, tc.ShouldSkip(op), tc.String())
		}
	}
	{
		tc := TopicConfig{
			SkippedOperations: "c, r, u",
		}
		tc.Load()
		for _, op := range []string{"c", "r", "u"} {
			assert.True(t, tc.ShouldSkip(op), tc.String())
		}
		assert.False(t, tc.ShouldSkip("d"), tc.String())
	}
	{
		tc := TopicConfig{
			SkippedOperations: "c",
		}
		tc.Load()
		assert.True(t, tc.ShouldSkip("c"), tc.String())
	}
}
