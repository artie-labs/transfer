package kafkalib

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetUniqueDatabaseAndSchemaPairs(t *testing.T) {
	{
		// No topic configs
		assert.Empty(t, GetUniqueDatabaseAndSchemaPairs(nil))
	}
	{
		// 1 topic config
		tcs := []*TopicConfig{
			{
				Database: "db",
				Schema:   "schema",
			},
		}

		actual := GetUniqueDatabaseAndSchemaPairs(tcs)
		assert.Len(t, actual, 1)
		assert.Equal(t, tcs[0].BuildDatabaseAndSchemaPair(), actual[0])
	}
	{
		// 2 topic configs (both the same)
		tcs := []*TopicConfig{
			{
				Database: "db",
				Schema:   "schema",
			},
			{
				Database: "db",
				Schema:   "schema",
			},
		}

		actual := GetUniqueDatabaseAndSchemaPairs(tcs)
		assert.Len(t, actual, 1)
		assert.Equal(t, tcs[0].BuildDatabaseAndSchemaPair(), actual[0])
	}
	{
		// 3 topic configs (2 the same)
		tcs := []*TopicConfig{
			{
				Database: "db",
				Schema:   "schema",
			},
			{
				Database: "db",
				Schema:   "schema",
			},
			{
				Database: "db",
				Schema:   "schema2",
			},
		}

		actual := GetUniqueDatabaseAndSchemaPairs(tcs)
		assert.Len(t, actual, 2)
		assert.ElementsMatch(t, []DatabaseAndSchemaPair{
			tcs[0].BuildDatabaseAndSchemaPair(),
			tcs[2].BuildDatabaseAndSchemaPair(),
		}, actual)
	}
}

func TestTopicConfig_String(t *testing.T) {
	tc := TopicConfig{
		Database:          "aaa",
		TableName:         "bbb",
		Schema:            "ccc",
		Topic:             "d",
		CDCFormat:         "f",
		SkippedOperations: "d",
	}

	assert.Contains(t, tc.String(), fmt.Sprintf("tableNameOverride=%s", tc.TableName), tc.String())
	assert.Contains(t, tc.String(), fmt.Sprintf("db=%s", tc.Database), tc.String())
	assert.Contains(t, tc.String(), fmt.Sprintf("schema=%s", tc.Schema), tc.String())
	assert.Contains(t, tc.String(), fmt.Sprintf("topic=%s", tc.Topic), tc.String())
	assert.Contains(t, tc.String(), fmt.Sprintf("cdcFormat=%s", tc.CDCFormat), tc.String())
	assert.Contains(t, tc.String(), fmt.Sprintf("skippedOperations=%s", tc.SkippedOperations), tc.String())
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

	tc.ColumnsToInclude = []string{"col1", "col2"}
	tc.ColumnsToExclude = []string{"col3"}
	assert.ErrorContains(t, tc.Validate(), "cannot specify both columnsToInclude and columnsToExclude", tc.String())

	tc.ColumnsToInclude = []string{}
	assert.NoError(t, tc.Validate(), tc.String())
}

func TestTopicConfig_Load_ShouldSkip(t *testing.T) {
	{
		tc := TopicConfig{SkippedOperations: "c, r, u"}
		tc.Load()
		for _, op := range []string{"c", "r", "u"} {
			assert.True(t, tc.ShouldSkip(op), tc.String())
		}
		assert.False(t, tc.ShouldSkip("d"), tc.String())
	}
	{
		tc := TopicConfig{SkippedOperations: "c"}
		tc.Load()
		assert.True(t, tc.ShouldSkip("c"), tc.String())
	}
	{
		tc := TopicConfig{SkippedOperations: "d"}
		tc.Load()
		assert.True(t, tc.ShouldSkip("d"), tc.String())
	}
}

func TestMultiStepMergeSettings_Validate(t *testing.T) {
	{
		// Not enabled
		assert.NoError(t, MultiStepMergeSettings{}.Validate())
	}
	{
		// Enable, but flush count is not set
		assert.ErrorContains(t, MultiStepMergeSettings{Enabled: true}.Validate(), "flush count must be greater than 0")
	}
	{
		// Valid
		assert.NoError(t, MultiStepMergeSettings{
			Enabled:    true,
			FlushCount: 1,
		}.Validate())
	}
}

func TestSoftPartitioning_Validate(t *testing.T) {
	tests := []struct {
		name    string
		sp      SoftPartitioning
		wantErr bool
		errMsg  string
	}{
		{
			name:    "disabled partitioning is always valid",
			sp:      SoftPartitioning{Enabled: false},
			wantErr: false,
		},
		{
			name:    "enabled but missing partition frequency",
			sp:      SoftPartitioning{Enabled: true, PartitionFrequency: "", PartitionColumn: "col"},
			wantErr: true,
			errMsg:  "partition frequency is required",
		},
		{
			name:    "enabled but invalid partition frequency",
			sp:      SoftPartitioning{Enabled: true, PartitionFrequency: "invalid", PartitionColumn: "col"},
			wantErr: true,
			errMsg:  "invalid partition frequency",
		},
		{
			name:    "enabled but missing partition column",
			sp:      SoftPartitioning{Enabled: true, PartitionFrequency: Daily, PartitionColumn: ""},
			wantErr: true,
			errMsg:  "partition column is required",
		},
		{
			name: "enabled and valid (daily)",
			sp: SoftPartitioning{
				Enabled:            true,
				PartitionFrequency: Daily,
				PartitionColumn:    "col",
			},
			wantErr: false,
		},
		{
			name: "enabled and valid (monthly)",
			sp: SoftPartitioning{
				Enabled:            true,
				PartitionFrequency: Monthly,
				PartitionColumn:    "col",
			},
			wantErr: false,
		},
		{
			name: "enabled and valid (hourly)",
			sp: SoftPartitioning{
				Enabled:            true,
				PartitionFrequency: Hourly,
				PartitionColumn:    "col",
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.sp.Validate()
			if tt.wantErr {
				assert.Error(t, err, "expected error but got nil")
				if tt.errMsg != "" {
					assert.ErrorContains(t, err, tt.errMsg)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
