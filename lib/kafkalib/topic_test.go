package kafkalib

import (
	"fmt"
	"testing"
	"time"

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
				MaxPartitions:      10,
			},
			wantErr: false,
		},
		{
			name: "enabled and valid (monthly)",
			sp: SoftPartitioning{
				Enabled:            true,
				PartitionFrequency: Monthly,
				PartitionColumn:    "col",
				MaxPartitions:      10,
			},
			wantErr: false,
		},
		{
			name: "enabled and valid (hourly)",
			sp: SoftPartitioning{
				Enabled:            true,
				PartitionFrequency: Hourly,
				PartitionColumn:    "col",
				MaxPartitions:      10,
			},
			wantErr: false,
		},
		{
			name: "enabled but maxPartitions is 0",
			sp: SoftPartitioning{
				Enabled:            true,
				PartitionFrequency: Daily,
				PartitionColumn:    "col",
				MaxPartitions:      0,
			},
			wantErr: true,
			errMsg:  "maxPartitions must be greater than 0",
		},
		{
			name: "enabled but maxPartitions is negative",
			sp: SoftPartitioning{
				Enabled:            true,
				PartitionFrequency: Daily,
				PartitionColumn:    "col",
				MaxPartitions:      -1,
			},
			wantErr: true,
			errMsg:  "maxPartitions must be greater than 0",
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

func TestPartitionFrequency_PartitionDistance(t *testing.T) {
	// Test data: January 1, 2024 12:00:00
	baseTime := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)

	t.Run("Monthly", func(t *testing.T) {
		tests := []struct {
			name     string
			from     time.Time
			now      time.Time
			expected int
		}{
			{
				name:     "same month",
				from:     baseTime,
				now:      baseTime,
				expected: 0,
			},
			{
				name:     "one month later",
				from:     baseTime,
				now:      time.Date(2024, 2, 1, 12, 0, 0, 0, time.UTC),
				expected: 1,
			},
			{
				name:     "one month earlier",
				from:     time.Date(2024, 2, 1, 12, 0, 0, 0, time.UTC),
				now:      baseTime,
				expected: -1,
			},
			{
				name:     "12 months later (one year)",
				from:     baseTime,
				now:      time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC),
				expected: 12,
			},
			{
				name:     "cross year boundary",
				from:     time.Date(2023, 12, 1, 12, 0, 0, 0, time.UTC),
				now:      time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC),
				expected: 1,
			},
			{
				name:     "multiple years",
				from:     time.Date(2022, 6, 1, 12, 0, 0, 0, time.UTC),
				now:      time.Date(2024, 3, 1, 12, 0, 0, 0, time.UTC),
				expected: 21, // (2024-2022)*12 + (3-6) = 24 - 3 = 21
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result := Monthly.PartitionDistance(tt.from, tt.now)
				assert.Equal(t, tt.expected, result, "PartitionDistance(%v, %v) = %d, want %d", tt.from, tt.now, result, tt.expected)
			})
		}
	})

	t.Run("Daily", func(t *testing.T) {
		tests := []struct {
			name     string
			from     time.Time
			now      time.Time
			expected int
		}{
			{
				name:     "same day",
				from:     baseTime,
				now:      baseTime,
				expected: 0,
			},
			{
				name:     "one day later",
				from:     baseTime,
				now:      time.Date(2024, 1, 2, 12, 0, 0, 0, time.UTC),
				expected: 1,
			},
			{
				name:     "one day earlier",
				from:     time.Date(2024, 1, 2, 12, 0, 0, 0, time.UTC),
				now:      baseTime,
				expected: -1,
			},
			{
				name:     "7 days later (one week)",
				from:     baseTime,
				now:      time.Date(2024, 1, 8, 12, 0, 0, 0, time.UTC),
				expected: 7,
			},
			{
				name:     "30 days later",
				from:     baseTime,
				now:      time.Date(2024, 1, 31, 12, 0, 0, 0, time.UTC),
				expected: 30,
			},
			{
				name:     "cross month boundary",
				from:     time.Date(2024, 1, 31, 12, 0, 0, 0, time.UTC),
				now:      time.Date(2024, 2, 1, 12, 0, 0, 0, time.UTC),
				expected: 1,
			},
			{
				name:     "partial day difference (should round down)",
				from:     baseTime,
				now:      time.Date(2024, 1, 1, 18, 0, 0, 0, time.UTC), // 6 hours later
				expected: 0,                                            // Less than 24 hours, so 0 days
			},
			{
				name:     "exactly 24 hours later",
				from:     baseTime,
				now:      time.Date(2024, 1, 2, 12, 0, 0, 0, time.UTC),
				expected: 1,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result := Daily.PartitionDistance(tt.from, tt.now)
				assert.Equal(t, tt.expected, result, "PartitionDistance(%v, %v) = %d, want %d", tt.from, tt.now, result, tt.expected)
			})
		}
	})

	t.Run("Hourly", func(t *testing.T) {
		tests := []struct {
			name     string
			from     time.Time
			now      time.Time
			expected int
		}{
			{
				name:     "same hour",
				from:     baseTime,
				now:      baseTime,
				expected: 0,
			},
			{
				name:     "one hour later",
				from:     baseTime,
				now:      time.Date(2024, 1, 1, 13, 0, 0, 0, time.UTC),
				expected: 1,
			},
			{
				name:     "one hour earlier",
				from:     time.Date(2024, 1, 1, 13, 0, 0, 0, time.UTC),
				now:      baseTime,
				expected: -1,
			},
			{
				name:     "24 hours later (one day)",
				from:     baseTime,
				now:      time.Date(2024, 1, 2, 12, 0, 0, 0, time.UTC),
				expected: 24,
			},
			{
				name:     "partial hour difference (should round down)",
				from:     baseTime,
				now:      time.Date(2024, 1, 1, 12, 30, 0, 0, time.UTC), // 30 minutes later
				expected: 0,                                             // Less than 1 hour, so 0 hours
			},
			{
				name:     "exactly 1 hour later",
				from:     baseTime,
				now:      time.Date(2024, 1, 1, 13, 0, 0, 0, time.UTC),
				expected: 1,
			},
			{
				name:     "cross day boundary",
				from:     time.Date(2024, 1, 1, 23, 0, 0, 0, time.UTC),
				now:      time.Date(2024, 1, 2, 1, 0, 0, 0, time.UTC),
				expected: 2,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				result := Hourly.PartitionDistance(tt.from, tt.now)
				assert.Equal(t, tt.expected, result, "PartitionDistance(%v, %v) = %d, want %d", tt.from, tt.now, result, tt.expected)
			})
		}
	})

	t.Run("Invalid partition frequency", func(t *testing.T) {
		// Test with an invalid partition frequency
		invalidPF := PartitionFrequency("invalid")
		result := invalidPF.PartitionDistance(baseTime, baseTime)
		assert.Equal(t, 0, result, "Invalid partition frequency should return 0")
	})
}
