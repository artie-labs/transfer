package kafkalib

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/artie-labs/transfer/lib/kafkalib/partition"
	"github.com/artie-labs/transfer/lib/stringutil"
)

type DatabaseAndSchemaPair struct {
	Database string
	Schema   string
}

func GetUniqueDatabaseAndSchemaPairs(tcs []*TopicConfig) []DatabaseAndSchemaPair {
	seenMap := make(map[DatabaseAndSchemaPair]bool)
	for _, tc := range tcs {
		seenMap[tc.BuildDatabaseAndSchemaPair()] = true
	}

	return slices.Collect(maps.Keys(seenMap))
}

type MultiStepMergeSettings struct {
	Enabled bool `yaml:"enabled"`
	// FlushCount is the number of times we will flush to the multi-step merge table before merging into the destination table.
	FlushCount int `yaml:"flushCount"`
}

func (m MultiStepMergeSettings) Validate() error {
	if !m.Enabled {
		return nil
	}

	if m.FlushCount <= 0 {
		return fmt.Errorf("flush count must be greater than 0")
	}

	return nil
}

type TopicConfig struct {
	Database                 string `yaml:"db"`
	TableName                string `yaml:"tableName"`
	Schema                   string `yaml:"schema"`
	Topic                    string `yaml:"topic"`
	CDCFormat                string `yaml:"cdcFormat"`
	CDCKeyFormat             string `yaml:"cdcKeyFormat"`
	DropDeletedColumns       bool   `yaml:"dropDeletedColumns"`
	SoftDelete               bool   `yaml:"softDelete"`
	SkippedOperations        string `yaml:"skippedOperations,omitempty"`
	IncludeArtieUpdatedAt    bool   `yaml:"includeArtieUpdatedAt"`
	IncludeDatabaseUpdatedAt bool   `yaml:"includeDatabaseUpdatedAt"`
	IncludeArtieOperation    bool   `yaml:"includeArtieOperation"`
	// TODO: Deprecate BigQueryPartitionSettings and use AdditionalMergePredicates instead.
	BigQueryPartitionSettings *partition.BigQuerySettings `yaml:"bigQueryPartitionSettings,omitempty"`
	AdditionalMergePredicates []partition.MergePredicates `yaml:"additionalMergePredicates,omitempty"`
	ColumnsToHash             []string                    `yaml:"columnsToHash,omitempty"`

	// [ColumnsToInclude] can be used to specify the exact columns that should be written to the destination.
	ColumnsToInclude []string `yaml:"columnsToInclude,omitempty"`
	// [ColumnsToExclude] can be used to exclude columns from being written to the destination.
	ColumnsToExclude       []string                `yaml:"columnsToExclude,omitempty"`
	PrimaryKeysOverride    []string                `yaml:"primaryKeysOverride,omitempty"`
	MultiStepMergeSettings *MultiStepMergeSettings `yaml:"multiStepMergeSettings,omitempty"`

	// Internal metadata
	opsToSkipMap map[string]bool `yaml:"-"`
}

func (t TopicConfig) BuildDatabaseAndSchemaPair() DatabaseAndSchemaPair {
	return DatabaseAndSchemaPair{Database: t.Database, Schema: t.Schema}
}

const (
	StringKeyFmt = "org.apache.kafka.connect.storage.StringConverter"
	JSONKeyFmt   = "org.apache.kafka.connect.json.JsonConverter"
)

var validKeyFormats = []string{StringKeyFmt, JSONKeyFmt}

func (t *TopicConfig) Load() {
	// Operations that we support today:
	// 1. c - create
	// 2. r - replication (backfill)
	// 3. u - update
	// 4. d - delete

	t.opsToSkipMap = make(map[string]bool)
	for _, op := range strings.Split(t.SkippedOperations, ",") {
		// Lowercase and trim space.
		t.opsToSkipMap[strings.ToLower(strings.TrimSpace(op))] = true
	}
}

func (t TopicConfig) ShouldSkip(op string) bool {
	if t.opsToSkipMap == nil {
		panic("opsToSkipMap is nil, Load() was never called")
	}

	_, isOk := t.opsToSkipMap[op]
	return isOk
}

func (t TopicConfig) String() string {
	var msmEnabled bool
	if t.MultiStepMergeSettings != nil {
		msmEnabled = t.MultiStepMergeSettings.Enabled
	}

	return fmt.Sprintf("db=%s, schema=%s, tableNameOverride=%s, topic=%s, cdcFormat=%s, dropDeletedColumns=%v, skippedOperations=%v, msmEnabled=%v",
		t.Database, t.Schema, t.TableName, t.Topic, t.CDCFormat, t.DropDeletedColumns, t.SkippedOperations, msmEnabled)
}

func (t TopicConfig) Validate() error {
	empty := stringutil.Empty(t.Database, t.Schema, t.Topic, t.CDCFormat)
	if empty {
		return fmt.Errorf("database, schema, topic or cdc format is empty")
	}

	if !slices.Contains(validKeyFormats, t.CDCKeyFormat) {
		return fmt.Errorf("invalid cdc key format: %s", t.CDCKeyFormat)
	}

	if t.opsToSkipMap == nil {
		return fmt.Errorf("opsToSkipMap is nil, call Load() first")
	}

	if t.MultiStepMergeSettings != nil {
		if err := t.MultiStepMergeSettings.Validate(); err != nil {
			return fmt.Errorf("invalid multi-step merge settings: %w", err)
		}
	}

	// You can't specify both [ColumnsToInclude] and [ColumnsToExclude]
	if len(t.ColumnsToInclude) > 0 && len(t.ColumnsToExclude) > 0 {
		return fmt.Errorf("cannot specify both columnsToInclude and columnsToExclude")
	}

	return nil
}
