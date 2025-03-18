package kafkalib

import (
	"fmt"
	"slices"
	"strings"

	"github.com/artie-labs/transfer/lib/kafkalib/partition"
	"github.com/artie-labs/transfer/lib/stringutil"
)

// GetUniqueTopicConfigs - will return a list of unique TopicConfigs based on the database and schema in O(n) time.
func GetUniqueTopicConfigs(tcs []*TopicConfig) []TopicConfig {
	var uniqueTopicConfigs []TopicConfig
	seenMap := make(map[string]bool)
	for _, tc := range tcs {
		key := fmt.Sprintf("%s###%s", tc.Database, tc.Schema)
		if _, isOk := seenMap[key]; !isOk {
			seenMap[key] = true                                  // Mark this as seen
			uniqueTopicConfigs = append(uniqueTopicConfigs, *tc) // Now add this to the list
		}
	}

	return uniqueTopicConfigs
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
	// TODO: Deprecate BigQueryPartitionSettings and use AdditionalMergePredicates instead.
	BigQueryPartitionSettings *partition.BigQuerySettings `yaml:"bigQueryPartitionSettings,omitempty"`
	AdditionalMergePredicates []partition.MergePredicates `yaml:"additionalMergePredicates,omitempty"`
	PrimaryKeysOverride       []string                    `yaml:"primaryKeysOverride,omitempty"`
	MultiStepMergeSettings    *MultiStepMergeSettings     `yaml:"multiStepMergeSettings,omitempty"`

	// Settings related to columns:
	ColumnsToHash []string `yaml:"columnsToHash,omitempty"`
	// [ColumnsToExclude] can be used to exclude columns from being written to the destination.
	ColumnsToExclude []string `yaml:"columnsToExclude,omitempty"`
	ColumnsToEncrypt []string `yaml:"columnsToEncrypt,omitempty"`
	ColumnsToDecrypt []string `yaml:"columnsToDecrypt,omitempty"`

	// Internal metadata
	opsToSkipMap map[string]bool `yaml:"-"`
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

	if err := t.ValidateColumns(); err != nil {
		return fmt.Errorf("invalid columns: %w", err)
	}

	return nil
}

func (t TopicConfig) ValidateColumns() error {
	seenColumns := make(map[string]string)
	checkDuplicate := func(columns []string, operation string) error {
		for _, col := range columns {
			if existingOp, exists := seenColumns[col]; exists {
				return fmt.Errorf("column %q cannot be both %q and %q", col, existingOp, operation)
			}
			seenColumns[col] = operation
		}
		return nil
	}

	// Check each list for duplicates
	if err := checkDuplicate(t.ColumnsToEncrypt, "encrypted"); err != nil {
		return err
	}
	if err := checkDuplicate(t.ColumnsToDecrypt, "decrypted"); err != nil {
		return err
	}
	if err := checkDuplicate(t.ColumnsToHash, "hashed"); err != nil {
		return err
	}
	if err := checkDuplicate(t.ColumnsToExclude, "excluded"); err != nil {
		return err
	}

	return nil
}
