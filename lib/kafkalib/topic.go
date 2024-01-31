package kafkalib

import (
	"fmt"
	"slices"

	"github.com/artie-labs/transfer/lib/kafkalib/partition"

	"github.com/artie-labs/transfer/lib/array"
)

type DatabaseSchemaPair struct {
	Database string
	Schema   string
}

// GetUniqueDatabaseAndSchema - does not guarantee ordering.
func GetUniqueDatabaseAndSchema(tcs []*TopicConfig) []DatabaseSchemaPair {
	dbMap := make(map[string]DatabaseSchemaPair)
	for _, tc := range tcs {
		key := fmt.Sprintf("%s###%s", tc.Database, tc.Schema)
		dbMap[key] = DatabaseSchemaPair{
			Database: tc.Database,
			Schema:   tc.Schema,
		}
	}

	var pairs []DatabaseSchemaPair
	for _, pair := range dbMap {
		pairs = append(pairs, pair)
	}

	return pairs
}

type TopicConfig struct {
	Database                  string                      `yaml:"db"`
	TableName                 string                      `yaml:"tableName"`
	Schema                    string                      `yaml:"schema"`
	Topic                     string                      `yaml:"topic"`
	IdempotentKey             string                      `yaml:"idempotentKey"`
	CDCFormat                 string                      `yaml:"cdcFormat"`
	CDCKeyFormat              string                      `yaml:"cdcKeyFormat"`
	DropDeletedColumns        bool                        `yaml:"dropDeletedColumns"`
	SoftDelete                bool                        `yaml:"softDelete"`
	SkipDelete                bool                        `yaml:"skipDelete"`
	IncludeArtieUpdatedAt     bool                        `yaml:"includeArtieUpdatedAt"`
	IncludeDatabaseUpdatedAt  bool                        `yaml:"includeDatabaseUpdatedAt"`
	BigQueryPartitionSettings *partition.BigQuerySettings `yaml:"bigQueryPartitionSettings"`
}

const (
	defaultKeyFormat = "org.apache.kafka.connect.storage.StringConverter"
	jsonFormat       = "org.apache.kafka.connect.json.JsonConverter"
)

var validKeyFormats = []string{defaultKeyFormat, jsonFormat}

func (t *TopicConfig) String() string {
	if t == nil {
		return ""
	}

	return fmt.Sprintf(
		"db=%s, schema=%s, tableNameOverride=%s, topic=%s, idempotentKey=%s, cdcFormat=%s, dropDeletedColumns=%v",
		t.Database, t.Schema, t.TableName, t.Topic, t.IdempotentKey, t.CDCFormat, t.DropDeletedColumns)
}

func (t *TopicConfig) Validate() error {
	if t == nil {
		return fmt.Errorf("topic config is nil")
	}

	// IdempotentKey is optional.
	empty := array.Empty([]string{t.Database, t.Schema, t.Topic, t.CDCFormat})
	if empty {
		return fmt.Errorf("database, schema, topic or cdc format is empty")
	}

	if t.CDCKeyFormat == "" {
		t.CDCKeyFormat = defaultKeyFormat
	}

	if !slices.Contains(validKeyFormats, t.CDCKeyFormat) {
		return fmt.Errorf("invalid cdc key format: %s", t.CDCKeyFormat)
	}

	return nil
}
