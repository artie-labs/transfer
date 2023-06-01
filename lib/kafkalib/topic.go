package kafkalib

import (
	"fmt"

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
	Database string `yaml:"db"`
	//TableName          string `yaml:"tableName"`
	Schema             string `yaml:"schema"`
	Topic              string `yaml:"topic"`
	IdempotentKey      string `yaml:"idempotentKey"`
	CDCFormat          string `yaml:"cdcFormat"`
	CDCKeyFormat       string `yaml:"cdcKeyFormat"`
	DropDeletedColumns bool   `yaml:"dropDeletedColumns"`
	SoftDelete         bool   `yaml:"softDelete"`
}

const (
	defaultKeyFormat = "org.apache.kafka.connect.storage.StringConverter"
)

var (
	validKeyFormats = []string{"org.apache.kafka.connect.json.JsonConverter",
		"org.apache.kafka.connect.storage.StringConverter"}
)

func (t *TopicConfig) String() string {
	if t == nil {
		return ""
	}

	return fmt.Sprintf(
		"db=%s, schema=%s, topic=%s, idempotentKey=%s, cdcFormat=%s, dropDeletedColumns=%v",
		t.Database, t.Schema, t.Topic, t.IdempotentKey, t.CDCFormat, t.DropDeletedColumns)
}

func (t *TopicConfig) Valid() bool {
	if t == nil {
		return false
	}

	// IdempotentKey is optional.
	empty := array.Empty([]string{t.Database, t.Schema, t.Topic, t.CDCFormat})
	if empty {
		return false
	}

	if t.CDCKeyFormat == "" {
		t.CDCKeyFormat = defaultKeyFormat
	}

	contains := array.StringContains(validKeyFormats, t.CDCKeyFormat)
	if !contains {
		return false
	}

	return true
}

func (t *TopicConfig) ToCacheKey(partition int64) string {
	return fmt.Sprintf("%s#%d", t.Topic, partition)
}

func ToCacheKey(topic string, partition int64) string {
	return fmt.Sprintf("%s#%d", topic, partition)
}
