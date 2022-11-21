package kafkalib

import (
	"fmt"
	"github.com/artie-labs/transfer/lib/array"
)

type TopicConfig struct {
	Database      string `yaml:"db"`
	TableName     string `yaml:"tableName"`
	Schema        string `yaml:"schema"`
	Topic         string `yaml:"topic"`
	IdempotentKey string `yaml:"idempotentKey"`
	CDCFormat     string `yaml:"cdcFormat"`
}

func (t *TopicConfig) String() string {
	// TODO test
	if t == nil {
		return ""
	}

	return fmt.Sprintf(
		"db=%s, tableName=%s, schema=%s, topic=%s, idempotentKey=%s, cdcFormat=%s",
		t.Database, t.TableName, t.Schema, t.Topic, t.IdempotentKey, t.CDCFormat)
}

func (t *TopicConfig) Valid() bool {
	// IdempotentKey is optional.

	return !array.Empty([]string{t.Database, t.TableName, t.Schema, t.Topic, t.CDCFormat})
}

func (t *TopicConfig) ToCacheKey(partition int64) string {
	return fmt.Sprintf("%s#%d", t.Topic, partition)
}

func ToCacheKey(topic string, partition int64) string {
	return fmt.Sprintf("%s#%d", topic, partition)
}

// ToFqName is the fully-qualified table name in DWH
func (t *TopicConfig) ToFqName() string {
	return fmt.Sprintf("%s.%s.%s", t.Database, t.Schema, t.TableName)
}
