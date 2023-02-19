package kafkalib

import (
	"fmt"
	"github.com/artie-labs/transfer/lib/array"
	"github.com/artie-labs/transfer/lib/config/constants"
)

type TopicConfig struct {
	Database           string `yaml:"db"`
	TableName          string `yaml:"tableName"`
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
		"db=%s, tableName=%s, schema=%s, topic=%s, idempotentKey=%s, cdcFormat=%s, dropDeletedColumns=%v",
		t.Database, t.TableName, t.Schema, t.Topic, t.IdempotentKey, t.CDCFormat, t.DropDeletedColumns)
}

func (t *TopicConfig) Valid() bool {
	if t == nil {
		return false
	}

	// IdempotentKey is optional.
	empty := array.Empty([]string{t.Database, t.TableName, t.Schema, t.Topic, t.CDCFormat})
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

// ToFqName is the fully-qualified table name in DWH
func (t *TopicConfig) ToFqName(kind constants.DestinationKind) string {
	switch kind {
	case constants.BigQuery:
		// BigQuery doesn't use schema
		return fmt.Sprintf("%s.%s", t.Database, t.TableName)
	default:
		return fmt.Sprintf("%s.%s.%s", t.Database, t.Schema, t.TableName)
	}
}
