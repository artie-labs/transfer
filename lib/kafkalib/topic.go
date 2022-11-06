package kafkalib

import "fmt"

type TopicConfig struct {
	Database  string `yaml:"db"`
	TableName string `yaml:"tableName"`
	Schema    string `yaml:"schema"`
	Topic     string `yaml:"topic"`
}

func (t *TopicConfig) ToCacheKey(partition int32) string {
	return fmt.Sprintf("%s#%d", t.Topic, partition)
}

func ToCacheKey(topic string, partition int32) string {
	return fmt.Sprintf("%s#%d", topic, partition)
}

// ToFqName is the fully-qualified table name in DWH
func (t *TopicConfig) ToFqName() string {
	return fmt.Sprintf("%s.%s.%s", t.Database, t.Schema, t.TableName)
}
