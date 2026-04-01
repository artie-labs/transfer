package config

import (
	"fmt"

	"github.com/artie-labs/transfer/lib/kafkalib"
)

type Kinesis struct {
	StreamName      string                  `yaml:"streamName"`
	Region          string                  `yaml:"region"`
	AccessKeyID     string                  `yaml:"accessKeyId,omitempty"`
	SecretAccessKey string                  `yaml:"secretAccessKey,omitempty"`
	TopicConfigs    []*kafkalib.TopicConfig  `yaml:"topicConfigs"`
}

func (k *Kinesis) Validate() error {
	if k.StreamName == "" {
		return fmt.Errorf("kinesis streamName is required")
	}
	if k.Region == "" {
		return fmt.Errorf("kinesis region is required")
	}
	return nil
}
