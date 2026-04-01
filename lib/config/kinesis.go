package config

import (
	"fmt"

	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/stringutil"
)

type Kinesis struct {
	StreamName     string                  `yaml:"streamName"`
	Region         string                  `yaml:"region"`
	AwsAccessKeyID string                  `yaml:"awsAccessKeyID"`
	AwsSecretKey   string                  `yaml:"awsSecretKey"`
	TopicConfigs   []*kafkalib.TopicConfig `yaml:"topicConfigs"`
}

func (k *Kinesis) Validate() error {
	if k == nil {
		return fmt.Errorf("kinesis config is nil")
	}

	if stringutil.Empty(k.StreamName) {
		return fmt.Errorf("kinesis streamName is empty")
	}

	if stringutil.Empty(k.Region) {
		return fmt.Errorf("kinesis region is empty")
	}

	return nil
}
