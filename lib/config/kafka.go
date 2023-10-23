package config

import (
	"strings"

	"github.com/artie-labs/transfer/lib/kafkalib"
)

type Kafka struct {
	// Comma-separated Kafka servers to port.
	// e.g. host1:port1,host2:port2,...
	// Following kafka's spec mentioned here: https://kafka.apache.org/documentation/#producerconfigs_bootstrap.servers
	BootstrapServer string                  `yaml:"bootstrapServer"`
	GroupID         string                  `yaml:"groupID"`
	Username        string                  `yaml:"username"`
	Password        string                  `yaml:"password"`
	EnableAWSMSKIAM bool                    `yaml:"enableAWSMKSIAM"`
	TopicConfigs    []*kafkalib.TopicConfig `yaml:"topicConfigs"`
}

func (k *Kafka) Brokers() []string {
	var brokers []string
	for _, bootstrapServer := range strings.Split(k.BootstrapServer, ",") {
		brokers = append(brokers, bootstrapServer)
	}

	return brokers
}
