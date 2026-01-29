package kafkalib

import (
	"fmt"
	"math/rand/v2"
	"strings"
)

type Kafka struct {
	// Comma-separated Kafka servers to port.
	// e.g. host1:port1,host2:port2,...
	// Following kafka's spec mentioned here: https://kafka.apache.org/documentation/#producerconfigs_bootstrap.servers
	BootstrapServer string         `yaml:"bootstrapServer"`
	GroupID         string         `yaml:"groupID"`
	TopicConfigs    []*TopicConfig `yaml:"topicConfigs"`

	// Optional parameters
	Username        string `yaml:"username,omitempty"`
	Password        string `yaml:"password,omitempty"`
	EnableAWSMSKIAM bool   `yaml:"enableAWSMKSIAM,omitempty"`
	DisableTLS      bool   `yaml:"disableTLS,omitempty"`

	// WaitForTopics - if true, polls until topics exist before consuming.
	// This prevents relying on broker auto-creation and allows graceful startup
	// when topics may not exist yet.
	WaitForTopics bool `yaml:"waitForTopics,omitempty"`

	// Franz-go fetch tuning options (optional, uses library defaults if not set)
	// FetchMaxBytes is the maximum bytes per broker per fetch call (default: 50 MiB)
	FetchMaxBytes int32 `yaml:"fetchMaxBytes,omitempty"`
	// FetchMaxPartitionBytes is the maximum bytes per partition per fetch (default: 1 MiB)
	FetchMaxPartitionBytes int32 `yaml:"fetchMaxPartitionBytes,omitempty"`
	// FetchMinBytes is the minimum bytes the broker waits to accumulate before responding (default: 1 byte)
	FetchMinBytes int32 `yaml:"fetchMinBytes,omitempty"`
	// FetchMaxWaitMs is how long the broker waits to accumulate FetchMinBytes in milliseconds (default: 5000ms)
	FetchMaxWaitMs int32 `yaml:"fetchMaxWaitMs,omitempty"`
}

func (k *Kafka) Topics() []string {
	var out []string
	for _, config := range k.TopicConfigs {
		out = append(out, config.Topic)
	}

	return out
}

func (k *Kafka) String() string {
	// Don't log credentials.
	return fmt.Sprintf("bootstrapServer=%s, groupID=%s, user_set=%v, pass_set=%v",
		k.BootstrapServer, k.GroupID, k.Username != "", k.Password != "")
}

func (k *Kafka) BootstrapServers(shuffle bool) []string {
	parts := strings.Split(k.BootstrapServer, ",")
	if shuffle {
		rand.Shuffle(len(parts), func(i, j int) {
			parts[i], parts[j] = parts[j], parts[i]
		})
	}

	return parts
}
