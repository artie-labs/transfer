package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestKafka_Brokers(t *testing.T) {
	type _tc struct {
		bootstrapServer string
		expectedBrokers []string
	}

	testCases := []_tc{
		{
			bootstrapServer: "host1:port1,host2:port2",
			expectedBrokers: []string{"host1:port1", "host2:port2"},
		},
		{
			bootstrapServer: "host1:port1",
			expectedBrokers: []string{"host1:port1"},
		},
	}

	for _, tc := range testCases {
		kafka := Kafka{
			BootstrapServer: tc.bootstrapServer,
		}

		assert.Equal(t, tc.expectedBrokers, kafka.Brokers(), tc.bootstrapServer)
	}
}
