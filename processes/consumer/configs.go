package consumer

import (
	"github.com/artie-labs/transfer/lib/cdc"
	"github.com/artie-labs/transfer/lib/kafkalib"
)

type TopicConfigFormatter struct {
	Tc *kafkalib.TopicConfig
	cdc.Format
}
