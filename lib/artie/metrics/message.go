package metrics

import (
	"time"

	"github.com/artie-labs/transfer/lib/artie"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/telemetry/metrics/base"
)

// EmitRowLag will diff against the partition's high watermark and the message's offset
func EmitRowLag(m artie.Message, metricsClient base.Client, mode config.Mode, groupID, table string) {
	metricsClient.GaugeWithSample(
		"row.lag",
		float64(m.HighWaterMark()-m.Offset()),
		map[string]string{
			"mode":    mode.String(),
			"groupID": groupID,
			"table":   table,
		},
		0.5)
}

func EmitIngestionLag(m artie.Message, metricsClient base.Client, mode config.Mode, groupID, table string) {
	metricsClient.Timing("ingestion.lag", time.Since(m.PublishTime()), map[string]string{
		"mode":    mode.String(),
		"groupID": groupID,
		"table":   table,
	})
}
