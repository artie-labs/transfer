package consumer

import (
	"context"
	"fmt"
	"github.com/artie-labs/transfer/lib/telemetry/metrics"
	"github.com/artie-labs/transfer/models"
	"time"
)

func processMessage(ctx context.Context, msg Message, topicToConfigFmtMap map[string]TopicConfigFormatter, groupID string) (shouldFlush bool, err error) {
	tags := map[string]string{
		"groupID": groupID,
		"topic":   msg.Topic(),
		"what":    "success",
	}
	st := time.Now()
	defer func() {
		metrics.FromContext(ctx).Timing("process.message", time.Since(st), tags)
	}()

	topicConfig, isOk := topicToConfigFmtMap[msg.Topic()]
	if !isOk {
		tags["what"] = "failed_topic_lookup"
		return false, fmt.Errorf("failed to get topic name: %s", msg.Topic)
	}

	tags["database"] = topicConfig.Tc.Database
	tags["schema"] = topicConfig.Tc.Schema
	tags["table"] = topicConfig.Tc.TableName

	pkName, pkValue, err := topicConfig.GetPrimaryKey(ctx, msg.Key(), topicConfig.Tc)
	if err != nil {
		tags["what"] = "marshall_pk_err"
		return false, fmt.Errorf("cannot unmarshall key, key: %s, err: %v", string(msg.Key()), err)
	}

	event, err := topicConfig.GetEventFromBytes(ctx, msg.Value())
	if err != nil {
		// TODO: Can we filter tombstone events?
		// A tombstone event will be sent to Kafka when a DELETE happens.
		// Which causes marshalling error.
		tags["what"] = "marshall_value_err"
		return false, fmt.Errorf("cannot unmarshall event, err: %v", err)
	}

	evt := models.ToMemoryEvent(ctx, event, pkName, pkValue, topicConfig.Tc)
	shouldFlush, err = evt.Save(topicConfig.Tc, msg)
	if err != nil {
		tags["what"] = "save_fail"
		err = fmt.Errorf("event failed to save, err: %v", err)
	}

	// Using a named return, don't need to pass
	return
}
