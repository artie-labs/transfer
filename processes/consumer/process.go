package consumer

import (
	"context"
	"fmt"
	"time"

	"github.com/artie-labs/transfer/lib/artie"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/destination"
	"github.com/artie-labs/transfer/lib/telemetry/metrics"
	"github.com/artie-labs/transfer/models/event"
)

type ProcessArgs struct {
	Msg                    artie.Message
	GroupID                string
	TopicToConfigFormatMap *TcFmtMap
}

// processMessage will return:
// 1. TableName (string)
// 2. Error
// We are using the TableName for emitting Kafka ingestion lag
func processMessage(ctx context.Context, cfg config.Config, dest destination.Baseline, processArgs ProcessArgs) (string, error) {
	if processArgs.TopicToConfigFormatMap == nil {
		return "", fmt.Errorf("failed to process, topicConfig is nil")
	}

	tags := map[string]string{
		"groupID": processArgs.GroupID,
		"topic":   processArgs.Msg.Topic(),
		"what":    "success",
	}
	st := time.Now()
	defer func() {
		metrics.FromContext(ctx).Timing("process.message", time.Since(st), tags)
	}()

	topicConfig, isOk := processArgs.TopicToConfigFormatMap.GetTopicFmt(processArgs.Msg.Topic())
	if !isOk {
		tags["what"] = "failed_topic_lookup"
		return "", fmt.Errorf("failed to get topic name: %s", processArgs.Msg.Topic())
	}

	tags["database"] = topicConfig.tc.Database
	tags["schema"] = topicConfig.tc.Schema

	pkMap, err := topicConfig.GetPrimaryKey(processArgs.Msg.Key(), topicConfig.tc)
	if err != nil {
		tags["what"] = "marshall_pk_err"
		return "", fmt.Errorf("cannot unmarshall key, key: %s, err: %v", string(processArgs.Msg.Key()), err)
	}

	typingSettings := cfg.SharedTransferConfig.TypingSettings
	_event, err := topicConfig.GetEventFromBytes(typingSettings, processArgs.Msg.Value())
	if err != nil {
		tags["what"] = "marshall_value_err"
		return "", fmt.Errorf("cannot unmarshall event, err: %v", err)
	}

	tags["op"] = _event.Operation()
	evt := event.ToMemoryEvent(_event, pkMap, topicConfig.tc)
	// Table name is only available after event has been casted
	tags["table"] = evt.Table

	// Check to see if we should skip first
	// This way, we can emit a specific tag to be more clear
	if evt.ShouldSkip(topicConfig.tc.SkipDelete) {
		tags["skipped"] = "yes"
		return evt.Table, nil
	}

	shouldFlush, flushReason, err := evt.Save(ctx, cfg, topicConfig.tc, processArgs.Msg)
	if err != nil {
		tags["what"] = "save_fail"
		return "", fmt.Errorf("event failed to save, err: %v", err)
	}

	if shouldFlush {
		return evt.Table, Flush(dest, Args{
			Context:       ctx,
			Reason:        flushReason,
			SpecificTable: evt.Table,
		})
	}

	return evt.Table, nil
}
