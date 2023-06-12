package consumer

import (
	"context"
	"fmt"
	"time"

	"github.com/artie-labs/transfer/lib/artie"
	"github.com/artie-labs/transfer/lib/telemetry/metrics"
	"github.com/artie-labs/transfer/models/event"
)

type ProcessArgs struct {
	Msg                    artie.Message
	GroupID                string
	TopicToConfigFormatMap *TcFmtMap
}

func processMessage(ctx context.Context, processArgs ProcessArgs) error {
	if processArgs.TopicToConfigFormatMap == nil {
		return fmt.Errorf("failed to process, topicConfig is nil")
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
		return fmt.Errorf("failed to get topic name: %s", processArgs.Msg.Topic())
	}

	tags["database"] = topicConfig.tc.Database
	tags["schema"] = topicConfig.tc.Schema

	pkMap, err := topicConfig.GetPrimaryKey(ctx, processArgs.Msg.Key(), topicConfig.tc)
	if err != nil {
		tags["what"] = "marshall_pk_err"
		return fmt.Errorf("cannot unmarshall key, key: %s, err: %v", string(processArgs.Msg.Key()), err)
	}

	_event, err := topicConfig.GetEventFromBytes(ctx, processArgs.Msg.Value())
	if err != nil {
		tags["what"] = "marshall_value_err"
		return fmt.Errorf("cannot unmarshall event, err: %v", err)
	}

	evt := event.ToMemoryEvent(ctx, _event, pkMap, topicConfig.tc)
	// Table name is only available after event has been casted
	tags["table"] = evt.Table
	shouldFlush, err := evt.Save(ctx, topicConfig.tc, processArgs.Msg)
	if err != nil {
		tags["what"] = "save_fail"
		err = fmt.Errorf("event failed to save, err: %v", err)
	}

	if shouldFlush {
		return Flush(Args{
			Context:       ctx,
			SpecificTable: evt.Table,
		})
	}

	return nil
}
