package consumer

import (
	"context"
	"fmt"
	"time"

	"github.com/artie-labs/transfer/lib/jitter"
	"github.com/artie-labs/transfer/lib/logger"

	"github.com/artie-labs/transfer/lib/artie"
	"github.com/artie-labs/transfer/lib/telemetry/metrics"
	"github.com/artie-labs/transfer/models/event"
)

type ProcessArgs struct {
	Msg                    artie.Message
	GroupID                string
	TopicToConfigFormatMap map[string]TopicConfigFormatter
	FlushChannel           chan bool
}

func processMessage(ctx context.Context, processArgs ProcessArgs) error {
	tags := map[string]string{
		"groupID": processArgs.GroupID,
		"topic":   processArgs.Msg.Topic(),
		"what":    "success",
	}
	st := time.Now()
	defer func() {
		metrics.FromContext(ctx).Timing("process.message", time.Since(st), tags)
	}()

	topicConfig, isOk := processArgs.TopicToConfigFormatMap[processArgs.Msg.Topic()]
	if !isOk {
		tags["what"] = "failed_topic_lookup"
		return fmt.Errorf("failed to get topic name: %s", processArgs.Msg.Topic())
	}

	tags["database"] = topicConfig.tc.Database
	tags["schema"] = topicConfig.tc.Schema
	tags["table"] = topicConfig.tc.TableName

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
	shouldFlush, reprocessRow, err := evt.Save(ctx, topicConfig.tc, processArgs.Msg)
	if err != nil {
		tags["what"] = "save_fail"
		err = fmt.Errorf("event failed to save, err: %v", err)
	}

	// Flush first, then reprocess row.
	if shouldFlush {
		processArgs.FlushChannel <- true
		// Jitter-sleep is necessary to allow the flush process to acquire the table lock
		// If it doesn't then the flush process may be over-exhausted since the lock got acquired by `processMessage(...)`.
		// This then leads us to make unnecessary flushes.
		jitterDuration := jitter.JitterMs(500, 1)
		time.Sleep(time.Duration(jitterDuration) * time.Millisecond)
	}

	if reprocessRow {
		logger.FromContext(ctx).WithField("key", string(processArgs.Msg.Key())).Info("this row was skipped to prevent a TOAST mismatch, re-processing this row...")
		return processMessage(ctx, processArgs)
	}
	return nil
}
