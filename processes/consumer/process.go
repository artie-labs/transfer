package consumer

import (
	"context"
	"fmt"
	"time"

	"github.com/artie-labs/transfer/lib/artie"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/destination"
	"github.com/artie-labs/transfer/lib/telemetry/metrics/base"
	"github.com/artie-labs/transfer/models"
	"github.com/artie-labs/transfer/models/event"
)

type processArgs struct {
	Msg                    artie.Message
	GroupID                string
	TopicToConfigFormatMap *TcFmtMap
}

func (p processArgs) process(ctx context.Context, cfg config.Config, inMemDB *models.DatabaseData, dest destination.Baseline, metricsClient base.Client) (string, error) {
	if p.TopicToConfigFormatMap == nil {
		return "", fmt.Errorf("failed to process, topicConfig is nil")
	}

	tags := map[string]string{
		"mode":    cfg.Mode.String(),
		"groupID": p.GroupID,
		"what":    "success",
	}

	st := time.Now()
	// We are wrapping this in a defer function so that the values do not get immediately evaluated and miss with our actual process duration.
	defer func() {
		metricsClient.Timing("process.message", time.Since(st), tags)
	}()

	topicConfig, ok := p.TopicToConfigFormatMap.GetTopicFmt(p.Msg.Topic())
	if !ok {
		tags["what"] = "failed_topic_lookup"
		return "", fmt.Errorf("failed to get topic name: %q", p.Msg.Topic())
	}

	tags["database"] = topicConfig.tc.Database
	tags["schema"] = topicConfig.tc.Schema

	pkMap, err := topicConfig.GetPrimaryKey(p.Msg.Key(), topicConfig.tc)
	if err != nil {
		tags["what"] = "marshall_pk_err"
		return "", fmt.Errorf("cannot unmarshall key %s: %w", string(p.Msg.Key()), err)
	}

	_event, err := topicConfig.GetEventFromBytes(p.Msg.Value())
	if err != nil {
		tags["what"] = "marshall_value_err"
		return "", fmt.Errorf("cannot unmarshall event: %w", err)
	}

	tags["op"] = string(_event.Operation())
	evt, err := event.ToMemoryEvent(_event, pkMap, topicConfig.tc, cfg.Mode)
	if err != nil {
		tags["what"] = "to_mem_event_err"
		return "", fmt.Errorf("cannot convert to memory event: %w", err)
	}

	// Table name is only available after event has been cast
	tags["table"] = evt.GetTable()
	if topicConfig.tc.ShouldSkip(string(_event.Operation())) {
		// Check to see if we should skip first
		// This way, we can emit a specific tag to be more clear
		tags["skipped"] = "yes"
		return evt.GetTable(), nil
	}

	if cfg.Reporting.EmitExecutionTime {
		evt.EmitExecutionTimeLag(metricsClient)
	}

	shouldFlush, flushReason, err := evt.Save(cfg, inMemDB, topicConfig.tc, p.Msg)
	if err != nil {
		tags["what"] = "save_fail"
		return "", fmt.Errorf("event failed to save: %w", err)
	}

	if shouldFlush {
		err = Flush(ctx, inMemDB, dest, metricsClient, Args{
			Reason:        flushReason,
			SpecificTable: evt.GetTable(),
		})
		if err != nil {
			tags["what"] = "flush_fail"
		}
		return evt.GetTable(), err
	}

	return evt.GetTable(), nil
}
