package consumer

import (
	"context"
	"fmt"
	"time"

	"github.com/artie-labs/transfer/lib/artie"
	"github.com/artie-labs/transfer/lib/cdc"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/destination"
	"github.com/artie-labs/transfer/lib/telemetry/metrics/base"
	"github.com/artie-labs/transfer/models"
	"github.com/artie-labs/transfer/models/event"
)

type processArgs[M artie.MessageType] struct {
	Msg                    artie.Message[M]
	GroupID                string
	TopicToConfigFormatMap *TcFmtMap
}

func (p processArgs[M]) process(ctx context.Context, cfg config.Config, inMemDB *models.DatabaseData, dest destination.Baseline, metricsClient base.Client) (cdc.TableID, error) {
	if p.TopicToConfigFormatMap == nil {
		return cdc.TableID{}, fmt.Errorf("failed to process, topicConfig is nil")
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
		return cdc.TableID{}, fmt.Errorf("failed to get topic name: %q", p.Msg.Topic())
	}

	tags["database"] = topicConfig.tc.Database
	tags["schema"] = topicConfig.tc.Schema
	pkMap, err := topicConfig.GetPrimaryKey(p.Msg.Key(), topicConfig.tc)
	if err != nil {
		tags["what"] = "marshall_pk_err"
		return cdc.TableID{}, fmt.Errorf("cannot unmarshal key %q: %w", string(p.Msg.Key()), err)
	}

	_event, err := topicConfig.GetEventFromBytes(p.Msg.Value())
	if err != nil {
		tags["what"] = "marshal_value_err"
		return cdc.TableID{}, fmt.Errorf("cannot unmarshal event: %w", err)
	}

	tags["op"] = string(_event.Operation())
	evt, err := event.ToMemoryEvent(ctx, dest, _event, pkMap, topicConfig.tc, cfg.Mode)
	if err != nil {
		tags["what"] = "to_mem_event_err"
		return cdc.TableID{}, fmt.Errorf("cannot convert to memory event: %w", err)
	}

	// Table name is only available after event has been cast
	tags["table"] = evt.GetTable()
	if topicConfig.tc.ShouldSkip(string(_event.Operation())) {
		// Check to see if we should skip first
		// This way, we can emit a specific tag to be more clear
		tags["skipped"] = "yes"
		return evt.GetTableID(), nil
	}

	if cfg.Reporting.EmitExecutionTime {
		evt.EmitExecutionTimeLag(metricsClient)
	}

	shouldFlush, flushReason, err := evt.Save(cfg, inMemDB, topicConfig.tc)
	if err != nil {
		tags["what"] = "save_fail"
		return cdc.TableID{}, fmt.Errorf("event failed to save: %w", err)
	}

	if shouldFlush {
		err = FlushSingleTopic[M](ctx, inMemDB, dest, metricsClient, Args{Reason: flushReason}, topicConfig.tc.Topic, false)
		if err != nil {
			tags["what"] = "flush_fail"
		}
		return evt.GetTableID(), err
	}

	return evt.GetTableID(), nil
}
