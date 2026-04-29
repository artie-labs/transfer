package consumer

import (
	"context"
	"fmt"
	"time"

	"github.com/artie-labs/transfer/lib/artie"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/destination"
	"github.com/artie-labs/transfer/lib/telemetry/metrics/base"
	"github.com/artie-labs/transfer/lib/webhooks"
	"github.com/artie-labs/transfer/models"
	"github.com/artie-labs/transfer/models/event"
)

type batchProcessArgs struct {
	Msgs                   []artie.Message
	GroupID                string
	TopicToConfigFormatMap *TcFmtMap
	WhClient               *webhooks.Client
	EncryptionKey          []byte
}

func (p batchProcessArgs) process(ctx context.Context, cfg config.Config, inMemDB *models.DatabaseData, dest destination.Destination, metricsClient base.Client) error {
	if p.TopicToConfigFormatMap == nil {
		return fmt.Errorf("failed to process, topicConfig is nil")
	}

	reservedColumns := destination.BuildReservedColumnNames(dest)

	tags := map[string]string{
		"mode":    cfg.Mode.String(),
		"groupID": p.GroupID,
		"what":    "success",
	}

	st := time.Now()
	// We are wrapping this in a defer function so that the values do not get immediately evaluated and miss with our actual process duration.
	defer func() {
		metricsClient.Timing("process.batch", time.Since(st), tags)
	}()

	var topicConfigPtr *TopicConfigFormatter = nil

	for _, msg := range p.Msgs {
		// handle tombstone
		if len(msg.Value()) == 0 {
			continue
		}

		if topicConfigPtr == nil {
			topicConfig, ok := p.TopicToConfigFormatMap.GetTopicFmt(p.Msgs[0].Topic())
			if !ok {
				tags["what"] = "failed_topic_lookup"
				return fmt.Errorf("failed to get topic name: %q", p.Msgs[0].Topic())
			}
			topicConfigPtr = &topicConfig
		}

		tags["database"] = topicConfigPtr.tc.Database
		tags["schema"] = topicConfigPtr.tc.Schema

		pkMap, err := topicConfigPtr.buildPKMap(msg.Key(), reservedColumns)
		if err != nil {
			tags["what"] = "marshall_pk_err"
			return fmt.Errorf("cannot unmarshal key %q: %w", string(msg.Key()), err)
		}

		_event, err := topicConfigPtr.GetEventFromBytes(msg.Value())
		if err != nil {
			tags["what"] = "marshal_value_err"
			return fmt.Errorf("cannot unmarshal event: %w", err)
		}

		tags["op"] = string(_event.Operation())
		evt, err := event.ToMemoryEvent(ctx, dest, _event, pkMap, topicConfigPtr.tc, cfg.Mode, cfg.SharedDestinationSettings, p.EncryptionKey)
		if err != nil {
			tags["what"] = "to_mem_event_err"
			return fmt.Errorf("cannot convert to memory event: %w", err)
		}

		// Table name is only available after event has been cast
		tags["table"] = evt.GetTable()
		if topicConfigPtr.ShouldSkip(string(_event.Operation())) {
			continue
		}

		if cfg.Reporting.EmitExecutionTime {
			evt.EmitExecutionTimeLag(metricsClient)
		}

		_, _, err = evt.Save(cfg, inMemDB, topicConfigPtr.tc, reservedColumns)
		if err != nil {
			tags["what"] = "save_fail"
			return fmt.Errorf("event failed to save: %w", err)
		}
	}

	// if topicConfigPtr is nil it means there's nothing to flush anyways
	if topicConfigPtr != nil {
		err := FlushSingleTopic(ctx, inMemDB, dest, metricsClient, p.WhClient, Args{Reason: "flushOnReceive", ReportDBExecutionTime: cfg.Reporting.EmitDBExecutionTime}, topicConfigPtr.tc.Topic, false)
		if err != nil {
			tags["what"] = "flush_fail"
		}
		return err
	}
	return nil
}
