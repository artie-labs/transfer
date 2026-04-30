package consumer

import (
	"context"
	"fmt"
	"time"

	"github.com/artie-labs/transfer/lib"
	"github.com/artie-labs/transfer/lib/artie"
	"github.com/artie-labs/transfer/lib/cdc"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/destination"
	"github.com/artie-labs/transfer/lib/telemetry/metrics/base"
	"github.com/artie-labs/transfer/lib/webhooks"
	"github.com/artie-labs/transfer/models"
	"github.com/artie-labs/transfer/models/event"
)

type processArgs struct {
	Msgs                   []artie.Message
	GroupID                string
	TopicToConfigFormatMap *TcFmtMap
	WhClient               *webhooks.Client
	EncryptionKey          []byte
	Cache                  *lib.KVCache[string]
	FlushByDefault         bool
}

func (p processArgs) process(ctx context.Context, cfg config.Config, inMemDB *models.DatabaseData, dest destination.Destination, metricsClient base.Client) (cdc.TableID, error) {
	if p.TopicToConfigFormatMap == nil {
		return cdc.TableID{}, fmt.Errorf("failed to process, topicConfig is nil")
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

	// if any if any events are successfully processed these will all be set
	var topicConfigPtr *TopicConfigFormatter = nil
	var shouldFlush bool = false
	var flushReason string = ""

	// if all events processed have the same tableId this will be set,
	// otherwise it will be unset
	var tableIdPtr *cdc.TableID

	for _, msg := range p.Msgs {
		// handle tombstone
		if len(msg.Value()) == 0 {
			continue
		}

		if topicConfigPtr == nil {
			topicConfig, ok := p.TopicToConfigFormatMap.GetTopicFmt(msg.Topic())
			if !ok {
				tags["what"] = "failed_topic_lookup"
				return cdc.TableID{}, fmt.Errorf("failed to get topic name: %q", msg.Topic())
			}
			topicConfigPtr = &topicConfig
		}

		tags["database"] = topicConfigPtr.tc.Database
		tags["schema"] = topicConfigPtr.tc.Schema

		pkMap, err := topicConfigPtr.buildPKMap(msg.Key(), reservedColumns)
		if err != nil {
			tags["what"] = "marshall_pk_err"
			return cdc.TableID{}, fmt.Errorf("cannot unmarshal key %q: %w", string(msg.Key()), err)
		}

		_event, err := topicConfigPtr.GetEventFromBytes(msg.Value())
		if err != nil {
			tags["what"] = "marshal_value_err"
			return cdc.TableID{}, fmt.Errorf("cannot unmarshal event: %w", err)
		}

		tags["op"] = string(_event.Operation())
		evt, err := event.ToMemoryEvent(ctx, dest, _event, pkMap, topicConfigPtr.tc, cfg.Mode, cfg.SharedDestinationSettings, p.EncryptionKey, p.Cache)
		if err != nil {
			tags["what"] = "to_mem_event_err"
			return cdc.TableID{}, fmt.Errorf("cannot convert to memory event: %w", err)
		}

		tableId := evt.GetTableID()
		if tableIdPtr == nil {
			tableIdPtr = &tableId
		} else if tableIdPtr.String() != tableId.String() {
			// using the zero value of the struct as a barrier value
			tableIdPtr = &cdc.TableID{}
		}

		// Table name is only available after event has been cast
		tags["table"] = evt.GetTable()
		if topicConfigPtr.ShouldSkip(string(_event.Operation())) {
			continue
		}

		if cfg.Reporting.EmitExecutionTime {
			evt.EmitExecutionTimeLag(metricsClient)
		}

		eventShouldFlush, eventFlushReason, err := evt.Save(cfg, inMemDB, topicConfigPtr.tc, reservedColumns)
		if err != nil {
			tags["what"] = "save_fail"
			return cdc.TableID{}, fmt.Errorf("event failed to save: %w", err)
		}
		shouldFlush = shouldFlush || eventShouldFlush
		if eventFlushReason != "" {
			flushReason = eventFlushReason
		}
	}

	if p.FlushByDefault {
		flushReason = "flushOnReceive"
	}

	if tableIdPtr == nil {
		tableIdPtr = &cdc.TableID{}
	}

	// if topicConfigPtr is nil it means there's nothing to flush anyways
	if topicConfigPtr != nil && (shouldFlush || p.FlushByDefault) {
		err := FlushSingleTopic(ctx, inMemDB, dest, metricsClient, p.WhClient, Args{Reason: flushReason, ReportDBExecutionTime: cfg.Reporting.EmitDBExecutionTime}, topicConfigPtr.tc.Topic, false)
		if err != nil {
			tags["what"] = "flush_fail"
		}
		return *tableIdPtr, err
	}
	return *tableIdPtr, nil
}
