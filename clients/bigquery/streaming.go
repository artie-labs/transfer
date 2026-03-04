package bigquery

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"cloud.google.com/go/bigquery/storage/managedwriter"
	"cloud.google.com/go/bigquery/storage/managedwriter/adapt"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/artie-labs/transfer/clients/bigquery/dialect"
	"github.com/artie-labs/transfer/lib/typing/columns"
)

type streamEntry struct {
	mu                sync.Mutex
	stream            *managedwriter.ManagedStream
	messageDescriptor *protoreflect.MessageDescriptor
	offset            int64
}

func (e *streamEntry) CurrentOffset() int64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.offset
}

func (e *streamEntry) AdvanceOffset(n int64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.offset += n
}

func NewStreamEntry(ctx context.Context, client *managedwriter.Client, cols []columns.Column, tableID dialect.TableIdentifier) (*streamEntry, error) {
	messageDescriptor, err := columnsToMessageDescriptor(cols)
	if err != nil {
		return nil, err
	}

	schemaDescriptor, err := adapt.NormalizeDescriptor(*messageDescriptor)
	if err != nil {
		return nil, err
	}

	stream, err := client.NewManagedStream(ctx,
		managedwriter.WithDestinationTable(
			managedwriter.TableParentFromParts(tableID.ProjectID(), tableID.Dataset(), tableID.Table()),
		),
		managedwriter.WithType(managedwriter.CommittedStream),
		managedwriter.WithSchemaDescriptor(schemaDescriptor),
		managedwriter.EnableWriteRetries(true),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create managed stream: %w", err)
	}
	return &streamEntry{stream: stream, messageDescriptor: messageDescriptor}, nil
}

type StreamManager struct {
	mu      sync.Mutex
	client  *managedwriter.Client
	streams map[string]*streamEntry
}

func NewStreamManager(ctx context.Context, projectID string) (*StreamManager, error) {
	client, err := managedwriter.NewClient(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("failed to create stream manager cleint: %w", err)
	}
	manager := StreamManager{
		mu:      sync.Mutex{},
		client:  client,
		streams: map[string]*streamEntry{},
	}
	return &manager, nil
}

func (sm *StreamManager) getOrCreateStream(ctx context.Context, tableID dialect.TableIdentifier, cols []columns.Column) (*streamEntry, error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	key := tableID.FullyQualifiedName()
	if entry, ok := sm.streams[key]; ok {
		return entry, nil
	}

	entry, err := NewStreamEntry(ctx, sm.client, cols, tableID)
	if err != nil {
		return nil, err
	}
	sm.streams[key] = entry
	return entry, nil
}

func (sm *StreamManager) Close() {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	for _, entry := range sm.streams {
		if err := entry.stream.Close(); err != nil {
			slog.Warn("failed to close managed stream", slog.Any("err", err))
		}
	}
	if err := sm.client.Close(); err != nil {
		slog.Warn("failed to close managed writer client", slog.Any("err", err))
	}
}
