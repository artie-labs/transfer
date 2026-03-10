package bigquery

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	storagepb "cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"cloud.google.com/go/bigquery/storage/managedwriter"
	"cloud.google.com/go/bigquery/storage/managedwriter/adapt"
	"google.golang.org/api/option"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

// To run:
//
//	BENCHMARK_BQ_PROJECT=your-project BENCHMARK_BQ_DATASET=your-dataset go test -run='^$' -bench=BenchmarkStream -benchtime=1x -v ./clients/bigquery/

func skipIfNoCredentials(t testing.TB) (string, string, string) {
	projectID := os.Getenv("BENCHMARK_BQ_PROJECT")
	datasetID := os.Getenv("BENCHMARK_BQ_DATASET")
	credsFile := os.Getenv("BENCHMARK_BQ_CREDS")
	if projectID == "" || datasetID == "" || credsFile == "" {
		t.Skip("Skipping: set BENCHMARK_BQ_PROJECT, BENCHMARK_BQ_DATASET, and BENCHMARK_BQ_CREDS to run")
	}
	return projectID, datasetID, credsFile
}

func setupBenchmarkTable(ctx context.Context, projectID, datasetID, credsFile string) (string, func(), error) {
	client, err := bigquery.NewClient(ctx, projectID, option.WithCredentialsFile(credsFile))
	if err != nil {
		return "", nil, fmt.Errorf("failed to create BQ client: %w", err)
	}

	tableName := fmt.Sprintf("stream_bench_%d", time.Now().UnixNano())
	schema := bigquery.Schema{
		{Name: "id", Type: bigquery.IntegerFieldType},
		{Name: "name", Type: bigquery.StringFieldType},
		{Name: "created_at", Type: bigquery.TimestampFieldType},
	}

	tableRef := client.Dataset(datasetID).Table(tableName)
	if err := tableRef.Create(ctx, &bigquery.TableMetadata{Schema: schema}); err != nil {
		client.Close()
		return "", nil, fmt.Errorf("failed to create table: %w", err)
	}

	cleanup := func() {
		_ = tableRef.Delete(ctx)
		client.Close()
	}

	return tableName, cleanup, nil
}

var benchSchema = &storagepb.TableSchema{
	Fields: []*storagepb.TableFieldSchema{
		{Name: "id", Type: storagepb.TableFieldSchema_INT64, Mode: storagepb.TableFieldSchema_NULLABLE},
		{Name: "name", Type: storagepb.TableFieldSchema_STRING, Mode: storagepb.TableFieldSchema_NULLABLE},
		{Name: "created_at", Type: storagepb.TableFieldSchema_TIMESTAMP, Mode: storagepb.TableFieldSchema_NULLABLE},
	},
}

func getMessageDescriptor() (protoreflect.MessageDescriptor, error) {
	descriptor, err := adapt.StorageSchemaToProto2Descriptor(benchSchema, "root")
	if err != nil {
		return nil, err
	}

	messageDescriptor, ok := descriptor.(protoreflect.MessageDescriptor)
	if !ok {
		return nil, fmt.Errorf("failed to cast to MessageDescriptor")
	}

	return messageDescriptor, nil
}

func buildProtoRow(id int, msgDesc protoreflect.MessageDescriptor) ([]byte, error) {
	msg := dynamicpb.NewMessage(msgDesc)
	msg.Set(msgDesc.Fields().ByName("id"), protoreflect.ValueOfInt64(int64(id)))
	msg.Set(msgDesc.Fields().ByName("name"), protoreflect.ValueOfString(fmt.Sprintf("row-%d", id)))
	msg.Set(msgDesc.Fields().ByName("created_at"), protoreflect.ValueOfInt64(time.Now().UnixMicro()))
	return proto.Marshal(msg)
}

func getNormalizedDescriptor() (*descriptorpb.DescriptorProto, protoreflect.MessageDescriptor, error) {
	msgDesc, err := getMessageDescriptor()
	if err != nil {
		return nil, nil, err
	}

	normalized, err := adapt.NormalizeDescriptor(msgDesc)
	if err != nil {
		return nil, nil, err
	}

	return normalized, msgDesc, nil
}

// BenchmarkStreamNewPerRow simulates the current approach: new client + stream per row.
func BenchmarkStreamNewPerRow(b *testing.B) {
	projectID, datasetID, credsFile := skipIfNoCredentials(b)
	ctx := context.Background()

	tableName, cleanup, err := setupBenchmarkTable(ctx, projectID, datasetID, credsFile)
	if err != nil {
		b.Fatalf("setup failed: %v", err)
	}
	defer cleanup()

	normalized, msgDesc, err := getNormalizedDescriptor()
	if err != nil {
		b.Fatalf("failed to get descriptor: %v", err)
	}

	const numRows = 50
	tableParent := managedwriter.TableParentFromParts(projectID, datasetID, tableName)

	b.ResetTimer()
	start := time.Now()

	for i := range numRows {
		mwClient, err := managedwriter.NewClient(ctx, projectID, option.WithCredentialsFile(credsFile))
		if err != nil {
			b.Fatalf("failed to create managed writer client: %v", err)
		}

		stream, err := mwClient.NewManagedStream(ctx,
			managedwriter.WithDestinationTable(tableParent),
			managedwriter.WithType(managedwriter.CommittedStream),
			managedwriter.WithSchemaDescriptor(normalized),
			managedwriter.EnableWriteRetries(true),
		)
		if err != nil {
			mwClient.Close()
			b.Fatalf("failed to create stream: %v", err)
		}

		row, err := buildProtoRow(i, msgDesc)
		if err != nil {
			stream.Close()
			mwClient.Close()
			b.Fatalf("failed to build row: %v", err)
		}

		result, err := stream.AppendRows(ctx, [][]byte{row})
		if err != nil {
			stream.Close()
			mwClient.Close()
			b.Fatalf("failed to append: %v", err)
		}

		if _, err := result.FullResponse(ctx); err != nil {
			stream.Close()
			mwClient.Close()
			b.Fatalf("append failed: %v", err)
		}

		stream.Close()
		mwClient.Close()
	}

	b.StopTimer()
	elapsed := time.Since(start)
	b.Logf("NewPerRow: %d rows in %v (%.1f ms/row)", numRows, elapsed, float64(elapsed.Milliseconds())/float64(numRows))
}

// BenchmarkStreamPersistent simulates the persistent approach: one client + stream, reused for all rows.
func BenchmarkStreamPersistent(b *testing.B) {
	projectID, datasetID, credsFile := skipIfNoCredentials(b)
	ctx := context.Background()

	tableName, cleanup, err := setupBenchmarkTable(ctx, projectID, datasetID, credsFile)
	if err != nil {
		b.Fatalf("setup failed: %v", err)
	}
	defer cleanup()

	normalized, msgDesc, err := getNormalizedDescriptor()
	if err != nil {
		b.Fatalf("failed to get descriptor: %v", err)
	}

	const numRows = 50
	tableParent := managedwriter.TableParentFromParts(projectID, datasetID, tableName)

	mwClient, err := managedwriter.NewClient(ctx, projectID, option.WithCredentialsFile(credsFile))
	if err != nil {
		b.Fatalf("failed to create managed writer client: %v", err)
	}
	defer mwClient.Close()

	stream, err := mwClient.NewManagedStream(ctx,
		managedwriter.WithDestinationTable(tableParent),
		managedwriter.WithType(managedwriter.CommittedStream),
		managedwriter.WithSchemaDescriptor(normalized),
		managedwriter.EnableWriteRetries(true),
	)
	if err != nil {
		b.Fatalf("failed to create stream: %v", err)
	}
	defer stream.Close()

	b.ResetTimer()
	start := time.Now()

	for i := range numRows {
		row, err := buildProtoRow(i, msgDesc)
		if err != nil {
			b.Fatalf("failed to build row: %v", err)
		}

		result, err := stream.AppendRows(ctx, [][]byte{row})
		if err != nil {
			b.Fatalf("failed to append: %v", err)
		}

		if _, err := result.FullResponse(ctx); err != nil {
			b.Fatalf("append failed: %v", err)
		}
	}

	b.StopTimer()
	elapsed := time.Since(start)
	b.Logf("Persistent: %d rows in %v (%.1f ms/row)", numRows, elapsed, float64(elapsed.Milliseconds())/float64(numRows))
}
