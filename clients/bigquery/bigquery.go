package bigquery

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/bigquery/storage/managedwriter"
	"cloud.google.com/go/bigquery/storage/managedwriter/adapt"
	_ "github.com/viant/bigquery"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/artie-labs/transfer/clients/bigquery/dialect"
	"github.com/artie-labs/transfer/clients/shared"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/db"
	"github.com/artie-labs/transfer/lib/destination"
	"github.com/artie-labs/transfer/lib/destination/ddl"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/logger"
	"github.com/artie-labs/transfer/lib/optimization"
	"github.com/artie-labs/transfer/lib/ptr"
	"github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/stringutil"
)

const (
	GooglePathToCredentialsEnvKey = "GOOGLE_APPLICATION_CREDENTIALS"
	describeNameCol               = "column_name"
	describeTypeCol               = "data_type"
	describeCommentCol            = "description"
)

type Store struct {
	configMap *types.DwhToTablesConfigMap
	batchSize int
	config    config.Config

	db.Store
}

func (s *Store) Append(tableData *optimization.TableData) error {
	return shared.Append(s, tableData, types.AdditionalSettings{})
}

func (s *Store) PrepareTemporaryTable(tableData *optimization.TableData, tableConfig *types.DwhTableConfig, tempTableID sql.TableIdentifier, _ types.AdditionalSettings, createTempTable bool) error {
	if createTempTable {
		tempAlterTableArgs := ddl.AlterTableArgs{
			Dialect:        s.Dialect(),
			Tc:             tableConfig,
			TableID:        tempTableID,
			CreateTable:    true,
			TemporaryTable: true,
			ColumnOp:       constants.Add,
			Mode:           tableData.Mode(),
		}

		if err := tempAlterTableArgs.AlterTable(s, tableData.ReadOnlyInMemoryCols().GetColumns()...); err != nil {
			return fmt.Errorf("failed to create temp table: %w", err)
		}
	}

	bqTempTableID, ok := tempTableID.(TableIdentifier)
	if !ok {
		return fmt.Errorf("unable to cast tempTableID to BigQuery TableIdentifier")
	}

	// Load the data
	return s.putTableViaLegacyAPI(context.Background(), bqTempTableID, tableData)
}

func buildLegacyRows(tableData *optimization.TableData, additionalDateFmts []string) ([]*Row, error) {
	// Cast the data into BigQuery values
	var rows []*Row
	columns := tableData.ReadOnlyInMemoryCols().ValidColumns()
	for _, value := range tableData.Rows() {
		data := make(map[string]bigquery.Value)
		for _, col := range columns {
			colVal, err := castColVal(value[col.Name()], col, additionalDateFmts)
			if err != nil {
				return nil, fmt.Errorf("failed to cast col %q: %w", col.Name(), err)
			}

			if colVal != nil {
				data[col.Name()] = colVal
			}
		}

		rows = append(rows, NewRow(data))
	}
	return rows, nil
}

func (s *Store) IdentifierFor(topicConfig kafkalib.TopicConfig, table string) sql.TableIdentifier {
	return NewTableIdentifier(s.config.BigQuery.ProjectID, topicConfig.Database, table)
}

func (s *Store) GetTableConfig(tableData *optimization.TableData) (*types.DwhTableConfig, error) {
	query := fmt.Sprintf("SELECT column_name, data_type, description FROM `%s.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS` WHERE table_name = ?;", tableData.TopicConfig().Database)
	return shared.GetTableCfgArgs{
		Dwh:                s,
		TableID:            s.IdentifierFor(tableData.TopicConfig(), tableData.Name()),
		ConfigMap:          s.configMap,
		Query:              query,
		Args:               []any{tableData.Name()},
		ColumnNameLabel:    describeNameCol,
		ColumnTypeLabel:    describeTypeCol,
		ColumnDescLabel:    describeCommentCol,
		EmptyCommentValue:  ptr.ToString(""),
		DropDeletedColumns: tableData.TopicConfig().DropDeletedColumns,
	}.GetTableConfig()
}

func (s *Store) GetConfigMap() *types.DwhToTablesConfigMap {
	if s == nil {
		return nil
	}

	return s.configMap
}

func (s *Store) Dialect() sql.Dialect {
	return dialect.BigQueryDialect{}
}

func (s *Store) AdditionalDateFormats() []string {
	return s.config.SharedTransferConfig.TypingSettings.AdditionalDateFormats
}

func (s *Store) GetClient(ctx context.Context) *bigquery.Client {
	client, err := bigquery.NewClient(ctx, s.config.BigQuery.ProjectID)
	if err != nil {
		logger.Panic("Failed to get bigquery client", slog.Any("err", err))
	}

	return client
}

func (s *Store) putTableViaLegacyAPI(ctx context.Context, tableID TableIdentifier, tableData *optimization.TableData) error {
	rows, err := buildLegacyRows(tableData, s.config.SharedTransferConfig.TypingSettings.AdditionalDateFormats)
	if err != nil {
		return err
	}

	if s.config.BigQuery.UseStorageWriteAPI {
		return s.putTableViaStorageWriteAPI(ctx, bqTableID, rows)
	} else {
		return s.putTableViaInsertAllAPI(ctx, bqTableID, rows)
	}
}

func (s *Store) putTableViaInsertAllAPI(ctx context.Context, bqTableID TableIdentifier, rows []*Row) error {
	client := s.GetClient(ctx)
	defer client.Close()

	batch := NewBatch(rows, s.batchSize)
	inserter := client.Dataset(tableID.Dataset()).Table(tableID.Table()).Inserter()
	for batch.HasNext() {
		if err := inserter.Put(ctx, batch.NextChunk()); err != nil {
			return fmt.Errorf("failed to insert rows: %w", err)
		}
	}

	return nil
}

func (s *Store) putTableViaStorageWriteAPI(ctx context.Context, bqTableID TableIdentifier, rows []*Row) error {
	// TODO: Think about whether we want to support batching in this method
	client := s.GetClient(ctx)
	defer client.Close()
	metadata, err := client.Dataset(bqTableID.Dataset()).Table(bqTableID.Table()).Metadata(ctx)
	if err != nil {
		return fmt.Errorf("failed to fetch table schema: %w", err)
	}
	messageDescriptor, err := schemaToMessageDescriptor(metadata.Schema)
	if err != nil {
		return err
	}
	schemaDescriptor, err := adapt.NormalizeDescriptor(*messageDescriptor)
	if err != nil {
		return err
	}

	managedWriterClient, err := managedwriter.NewClient(ctx, bqTableID.ProjectID())
	if err != nil {
		return fmt.Errorf("failed to create new managedwriter client: %w", err)
	}
	defer client.Close()

	managedStream, err := managedWriterClient.NewManagedStream(ctx,
		managedwriter.WithDestinationTable(
			managedwriter.TableParentFromParts(bqTableID.ProjectID(), bqTableID.Dataset(), bqTableID.Table()),
		),
		managedwriter.WithType(managedwriter.DefaultStream), // TODO: Look into using other stream types
		managedwriter.WithSchemaDescriptor(schemaDescriptor),
		managedwriter.EnableWriteRetries(true),
	)
	if err != nil {
		return fmt.Errorf("failed to create managed stream")
	}
	defer managedStream.Close()

	encoded := make([][]byte, len(rows))
	for i, row := range rows {
		message, err := rowToMessage(row, *messageDescriptor)
		if err != nil {
			return err
		}

		bytes, err := proto.Marshal(message)
		if err != nil {
			return fmt.Errorf("failed to marshal message: %w", err)
		}
		encoded[i] = bytes
	}

	result, err := managedStream.AppendRows(ctx, encoded)
	if err != nil {
		return err
	}

	if _, err := result.GetResult(ctx); err != nil {
		return err
	}

	return nil
}

func (s *Store) Dedupe(tableID sql.TableIdentifier, primaryKeys []string, topicConfig kafkalib.TopicConfig) error {
	stagingTableID := shared.TempTableID(tableID, strings.ToLower(stringutil.Random(5)))

	dedupeQueries := s.Dialect().BuildDedupeQueries(tableID, stagingTableID, primaryKeys, topicConfig)

	defer func() { _ = ddl.DropTemporaryTable(s, stagingTableID, false) }()

	return destination.ExecStatements(s, dedupeQueries)
}

func LoadBigQuery(cfg config.Config, _store *db.Store) (*Store, error) {
	cfg.BigQuery.LoadDefaultValues()
	if _store != nil {
		// Used for tests.
		return &Store{
			Store: *_store,

			configMap: &types.DwhToTablesConfigMap{},
			config:    cfg,
		}, nil
	}

	if credPath := cfg.BigQuery.PathToCredentials; credPath != "" {
		// If the credPath is set, let's set it into the env var.
		slog.Debug("Writing the path to BQ credentials to env var for google auth")
		err := os.Setenv(GooglePathToCredentialsEnvKey, credPath)
		if err != nil {
			return nil, fmt.Errorf("error setting env var for %q : %w", GooglePathToCredentialsEnvKey, err)
		}
	}

	store, err := db.Open("bigquery", cfg.BigQuery.DSN())
	if err != nil {
		return nil, err
	}
	return &Store{
		Store:     store,
		configMap: &types.DwhToTablesConfigMap{},
		batchSize: cfg.BigQuery.BatchSize,
		config:    cfg,
	}, nil
}

func schemaToMessageDescriptor(schema bigquery.Schema) (*protoreflect.MessageDescriptor, error) {
	storageSchema, err := adapt.BQSchemaToStorageTableSchema(schema)
	if err != nil {
		return nil, fmt.Errorf("failed to adapt BQ schema to protocol buffer schema")
	}
	descriptor, err := adapt.StorageSchemaToProto2Descriptor(storageSchema, "root")
	if err != nil {
		return nil, fmt.Errorf("failed to build protocol buffer descriptor: %w", err)
	}
	messageDescriptor, ok := descriptor.(protoreflect.MessageDescriptor)
	if !ok {
		return nil, fmt.Errorf("adapted descriptor is not a message descriptor")
	}
	return &messageDescriptor, nil
}

func rowToMessage(row *Row, messageDescriptor protoreflect.MessageDescriptor) (*dynamicpb.Message, error) {
	jsonBytes, err := json.Marshal(&row.data)
	if err != nil {
		return nil, err
	}

	message := dynamicpb.NewMessage(messageDescriptor)
	err = protojson.Unmarshal(jsonBytes, message)
	if err != nil {
		return nil, err
	}

	// for k, v := range row.data {
	// 	field := message.Descriptor().Fields().ByTextName(k)
	// 	if field == nil {
	// 		return nil, fmt.Errorf("failed to find a field named %q", k)
	// 	}

	// 	fmt.Printf("%v %T %v\n", field.Kind(), v, v)

	// 	switch v := v.(type) {
	// 	// Types natively supported by [protoreflect.ValueOf]
	// 	case nil,
	// 		bool,
	// 		int32,
	// 		int64,
	// 		float32,
	// 		float64,
	// 		string,
	// 		[]byte:
	// 		message.Set(field, protoreflect.ValueOf(v))
	// 	// Additional types:
	// 	case int:
	// 		message.Set(field, protoreflect.ValueOfInt64(int64(v)))
	// 	case []string:
	// 		list := message.Mutable(field).List()
	// 		for _, j := range v {
	// 			list.Append(protoreflect.ValueOf(j))
	// 		}
	// 	default:
	// 		return nil, fmt.Errorf("unable to convert %v of type %T to a proto value", v, v)
	// 	}
	// }
	return message, nil
}
