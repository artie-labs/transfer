package elasticsearch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"strings"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/config/constants"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/optimization"
	sqllib "github.com/artie-labs/transfer/lib/sql"
	"github.com/artie-labs/transfer/lib/typing"
	"github.com/artie-labs/transfer/lib/webhooks"
)

type Store struct {
	config config.Config
	client *elasticsearch.Client
}

func (s *Store) Label() constants.DestinationKind {
	return constants.Elasticsearch
}

func (s *Store) GetConfig() config.Config {
	return s.config
}

func (s *Store) IsOLTP() bool {
	return false
}

func (s *Store) IdentifierFor(topicConfig kafkalib.DatabaseAndSchemaPair, table string) sqllib.TableIdentifier {
	// Use exactly the table name or a combined name
	// Elasticsearch indices should be lowercase
	name := fmt.Sprintf("%s_%s_%s", topicConfig.Database, topicConfig.Schema, table)
	name = strings.ToLower(name)
	return NewTableIdentifier("", "", name)
}

func buildDocumentID(row optimization.Row, primaryKeys []string) string {
	if len(primaryKeys) == 0 {
		return ""
	}
	var parts []string
	for _, pk := range primaryKeys {
		if val, ok := row.GetValue(pk); ok && val != nil {
			parts = append(parts, fmt.Sprintf("%v", val))
		} else {
			parts = append(parts, "null")
		}
	}
	return strings.Join(parts, "_")
}

func (s *Store) IsTableCreatedAlready(ctx context.Context, tableID sqllib.TableIdentifier) (bool, error) {
	esIdent := tableID.(TableIdentifier)
	req := esapi.IndicesExistsRequest{
		Index: []string{esIdent.Name()},
	}
	res, err := req.Do(ctx, s.client)
	if err != nil {
		return false, fmt.Errorf("check index existence: %w", err)
	}
	defer res.Body.Close()

	if res.StatusCode == 200 {
		return true, nil
	}
	if res.StatusCode == 404 {
		return false, nil
	}
	return false, fmt.Errorf("unexpected status code: %d", res.StatusCode)
}

func (s *Store) DropTable(ctx context.Context, tableID sqllib.TableIdentifier) error {
	esIdent := tableID.(TableIdentifier)
	req := esapi.IndicesDeleteRequest{
		Index: []string{esIdent.Name()},
	}
	res, err := req.Do(ctx, s.client)
	if err != nil {
		return fmt.Errorf("delete index request: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() && res.StatusCode != 404 {
		return fmt.Errorf("delete index failed: %s", res.String())
	}
	slog.Info("Dropped Elasticsearch index", slog.String("index", esIdent.Name()))
	return nil
}

func mapArtieTypeToElasticsearch(col typing.KindDetails) string {
	// Check for BIGINT before falling through to the generic int case. BIGINT columns have
	// Kind == "int" but OptionalIntegerKind == BigIntegerKind, so we must inspect both.
	if col.Kind == typing.Integer.Kind && col.OptionalIntegerKind != nil && *col.OptionalIntegerKind == typing.BigIntegerKind {
		return "long"
	}

	switch col.Kind {
	case typing.Invalid.Kind, typing.String.Kind, typing.Array.Kind:
		return "keyword"
	case typing.Integer.Kind:
		return "integer"
	case typing.Float.Kind:
		return "float"
	case typing.EDecimal.Kind:
		return "double"
	case typing.Boolean.Kind:
		return "boolean"
	case typing.TimestampNTZ.Kind, typing.TimestampTZ.Kind, typing.Date.Kind:
		return "date"
	case typing.TimeKindDetails.Kind:
		return "keyword"
	case typing.Struct.Kind:
		return "object"
	case typing.Bytes.Kind:
		return "binary"
	default:
		return "keyword"
	}
}

func (s *Store) CreateTable(ctx context.Context, tableID sqllib.TableIdentifier, tableData *optimization.TableData) error {
	properties := make(map[string]any)
	for _, col := range tableData.ReadOnlyInMemoryCols().ValidColumns() {
		properties[col.Name()] = map[string]string{
			"type": mapArtieTypeToElasticsearch(col.KindDetails),
		}
	}

	mappings := map[string]any{
		"properties": properties,
	}

	// Default to 1 shard and 1 replica if not explicitly configured.
	// Elasticsearch rejects index creation when number_of_shards is 0.
	numShards := s.config.Elasticsearch.IndexSettings.NumberOfShards
	if numShards <= 0 {
		numShards = 1
	}
	numReplicas := s.config.Elasticsearch.IndexSettings.NumberOfReplicas
	if numReplicas < 0 {
		numReplicas = 1
	}

	settings := map[string]any{
		"number_of_shards":   numShards,
		"number_of_replicas": numReplicas,
	}

	body := map[string]any{
		"mappings": mappings,
		"settings": settings,
	}

	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("marshal index body: %w", err)
	}

	esIdent := tableID.(TableIdentifier)
	req := esapi.IndicesCreateRequest{
		Index: esIdent.Name(),
		Body:  bytes.NewReader(bodyBytes),
	}
	res, err := req.Do(ctx, s.client)
	if err != nil {
		return fmt.Errorf("create index request: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		b, _ := io.ReadAll(res.Body)
		return fmt.Errorf("create index failed: %s, body: %s", res.Status(), string(b))
	}

	slog.Info("Created Elasticsearch index", slog.String("index", esIdent.Name()))
	return nil
}

func (s *Store) Merge(ctx context.Context, tableData *optimization.TableData, whClient *webhooks.Client) (bool, error) {
	if tableData.ShouldSkipUpdate() {
		return false, nil
	}

	tableID := s.IdentifierFor(tableData.TopicConfig().BuildDatabaseAndSchemaPair(), tableData.Name())
	esIdent := tableID.(TableIdentifier)

	exists, err := s.IsTableCreatedAlready(ctx, tableID)
	if err != nil {
		return false, fmt.Errorf("check table exists: %w", err)
	}
	if !exists {
		if err := s.CreateTable(ctx, tableID, tableData); err != nil {
			return false, fmt.Errorf("create table: %w", err)
		}
	}

	rows := tableData.Rows()
	if len(rows) == 0 {
		return false, nil
	}

	cols := tableData.ReadOnlyInMemoryCols().ValidColumns()
	pks := tableData.PrimaryKeys()

	// Create _bulk request payload
	var buf bytes.Buffer
	var recordsWritten int

	for _, row := range rows {
		docID := buildDocumentID(row, pks)
		action := map[string]any{
			"_index": esIdent.Name(),
		}
		if docID != "" {
			action["_id"] = docID
		}

		// Hard-delete: emit a bulk delete action instead of re-indexing the document.
		delVal, _ := row.GetValue(constants.DeleteColumnMarker)
		isDelete, _ := delVal.(bool)
		if isDelete {
			if docID == "" {
				// Can't delete without an _id — skip silently.
				continue
			}
			metaJSON, err := json.Marshal(map[string]any{"delete": action})
			if err != nil {
				return false, fmt.Errorf("marshal delete meta: %w", err)
			}
			buf.Write(metaJSON)
			buf.WriteByte('\n')
			recordsWritten++
			continue
		}

		metaJSON, err := json.Marshal(map[string]any{"index": action})
		if err != nil {
			return false, fmt.Errorf("marshal meta: %w", err)
		}
		buf.Write(metaJSON)
		buf.WriteByte('\n')

		rowData := make(map[string]any)
		for _, col := range cols {
			if col.Name() == constants.DeleteColumnMarker {
				continue
			}
			value, _ := row.GetValue(col.Name())
			rowData[col.Name()] = value
		}

		dataJSON, err := json.Marshal(rowData)
		if err != nil {
			return false, fmt.Errorf("marshal data: %w", err)
		}
		buf.Write(dataJSON)
		buf.WriteByte('\n')

		recordsWritten++
	}

	if buf.Len() == 0 {
		return false, nil
	}
	res, err := s.client.Bulk(bytes.NewReader(buf.Bytes()), s.client.Bulk.WithContext(ctx))
	if err != nil {
		return false, fmt.Errorf("bulk request: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		b, _ := io.ReadAll(res.Body)
		return false, fmt.Errorf("bulk failed: %s, body: %s", res.Status(), string(b))
	}

	// Parse response to check for individual errors
	var blk map[string]any
	if err := json.NewDecoder(res.Body).Decode(&blk); err != nil {
		return false, fmt.Errorf("decode bulk response: %w", err)
	}

	if errorsFlag, ok := blk["errors"].(bool); ok && errorsFlag {
		return false, fmt.Errorf("bulk operation contained errors")
	}

	slog.Info("Successfully wrote records to Elasticsearch",
		slog.String("index", esIdent.Name()),
		slog.Int("recordCount", recordsWritten),
	)

	return true, nil
}

func (s *Store) Append(ctx context.Context, tableData *optimization.TableData, whClient *webhooks.Client, _ bool) error {
	if _, err := s.Merge(ctx, tableData, whClient); err != nil {
		return fmt.Errorf("append failed: %w", err)
	}
	return nil
}

func (s *Store) IsRetryableError(err error) bool {
	if err == nil {
		return false
	}
	// Add custom Elasticsearch retry logic here if needed
	errMsg := strings.ToLower(err.Error())
	if strings.Contains(errMsg, "timeout") || strings.Contains(errMsg, "connection refused") || strings.Contains(errMsg, "too many requests") {
		return true
	}
	return false
}

func LoadStore(ctx context.Context, cfg config.Config) (*Store, error) {
	if cfg.Elasticsearch == nil {
		return nil, fmt.Errorf("elasticsearch config is nil")
	}

	esCfg := elasticsearch.Config{
		Addresses: []string{cfg.Elasticsearch.Host},
		Username:  cfg.Elasticsearch.Username,
		Password:  cfg.Elasticsearch.Password,
		APIKey:    cfg.Elasticsearch.APIKey,
	}

	es, err := elasticsearch.NewClient(esCfg)
	if err != nil {
		return nil, fmt.Errorf("create elasticsearch client: %w", err)
	}

	res, err := es.Info(es.Info.WithContext(ctx))
	if err != nil {
		return nil, fmt.Errorf("ping elasticsearch: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, fmt.Errorf("ping failed: %s", res.Status())
	}

	store := &Store{
		config: cfg,
		client: es,
	}

	slog.Info("Successfully connected to Elasticsearch", slog.String("host", cfg.Elasticsearch.Host))

	return store, nil
}
