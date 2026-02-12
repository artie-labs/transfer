package redis

import (
	"context"
	"crypto/tls"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"

	"github.com/redis/go-redis/v9"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/db"
	"github.com/artie-labs/transfer/lib/destination"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/optimization"
	sqllib "github.com/artie-labs/transfer/lib/sql"
	webhooksclient "github.com/artie-labs/transfer/lib/webhooksClient"
)

const (
	dataField    = "data"
	streamMaxLen = 1000 // Auto trim to 1000 entries
)

type Store struct {
	config      config.Config
	redisClient *redis.Client
	configMap   *types.DestinationTableConfigMap
}

func (s *Store) GetConfig() config.Config {
	return s.config
}

func (s *Store) IsOLTP() bool {
	return false
}

func (s *Store) Validate() error {
	return s.config.Redis.Validate()
}

func (s *Store) IdentifierFor(topicConfig kafkalib.DatabaseAndSchemaPair, table string) sqllib.TableIdentifier {
	return NewTableIdentifier(topicConfig.Database, topicConfig.Schema, table)
}

func (s *Store) Dialect() sqllib.Dialect {
	// Redis doesn't use SQL dialects
	return nil
}

func (s *Store) Dedupe(_ context.Context, _ sqllib.TableIdentifier, _ kafkalib.DatabaseAndSchemaPair, _ []string, _ bool) error {
	return fmt.Errorf("dedupe is not supported for Redis")
}

func (s *Store) GetTableConfig(_ context.Context, tableID sqllib.TableIdentifier, _ bool) (*types.DestinationTableConfig, error) {
	tableConfig := s.configMap.GetTableConfig(tableID)
	if tableConfig == nil {
		tableConfig = types.NewDestinationTableConfig(nil, false)
		s.configMap.AddTable(tableID, tableConfig)
	}
	return tableConfig, nil
}

func (s *Store) SweepTemporaryTables(_ context.Context, _ *webhooksclient.Client) error {
	return nil
}

func (s *Store) ExecContext(_ context.Context, _ string, _ ...any) (sql.Result, error) {
	return nil, fmt.Errorf("ExecContext is not supported for Redis")
}

func (s *Store) QueryContext(_ context.Context, _ string, _ ...any) (*sql.Rows, error) {
	return nil, fmt.Errorf("QueryContext is not supported for Redis")
}

func (s *Store) Begin() (*sql.Tx, error) {
	return nil, fmt.Errorf("transactions are not supported for Redis")
}

func (s *Store) LoadDataIntoTable(_ context.Context, _ *optimization.TableData, _ *types.DestinationTableConfig, _, _ sqllib.TableIdentifier, _ types.AdditionalSettings, _ bool) error {
	return fmt.Errorf("LoadDataIntoTable is not supported for Redis")
}

func (s *Store) Append(ctx context.Context, tableData *optimization.TableData, whClient *webhooksclient.Client, _ bool) error {
	// For Redis, append and merge are the same - we always create new records
	if _, err := s.Merge(ctx, tableData, whClient); err != nil {
		return fmt.Errorf("failed to append: %w", err)
	}
	return nil
}

// Merge writes rows from TableData as individual entries into a Redis Stream.
// Each entry contains two fields: the CDC event JSON (artieData) and an emitted-at timestamp (artieEmittedAt).
func (s *Store) Merge(ctx context.Context, tableData *optimization.TableData, whClient *webhooksclient.Client) (bool, error) {
	if tableData.ShouldSkipUpdate() {
		return false, nil
	}

	tableID := s.IdentifierFor(tableData.TopicConfig().BuildDatabaseAndSchemaPair(), tableData.Name())
	redisTableID, ok := tableID.(TableIdentifier)
	if !ok {
		return false, fmt.Errorf("expected tableID to be a TableIdentifier, got %T", tableID)
	}

	rows := tableData.Rows()
	if len(rows) == 0 {
		return false, nil
	}

	cols := tableData.ReadOnlyInMemoryCols().ValidColumns()
	streamKey := redisTableID.StreamKey()
	var recordsWritten int
	pipeline := s.redisClient.Pipeline()

	for _, row := range rows {
		rowData := make(map[string]any)
		for _, col := range cols {
			value, _ := row.GetValue(col.Name())
			rowData[col.Name()] = value
		}

		jsonData, err := json.Marshal(rowData)
		if err != nil {
			return false, fmt.Errorf("failed to marshal row data: %w", err)
		}

		slog.Info("Writing stream entry",
			slog.String("stream", streamKey),
			slog.Int("jsonDataLen", len(jsonData)),
		)

		pipeline.XAdd(ctx, &redis.XAddArgs{
			Stream: streamKey,
			MaxLen: streamMaxLen,
			Approx: true,
			ID:     "*",
			Values: map[string]interface{}{
				dataField: string(jsonData),
			},
		})
		recordsWritten++
	}

	cmds, err := pipeline.Exec(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to execute pipeline: %w", err)
	}

	// Check individual command errors
	for i, cmd := range cmds {
		if err := cmd.Err(); err != nil {
			return false, fmt.Errorf("failed to execute XAdd command %d: %w", i, err)
		}
	}

	slog.Info("Successfully wrote records to Redis Stream",
		slog.String("stream", streamKey),
		slog.Int("recordCount", recordsWritten),
	)

	return true, nil
}

func (s *Store) IsRetryableError(err error) bool {
	if err == nil {
		return false
	}

	// Check for standard network errors using the generic db package
	if db.IsRetryableError(err) {
		return true
	}

	// Check for Redis-specific retryable errors using typed error checks
	if isRedisRetryableError(err) {
		return true
	}

	errMsg := err.Error()

	// Check for substring matches to handle prefixed errors (e.g., "redis: connection pool timeout")
	return strings.Contains(errMsg, "connection pool timeout") || strings.Contains(errMsg, "i/o timeout")
}

// isRedisRetryableError checks for Redis-specific errors that are retryable
func isRedisRetryableError(err error) bool {
	if _, moved := redis.IsMovedError(err); moved {
		return true
	}
	if _, ask := redis.IsAskError(err); ask {
		return true
	}
	if redis.IsClusterDownError(err) {
		return true
	}
	if redis.IsTryAgainError(err) {
		return true
	}
	if redis.IsLoadingError(err) {
		return true
	}
	if redis.IsReadOnlyError(err) {
		return true
	}
	if redis.IsMasterDownError(err) {
		return true
	}
	if strings.Contains(err.Error(), "BUSY") {
		return true
	}

	return false
}

func (s *Store) DropTable(ctx context.Context, tableID sqllib.TableIdentifier) error {
	redisTableID, ok := tableID.(TableIdentifier)
	if !ok {
		return fmt.Errorf("expected tableID to be a TableIdentifier, got %T", tableID)
	}

	// Delete the stream key
	streamKey := redisTableID.StreamKey()

	if err := s.redisClient.Del(ctx, streamKey).Err(); err != nil {
		return fmt.Errorf("failed to delete stream: %w", err)
	}

	slog.Info("Dropped Redis stream",
		slog.String("stream", streamKey),
	)

	s.configMap.RemoveTable(tableID)

	return nil
}

func LoadRedis(ctx context.Context, cfg config.Config, _ *db.Store) (destination.Destination, error) {
	if cfg.Redis == nil {
		return nil, fmt.Errorf("redis config is nil")
	}

	opts := &redis.Options{
		Addr:     fmt.Sprintf("%s:%d", cfg.Redis.Host, cfg.Redis.Port),
		Username: cfg.Redis.Username,
		Password: cfg.Redis.Password,
		DB:       cfg.Redis.Database,
	}

	// Enable TLS
	if cfg.Redis.TLS {
		opts.TLSConfig = &tls.Config{
			MinVersion: tls.VersionTLS12,
		}
	}

	rdb := redis.NewClient(opts)

	// Test connection
	if err := rdb.Ping(ctx).Err(); err != nil {
		if closeErr := rdb.Close(); closeErr != nil {
			slog.Error("Failed to close Redis client after ping failure", slog.Any("error", closeErr))
		}
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	store := &Store{
		config:      cfg,
		redisClient: rdb,
		configMap:   &types.DestinationTableConfigMap{},
	}

	if err := store.Validate(); err != nil {
		if closeErr := rdb.Close(); closeErr != nil {
			slog.Error("Failed to close Redis client after validation failure", slog.Any("error", closeErr))
		}
		return nil, err
	}

	slog.Info("Successfully connected to Redis",
		slog.String("host", cfg.Redis.Host),
		slog.Int("port", cfg.Redis.Port),
		slog.Int("database", cfg.Redis.Database),
	)

	return store, nil
}
