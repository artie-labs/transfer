package redis

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/db"
	"github.com/artie-labs/transfer/lib/destination"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/artie-labs/transfer/lib/optimization"
	sqllib "github.com/artie-labs/transfer/lib/sql"
)

const (
	artieEmittedAtField = "_artie_emitted_at"
	artieDataField      = "_artie_data"
)

type Store struct {
	config      config.Config
	redisClient *redis.Client
	configMap   types.DestinationTableConfigMap
}

func (s *Store) GetConfig() config.Config {
	return s.config
}

func (s *Store) Validate() error {
	if s.config.Redis == nil {
		return fmt.Errorf("redis config is nil")
	}

	if s.config.Redis.Host == "" {
		return fmt.Errorf("redis host is empty")
	}

	if s.config.Redis.Port <= 0 {
		return fmt.Errorf("invalid redis port: %d", s.config.Redis.Port)
	}

	return nil
}

func (s *Store) IdentifierFor(topicConfig kafkalib.DatabaseAndSchemaPair, table string) sqllib.TableIdentifier {
	return NewTableIdentifier(topicConfig.Database, topicConfig.Schema, table)
}

func (s *Store) Dialect() sqllib.Dialect {
	// Redis doesn't use SQL dialects
	return nil
}

func (s *Store) Dedupe(_ context.Context, _ sqllib.TableIdentifier, _ []string, _ bool) error {
	return fmt.Errorf("dedupe is not supported for Redis")
}

func (s *Store) GetTableConfig(_ context.Context, tableID sqllib.TableIdentifier, _ bool) (*types.DestinationTableConfig, error) {
	tableConfig := s.configMap.GetTableConfig(tableID)
	if tableConfig == nil {
		// Return an empty config - Redis doesn't need to track columns like SQL databases
		tableConfig = types.NewDestinationTableConfig(nil, false)
		s.configMap.AddTable(tableID, tableConfig)
	}
	return tableConfig, nil
}

func (s *Store) SweepTemporaryTables(_ context.Context) error {
	// Redis doesn't use temporary tables
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

func (s *Store) Append(ctx context.Context, tableData *optimization.TableData, _ bool) error {
	// For Redis, append and merge are the same - we always create new records
	if _, err := s.Merge(ctx, tableData); err != nil {
		return fmt.Errorf("failed to append: %w", err)
	}
	return nil
}

// Merge writes table data to Redis as Hash entries
// Each row becomes a Redis Hash with key pattern: namespace:schema:table:id
// The ID is generated using Redis INCR on a counter key
func (s *Store) Merge(ctx context.Context, tableData *optimization.TableData) (bool, error) {
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
	counterKey := redisTableID.CounterKey()
	recordsWritten := 0

	// Use a pipeline for better performance
	pipe := s.redisClient.Pipeline()

	for _, row := range rows {
		// Convert row to map for JSON serialization
		rowData := make(map[string]any)
		for _, col := range cols {
			value, _ := row.GetValue(col.Name())
			rowData[col.Name()] = value
		}

		// Serialize row data to JSON
		jsonData, err := json.Marshal(rowData)
		if err != nil {
			return false, fmt.Errorf("failed to marshal row data: %w", err)
		}

		// Generate unique ID for this record using INCR
		id, err := s.redisClient.Incr(ctx, counterKey).Result()
		if err != nil {
			return false, fmt.Errorf("failed to generate ID: %w", err)
		}

		// Create the Redis key for this record
		recordKey := redisTableID.RecordKey(id)

		// Store as Hash with two fields: _artie_emitted_at and _artie_data
		pipe.HSet(ctx, recordKey, artieEmittedAtField, time.Now().UTC().Format(time.RFC3339))
		pipe.HSet(ctx, recordKey, artieDataField, string(jsonData))

		recordsWritten++
	}

	// Execute the pipeline
	if _, err := pipe.Exec(ctx); err != nil {
		return false, fmt.Errorf("failed to execute Redis pipeline: %w", err)
	}

	slog.Info("Successfully wrote records to Redis",
		slog.String("table", redisTableID.FullyQualifiedName()),
		slog.Int("recordCount", recordsWritten),
	)

	return true, nil
}

func (s *Store) IsRetryableError(err error) bool {
	// Network errors, connection errors, etc. are retryable
	// This is a simplified implementation
	return err != nil && (err == context.DeadlineExceeded || err == context.Canceled)
}

func (s *Store) DropTable(ctx context.Context, tableID sqllib.TableIdentifier) error {
	redisTableID, ok := tableID.(TableIdentifier)
	if !ok {
		return fmt.Errorf("expected tableID to be a TableIdentifier, got %T", tableID)
	}

	// Delete all keys matching the pattern: namespace:schema:table:*
	pattern := redisTableID.KeyPattern()
	iter := s.redisClient.Scan(ctx, 0, pattern, 100).Iterator()

	keysToDelete := []string{}
	for iter.Next(ctx) {
		keysToDelete = append(keysToDelete, iter.Val())
	}

	if err := iter.Err(); err != nil {
		return fmt.Errorf("failed to scan keys: %w", err)
	}

	if len(keysToDelete) > 0 {
		// Also delete the counter key
		keysToDelete = append(keysToDelete, redisTableID.CounterKey())

		if err := s.redisClient.Del(ctx, keysToDelete...).Err(); err != nil {
			return fmt.Errorf("failed to delete keys: %w", err)
		}

		slog.Info("Dropped Redis table",
			slog.String("table", redisTableID.FullyQualifiedName()),
			slog.Int("keysDeleted", len(keysToDelete)),
		)
	}

	// Remove from config map
	s.configMap.RemoveTable(tableID)

	return nil
}

func LoadRedis(ctx context.Context, cfg config.Config, _ *db.Store) (destination.Destination, error) {
	if cfg.Redis == nil {
		return nil, fmt.Errorf("redis config is nil")
	}

	// Create Redis client
	rdb := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", cfg.Redis.Host, cfg.Redis.Port),
		Password: cfg.Redis.Password,
		DB:       cfg.Redis.Database,
	})

	// Test connection
	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	store := &Store{
		config:      cfg,
		redisClient: rdb,
	}

	if err := store.Validate(); err != nil {
		return nil, err
	}

	slog.Info("Successfully connected to Redis",
		slog.String("host", cfg.Redis.Host),
		slog.Int("port", cfg.Redis.Port),
		slog.Int("database", cfg.Redis.Database),
	)

	return store, nil
}
