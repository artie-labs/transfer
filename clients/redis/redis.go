package redis

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"syscall"
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
	streamMaxLen        = 1000 // Auto trim to 1000 entries
)

var retryableNetworkErrors = []error{
	syscall.ECONNRESET,
	syscall.ECONNREFUSED,
	io.EOF,
	syscall.ETIMEDOUT,
}

// isRetryableNetworkError checks for common network errors that are retryable
func isRetryableNetworkError(err error) bool {
	if err == nil {
		return false
	}

	// Check for standard network errors
	for _, retryableErr := range retryableNetworkErrors {
		if errors.Is(err, retryableErr) {
			return true
		}
	}

	// Check for net.Error timeout
	if netErr, ok := err.(net.Error); ok {
		if netErr.Timeout() {
			return true
		}
	}

	return false
}

type Store struct {
	config      config.Config
	redisClient *redis.Client
	configMap   *types.DestinationTableConfigMap
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
		tableConfig = types.NewDestinationTableConfig(nil, false)
		s.configMap.AddTable(tableID, tableConfig)
	}
	return tableConfig, nil
}

func (s *Store) SweepTemporaryTables(_ context.Context) error {
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

// Merge writes rows from TableData as individual entries into a Redis Stream.
// Each entry contains two fields: the CDC event JSON (artieData) and an emitted-at timestamp (artieEmittedAt).
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
	streamKey := redisTableID.StreamKey()
	recordsWritten := 0
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
				artieEmittedAtField: time.Now().UTC().Format(time.RFC3339),
				artieDataField:      string(jsonData),
			},
		})
		recordsWritten++
	}

	if _, err := pipeline.Exec(ctx); err != nil {
		return false, fmt.Errorf("failed to execute pipeline: %w", err)
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

	// Check for standard network errors (connection reset, refused, timeout, etc.)
	// These are handled by the lib/db package
	if isRetryableNetworkError(err) {
		return true
	}

	// Check for Redis-specific retryable errors
	errMsg := err.Error()

	// Server busy or loading errors
	if strings.Contains(errMsg, "BUSY") ||
		strings.Contains(errMsg, "TRYAGAIN") ||
		strings.Contains(errMsg, "LOADING") {
		return true
	}

	// Connection pool errors
	if strings.Contains(errMsg, "connection pool timeout") ||
		strings.Contains(errMsg, "i/o timeout") {
		return true
	}

	// Cluster-specific retryable errors
	if strings.Contains(errMsg, "CLUSTERDOWN") ||
		strings.Contains(errMsg, "MOVED") ||
		strings.Contains(errMsg, "ASK") {
		return true
	}

	// Master/replica errors
	if strings.Contains(errMsg, "READONLY") ||
		strings.Contains(errMsg, "MASTERDOWN") {
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
		configMap:   &types.DestinationTableConfigMap{},
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
