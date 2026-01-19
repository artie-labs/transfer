package redis

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/config"
	"github.com/artie-labs/transfer/lib/destination/types"
	"github.com/artie-labs/transfer/lib/kafkalib"
)

func TestStore_GetConfig(t *testing.T) {
	cfg := config.Config{
		Redis: &config.Redis{
			Host: "localhost",
			Port: 6379,
		},
	}
	store := &Store{
		config:    cfg,
		configMap: &types.DestinationTableConfigMap{},
	}

	assert.Equal(t, cfg, store.GetConfig())
}

func TestStore_Validate(t *testing.T) {
	{
		// Valid config
		store := &Store{
			config: config.Config{
				Redis: &config.Redis{
					Host: "localhost",
					Port: 6379,
				},
			},
		}
		assert.NoError(t, store.Validate())
	}
	{
		// Nil redis config
		store := &Store{
			config: config.Config{
				Redis: nil,
			},
		}
		assert.ErrorContains(t, store.Validate(), "redis config is nil")
	}
	{
		// Empty host
		store := &Store{
			config: config.Config{
				Redis: &config.Redis{
					Host: "",
					Port: 6379,
				},
			},
		}
		assert.ErrorContains(t, store.Validate(), "redis host is empty")
	}
	{
		// Invalid port
		store := &Store{
			config: config.Config{
				Redis: &config.Redis{
					Host: "localhost",
					Port: 0,
				},
			},
		}
		assert.ErrorContains(t, store.Validate(), "invalid redis port")
	}
	{
		// Negative database
		store := &Store{
			config: config.Config{
				Redis: &config.Redis{
					Host:     "localhost",
					Port:     6379,
					Database: -1,
				},
			},
		}
		assert.ErrorContains(t, store.Validate(), "invalid redis database")
	}
}

func TestStore_IdentifierFor(t *testing.T) {
	store := &Store{
		config:    config.Config{},
		configMap: &types.DestinationTableConfigMap{},
	}

	topicConfig := kafkalib.DatabaseAndSchemaPair{
		Database: "mydb",
		Schema:   "myschema",
	}

	tableID := store.IdentifierFor(topicConfig, "mytable")

	redisTableID, ok := tableID.(TableIdentifier)
	assert.True(t, ok)
	assert.Equal(t, "mydb", redisTableID.Database())
	assert.Equal(t, "myschema", redisTableID.Schema())
	assert.Equal(t, "mytable", redisTableID.Table())
	assert.Equal(t, "mydb:myschema:mytable", redisTableID.FullyQualifiedName())
}

func TestStore_Dialect(t *testing.T) {
	store := &Store{}
	// Redis doesn't use SQL dialects
	assert.Nil(t, store.Dialect())
}

func TestStore_Dedupe(t *testing.T) {
	store := &Store{}
	err := store.Dedupe(context.TODO(), nil, nil, false)
	assert.Error(t, err)
	assert.ErrorContains(t, err, "dedupe is not supported for Redis")
}

func TestStore_SweepTemporaryTables(t *testing.T) {
	store := &Store{}
	// Should return nil as Redis doesn't have temp tables to sweep
	err := store.SweepTemporaryTables(context.TODO(), nil)
	assert.NoError(t, err)
}

func TestStore_ExecContext(t *testing.T) {
	store := &Store{}
	result, err := store.ExecContext(context.TODO(), "SELECT 1")
	assert.Nil(t, result)
	assert.Error(t, err)
	assert.ErrorContains(t, err, "ExecContext is not supported for Redis")
}

func TestStore_QueryContext(t *testing.T) {
	store := &Store{}
	rows, err := store.QueryContext(context.TODO(), "SELECT 1")
	assert.Nil(t, rows)
	assert.Error(t, err)
	assert.ErrorContains(t, err, "QueryContext is not supported for Redis")
}

func TestStore_Begin(t *testing.T) {
	store := &Store{}
	tx, err := store.Begin()
	assert.Nil(t, tx)
	assert.Error(t, err)
	assert.ErrorContains(t, err, "transactions are not supported for Redis")
}

func TestStore_LoadDataIntoTable(t *testing.T) {
	store := &Store{}
	err := store.LoadDataIntoTable(context.TODO(), nil, nil, nil, nil, types.AdditionalSettings{}, false)
	assert.Error(t, err)
	assert.ErrorContains(t, err, "LoadDataIntoTable is not supported for Redis")
}

func TestStore_GetTableConfig(t *testing.T) {
	store := &Store{
		configMap: &types.DestinationTableConfigMap{},
	}

	tableID := NewTableIdentifier("mydb", "myschema", "mytable")

	// First call should create a new table config
	tableConfig, err := store.GetTableConfig(context.TODO(), tableID, false)
	assert.NoError(t, err)
	assert.NotNil(t, tableConfig)

	// Second call should return the same config
	tableConfig2, err := store.GetTableConfig(context.TODO(), tableID, false)
	assert.NoError(t, err)
	assert.Equal(t, tableConfig, tableConfig2)
}

func TestIsRetryableError(t *testing.T) {
	store := &Store{}

	// Nil error
	assert.False(t, store.IsRetryableError(nil))

	// Connection pool timeout
	assert.True(t, store.IsRetryableError(errors.New("redis: connection pool timeout")))

	// I/O timeout
	assert.True(t, store.IsRetryableError(errors.New("i/o timeout")))

	// Random non-retryable error
	assert.False(t, store.IsRetryableError(errors.New("some random error")))

	// Wrapped timeout error
	assert.True(t, store.IsRetryableError(fmt.Errorf("failed to execute: %w", errors.New("connection pool timeout"))))
}

func TestIsRedisRetryableError(t *testing.T) {
	{
		// BUSY error
		err := errors.New("BUSY Redis is busy running a script")
		assert.True(t, isRedisRetryableError(err))
	}
	{
		// Random error
		err := errors.New("some random error")
		assert.False(t, isRedisRetryableError(err))
	}
}

func TestTableIdentifier_OnlyTable(t *testing.T) {
	ti := NewTableIdentifier("", "", "users")

	assert.Equal(t, "", ti.Database())
	assert.Equal(t, "", ti.Schema())
	assert.Equal(t, "users", ti.Table())
	assert.Equal(t, "users", ti.FullyQualifiedName())
	assert.Equal(t, "users", ti.StreamKey())
}
