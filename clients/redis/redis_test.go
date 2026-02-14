package redis

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/artie-labs/transfer/lib/config"
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
		config: cfg,
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
		config: config.Config{},
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
