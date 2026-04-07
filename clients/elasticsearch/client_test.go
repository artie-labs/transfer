package elasticsearch

import (
	"errors"
	"testing"

	"github.com/artie-labs/transfer/lib/kafkalib"
	"github.com/stretchr/testify/assert"
)

func TestStore_IdentifierFor(t *testing.T) {
	store := &Store{}
	topicConfig := kafkalib.DatabaseAndSchemaPair{
		Database: "testdb",
		Schema:   "public",
	}

	identifier := store.IdentifierFor(topicConfig, "my_table")
	esIdent := identifier.(TableIdentifier)
	assert.Equal(t, "testdb_public_my_table", esIdent.Name())
}

func TestStore_IsRetryableError(t *testing.T) {
	store := &Store{}

	t.Run("nil error", func(t *testing.T) {
		assert.False(t, store.IsRetryableError(nil))
	})

	t.Run("timeout error", func(t *testing.T) {
		assert.True(t, store.IsRetryableError(errors.New("i/o timeout")))
	})

	t.Run("connection refused", func(t *testing.T) {
		assert.True(t, store.IsRetryableError(errors.New("dial tcp: connection refused")))
	})

	t.Run("too many requests", func(t *testing.T) {
		assert.True(t, store.IsRetryableError(errors.New("too many requests (429)")))
	})

	t.Run("non-retryable error", func(t *testing.T) {
		assert.False(t, store.IsRetryableError(errors.New("invalid index name")))
	})
}
