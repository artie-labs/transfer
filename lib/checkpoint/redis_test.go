package checkpoint

import (
	"context"
	"fmt"
	"github.com/artie-labs/transfer/lib/config"
	"io"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRedisErrValidation(t *testing.T) {
	assert.Error(t, StartRedisClient(nil))
}

func TestBasicRedisCmd(t *testing.T) {
	// Start the Redis client
	randomFilePath := fmt.Sprintf("/tmp/redis-%s", time.Now().String())
	file, err := os.Create(randomFilePath)
	assert.Nil(t, err)

	_, err = io.WriteString(file, fmt.Sprintf(`
redis:
 address: localhost:6379
`))

	assert.Nil(t, err)
	cfg, err := config.ReadFileToConfig(randomFilePath)
	assert.Nil(t, err, "Reading config")

	// Starting Redis

	assert.Nil(t, StartRedisClient(cfg), "Starting Redis")

	// When the key does not exist
	key := "foo"
	ctx := context.Background()
	val, err := Get(ctx, key)
	assert.NotNil(t, err) // No value
	assert.Equal(t, val, "")

	expectedVal := "bar"
	// When the key does exist.
	assert.Nil(t, SetTTL(ctx, key, expectedVal, 5*time.Second))

	val, err = Get(ctx, key)
	assert.Nil(t, err)
	assert.Equal(t, val, expectedVal)
}
