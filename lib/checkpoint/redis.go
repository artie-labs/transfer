package checkpoint

import (
	"context"
	"errors"
	"github.com/artie-labs/transfer/lib/config"
	"github.com/go-redis/redis/v9"
	"time"
)

var redisClient *redis.Client

func Get(ctx context.Context, key string) (string, error) {
	return redisClient.Get(ctx, key).Result()
}

func SetTTL(ctx context.Context, key string, value interface{}, td time.Duration) error {
	return redisClient.SetEx(ctx, key, value, td).Err()
}

func StartRedisClient(config *config.Config) error {
	if redisClient != nil {
		return nil
	}

	if config == nil {
		return errors.New("config is empty")
	}

	redisClient = redis.NewClient(&redis.Options{
		Addr:     config.Redis.Address,
		Password: config.Redis.Password,
		DB:       config.Redis.Database,
	})

	return nil
}
