package buffer

import (
	"context"
	"data2parquet/pkg/config"
	"data2parquet/pkg/domain"
	"log/slog"

	"github.com/go-redis/redis/v8"
)

type Redis struct {
	config *config.Config
	client *redis.Client
}

func NewRedis(config *config.Config) Buffer {
	ret := &Redis{
		config: config,
	}

	ret.client = redis.NewClient(&redis.Options{
		Addr:     config.RedisHost,
		Password: config.RedisPassword,
		DB:       config.RedisDB,
	})

	if !ret.IsReady() {
		slog.Error("Redis is not ready", "module", "buffer", "function", "NewRedis")
		return nil
	}

	slog.Debug("Connected to redis", "module", "buffer", "function", "NewRedis")

	return ret
}

func (r *Redis) Push(key string, item domain.Record) error {
	r.client.LPush(context.Background(), key, item.ToJson())
	return nil
}

func (r *Redis) Get(key string) []domain.Record {
	cmd := r.client.LLen(context.Background(), key)

	if cmd.Err() != nil {
		slog.Error("Error getting key", "error", cmd.Err())
		return nil
	}

	size := cmd.Val()

	result := r.client.LRange(context.Background(), key, 0, size-1)

	if result.Err() != nil {
		slog.Error("Error getting key", "error", result.Err())
		return nil
	}

	ret := make([]domain.Record, size)

	for i, v := range result.Val() {
		r := &domain.Log{}
		r.FromJson(v)
		ret[i] = r
		slog.Debug("Getting buffer", "key", key, "module", "buffer.redis", "function", "Get", "record", r.ToString())
	}

	return ret
}

func (r *Redis) Clear(key string, size int) error {
	slog.Debug("Clearing buffer", "key", key, "size", size, "module", "buffer.redis", "function", "Clear")
	r.client.LTrim(context.Background(), key, 0, int64(size)-1)
	return nil
}

func (r *Redis) Keys() []string {
	slog.Debug("Getting keys", "module", "buffer.redis", "function", "Keys")
	cmd := r.client.Keys(context.Background(), "*")

	if cmd.Err() != nil {
		slog.Error("Error getting keys", "error", cmd.Err())
		return []string{}
	}

	return cmd.Val()
}

func (r *Redis) IsReady() bool {
	cmd := r.client.Ping(context.Background())

	if cmd.Err() != nil {
		slog.Error("Error pinging redis", "error", cmd.Err())
		return false
	}

	return true
}
