package buffer

import (
	"context"
	"data2parquet/pkg/config"
	"data2parquet/pkg/domain"
	"fmt"
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

	ret.client = createClient(config)

	if !ret.IsReady() {
		slog.Error("Redis is not ready", "module", "buffer", "function", "NewRedis")
		return nil
	}

	slog.Debug("Connected to redis", "module", "buffer", "function", "NewRedis")

	return ret
}

func NewRedisWithClient(config *config.Config, client *redis.Client) Buffer {
	ret := &Redis{
		config: config,
		client: client,
	}

	if !ret.IsReady() {
		slog.Error("Redis is not ready", "module", "buffer", "function", "NewRedisWithClient")
		return nil
	}

	slog.Debug("Connected to redis", "module", "buffer", "function", "NewRedisWithClient")

	return ret
}

func (r *Redis) Close() error {
	if r.client != nil {
		err := r.client.Close()

		if err != nil {
			slog.Error("Error closing redis", "error", err)
			return err
		}
	}

	slog.Debug("Closed redis", "module", "buffer.redis", "function", "Close")
	return nil
}

func createClient(cfg *config.Config) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:     cfg.RedisHost,
		Password: cfg.RedisPassword,
		DB:       cfg.RedisDB,
	})
}

func (r *Redis) makeDataKey(key string) string {
	return fmt.Sprintf("%s:%s", r.config.RedisDataPrefix, key)
}

func (r *Redis) Len(key string) int {
	cmd := r.client.LLen(context.Background(), r.makeDataKey(key))

	if cmd.Err() != nil {
		slog.Error("Error getting key length", "error", cmd.Err())
		return 0
	}

	return int(cmd.Val())
}

func (r *Redis) Push(key string, item *domain.Record) error {
	rkey := r.makeDataKey(key)
	sadd := r.client.SAdd(context.Background(), r.config.RedisKeys, rkey)

	if sadd.Err() != nil {
		slog.Error("Error adding key", "error", sadd.Err())
		return sadd.Err()
	}

	lpush := r.client.LPush(context.Background(), rkey, item.ToMsgPack())

	if lpush.Err() != nil {
		slog.Error("Error pushing to key", "error", lpush.Err())
		return lpush.Err()
	}

	return nil
}

func (r *Redis) Get(key string) []*domain.Record {
	if r.config.RedisSkipFlush {
		slog.Info("Skipping buffer get", "key", key, "module", "buffer.redis", "function", "Get")
		return make([]*domain.Record, 0)
	}

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

	ret := make([]*domain.Record, size)

	var err error
	for i, v := range result.Val() {
		r := &domain.Record{}
		err = r.FromMsgPack([]byte(v))
		if err != nil {
			slog.Error("Error decoding record", "error", err, "module", "buffer.redis", "function", "Get", "record", v)
			return nil
		}
		ret[i] = r
	}

	slog.Debug("Got buffer", "key", key, "size", size, "module", "buffer.redis", "function", "Get", "records", len(ret))

	return ret
}

func (r *Redis) Clear(key string, size int) error {
	if r.config.RedisSkipFlush {
		slog.Info("Skipping buffer clear", "key", key, "module", "buffer.redis", "function", "Clear")
		return nil
	}

	cmd := r.client.LPopCount(context.Background(), key, size)

	if cmd.Err() != nil {
		slog.Error("Error clearing key", "error", cmd.Err())
		return cmd.Err()
	}

	slog.Debug("Cleared buffer", "key", key, "size", size, "module", "buffer.redis", "function", "Clear")
	return nil
}

func (r *Redis) Keys() []string {
	cmd := r.client.SMembers(context.Background(), r.config.RedisKeys)

	if cmd.Err() != nil {
		slog.Error("Error getting keys", "error", cmd.Err())
		return []string{}
	}

	keys := cmd.Val()

	slog.Info("Got keys", "keys", keys, "module", "buffer.redis", "function", "Keys")

	return keys
}

func (r *Redis) IsReady() bool {
	cmd := r.client.Ping(context.Background())

	if cmd.Err() != nil {
		slog.Error("Error pinging redis", "error", cmd.Err())
		return false
	}

	return true
}
