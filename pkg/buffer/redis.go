package buffer

import (
	"bytes"
	"context"
	"data2parquet/pkg/config"
	"data2parquet/pkg/domain"
	"fmt"
	"log/slog"
	"time"

	"github.com/go-redis/redis/v8"
)

// / Redis buffer
// / @struct Redis
// / @implements Buffer
type Redis struct {
	config *config.Config
	client *redis.Client
	ctx    context.Context
}

// / New redis buffer
// / @param config *config.Config
// / @return Buffer
func NewRedis(ctx context.Context, config *config.Config) Buffer {
	ret := &Redis{
		config: config,
		ctx:    ctx,
	}

	ret.client = createClient(config)

	if !ret.IsReady() {
		slog.Error("Redis is not ready", "module", "buffer", "function", "NewRedis")
		return nil
	}

	slog.Debug("Connected to redis", "module", "buffer", "function", "NewRedis")

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

func (r *Redis) makeRecoveryKey(key string) string {
	return fmt.Sprintf("%s:%s", r.config.RedisRecoveryKey, key)
}

func (r *Redis) makeDLQKey(key string) string {
	return fmt.Sprintf("%s:%s", r.config.RedisDLQPrefix, key)
}

func (r *Redis) Len(key string) int {
	cmd := r.client.LLen(r.ctx, r.makeDataKey(key))

	if cmd.Err() != nil {
		slog.Error("Error getting key length", "error", cmd.Err())
		return 0
	}

	return int(cmd.Val())
}

func (r *Redis) Push(key string, item domain.Record) error {
	return r.pushRedis(r.makeDataKey(key), item.ToMsgPack())
}

func (r *Redis) PushRecovery(key string, item domain.Record) error {
	return r.pushRedis(r.makeRecoveryKey(key), item.ToMsgPack())
}

func (r *Redis) pushRedis(key string, data []byte) error {
	ctx := r.ctx
	sadd := r.client.SAdd(ctx, r.config.RedisKeys, key)

	if sadd.Err() != nil {
		slog.Error("Error adding key", "error", sadd.Err())
		return sadd.Err()
	}

	lpush := r.client.RPush(ctx, key, data)

	if lpush.Err() != nil {
		slog.Error("Error pushing to key", "error", lpush.Err())
		return lpush.Err()
	}

	return nil
}

func (r *Redis) RecoveryData() error {
	ctx := r.ctx

	keys := r.client.Keys(ctx, r.makeRecoveryKey("*"))

	if keys.Err() != nil {
		slog.Error("Error getting keys", "error", keys.Err())
		return keys.Err()
	}

	for _, key := range keys.Val() {
		result := r.client.LRange(ctx, key, 0, -1)

		if result.Err() != nil {
			slog.Error("Error getting key", "error", result.Err())
			return result.Err()
		}

		vals := result.Val()

		for _, v := range vals {
			record := domain.NewObj(r.config.RecordType)
			err := record.FromMsgPack([]byte(v))
			if err != nil {
				slog.Error("Error decoding record", "error", err, "module", "buffer.redis", "function", "RecoveryData")
				return err
			}

			err = r.Push(record.Key(), record)
			if err != nil {
				slog.Error("Error pushing record", "error", err, "module", "buffer.redis", "function", "RecoveryData", "record", record.ToString())
				return err
			}
		}

		popRet := r.client.LPopCount(ctx, key, len(vals))

		if popRet.Err() != nil {
			slog.Error("Error deleting key", "error", popRet.Err())
			return popRet.Err()
		}
	}

	return nil
}

func (r *Redis) Get(key string) []domain.Record {
	rkey := r.makeDataKey(key)
	if r.config.RedisSkipFlush {
		slog.Info("Skipping buffer get", "key", key, "module", "buffer.redis", "function", "Get")
		return make([]domain.Record, 0)
	}

	ctx := r.ctx
	cmd := r.client.LLen(ctx, rkey)

	if cmd.Err() != nil {
		slog.Error("Error getting key", "error", cmd.Err())
		return nil
	}

	size := cmd.Val()

	result := r.client.LRange(ctx, rkey, 0, size-1)

	if result.Err() != nil {
		slog.Error("Error getting key", "error", result.Err())
		return nil
	}

	ret := make([]domain.Record, size)

	var err error
	for i, v := range result.Val() {
		r := domain.NewObj(r.config.RecordType)
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
	rkey := r.makeDataKey(key)
	if r.config.RedisSkipFlush {
		slog.Debug("Skipping buffer clear", "key", key, "module", "buffer.redis", "function", "Clear")
		return nil
	}

	cmd := r.client.LPopCount(r.ctx, rkey, size)

	if cmd.Err() != nil {
		slog.Error("Error clearing key", "error", cmd.Err())
		return cmd.Err()
	}

	slog.Debug("Cleared buffer", "key", key, "size", size, "module", "buffer.redis", "function", "Clear")
	return nil
}

func (r *Redis) Keys() []string {
	cmd := r.client.SMembers(r.ctx, r.config.RedisKeys)

	if cmd.Err() != nil {
		slog.Error("Error getting keys", "error", cmd.Err())
		return []string{}
	}

	keys := cmd.Val()

	slog.Debug("Got keys", "keys", keys, "module", "buffer.redis", "function", "Keys")

	return keys
}

func (r *Redis) IsReady() bool {
	cmd := r.client.Ping(r.ctx)

	if cmd.Err() != nil {
		slog.Error("Error pinging redis", "error", cmd.Err())
		return false
	}

	return true
}

func (r *Redis) HasRecovery() bool {
	cmd := r.client.Keys(r.ctx, r.makeRecoveryKey("*"))

	if cmd.Err() != nil {
		slog.Error("Error getting keys", "error", cmd.Err())
		return false
	}

	return len(cmd.Val()) > 0
}

func (r *Redis) PushDLQ(key string, buf *bytes.Buffer) error {
	slog.Debug("Pushing to DLQ", "key", key, "module", "buffer.redis", "function", "PushDLQ", "size", buf.Len())
	data := &DLQData{
		Key:       key,
		Data:      buf.Bytes(),
		Timestamp: time.Now(),
	}
	return r.pushRedis(r.makeDLQKey(key), data.ToMsgPack())
}

func (r *Redis) GetDLQ() []*DLQData {
	keys := r.client.Keys(r.ctx, r.makeDLQKey("*"))

	if keys.Err() != nil {
		slog.Error("Error getting keys", "error", keys.Err())
		return []*DLQData{}
	}

	recKeys := keys.Val()
	ret := make([]*DLQData, len(recKeys))

	for i, key := range recKeys {
		result := r.client.LRange(r.ctx, key, 0, -1)

		if result.Err() != nil {
			slog.Error("Error getting key", "error", result.Err())
			return []*DLQData{}
		}

		vals := result.Val()
		item := &DLQData{}

		for _, v := range vals {
			err := item.FromMsgPack([]byte(v))

			if err != nil {
				slog.Error("Error decoding record", "error", err, "module", "buffer.redis", "function", "GetDLQ", "record", v)
			}

			ret[i] = item
		}
	}

	return ret
}

func (r *Redis) ClearDLQ() error {
	keys := r.client.Keys(r.ctx, r.makeDLQKey("*"))

	if keys.Err() != nil {
		slog.Error("Error getting keys", "error", keys.Err())
		return keys.Err()
	}

	recKeys := keys.Val()

	for _, key := range recKeys {
		popRet := r.client.Del(r.ctx, key)

		if popRet.Err() != nil {
			slog.Error("Error deleting key", "error", popRet.Err())
			return popRet.Err()
		}
	}

	return nil
}
