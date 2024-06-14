package buffer

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/oklog/ulid"

	"data2parquet/pkg/config"
	"data2parquet/pkg/domain"
)

type Redis struct {
	config     *config.Config
	client     *redis.Client
	ctx        context.Context
	instanceId string
}

func NewRedis(ctx context.Context, config *config.Config, client *redis.Client) Buffer {
	ret := &Redis{
		config: config,
		ctx:    ctx,
	}

	if client != nil {
		ret.client = client
	} else {
		ret.client = createClient(config)
	}

	ret.makeInstanceName()

	if !ret.IsReady() {
		slog.Error("Redis is not ready", "module", "buffer", "function", "NewRedis")
		return nil
	}

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
		Addr:        cfg.RedisHost,
		Password:    cfg.RedisPassword,
		DB:          cfg.RedisDB,
		ReadTimeout: time.Duration(cfg.RedisTimeout),
	})
}

func (r *Redis) getClient() *redis.Client {
	if r.client == nil {
		slog.Info("Connecting to redis", "host", r.config.RedisHost, "db", r.config.RedisDB)
		r.client = createClient(r.config)

		slog.Debug("Connected to redis")
	}

	return r.client
}

func (r *Redis) makeInstanceName() {
	entropy := rand.New(rand.NewSource(time.Now().UnixNano()))
	r.instanceId = fmt.Sprintf("%s-%s", r.config.RedisLockInstanceName, ulid.MustNew(ulid.Timestamp(time.Now()), entropy).String())
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

func (r *Redis) makeLockKey(key string) string {
	return fmt.Sprintf("%s:%s", r.config.RedisLockPrefix, key)
}

func (r *Redis) Len(key string) int {
	client := r.getClient()
	cmd := client.LLen(r.ctx, r.makeDataKey(key))

	if cmd.Err() != nil {
		slog.Error("Error getting key length", "error", cmd.Err())
		return 0
	}

	return int(cmd.Val())
}

func (r *Redis) Push(key string, item domain.Record) (int, error) {
	if len(key) == 0 {
		slog.Warn("Key is empty", "module", "buffer.redis", "function", "Push")
		return 0, fmt.Errorf("key is empty") // Fix: Changed "Key" to "key"
	}

	if item == nil {
		slog.Warn("Item is nil", "module", "buffer.redis", "function", "Push")
		return 0, fmt.Errorf("item is nil") // Fix: Changed "Item" to "item"
	}

	client := r.getClient()
	sadd := client.SAdd(r.ctx, r.config.RedisKeys, key)

	if sadd.Err() != nil {
		slog.Error("Error adding key", "error", sadd.Err())
		return 0, sadd.Err()
	}

	rkey := r.makeDataKey(key)
	data := item.ToMsgPack()

	ret := client.RPush(r.ctx, rkey, data)

	if ret.Err() != nil {
		slog.Error("Error pushing to key", "error", ret.Err())
		return 0, ret.Err()
	}

	l := int(ret.Val())

	return l, nil
}

func (r *Redis) PushDLQ(key string, item domain.Record) error {
	rKey := r.makeDLQKey(key)

	msg := item.ToMsgPack()
	slog.Debug("Pushing to DLQ", "key", key, "module", "buffer.redis", "function", "PushDLQ", "size", len(msg))

	client := r.getClient()
	cmd := client.RPush(r.ctx, rKey, msg)

	if cmd.Err() != nil {
		slog.Error("Error pushing to DLQ", "error", cmd.Err())
		return cmd.Err()
	}

	return nil
}

func (r *Redis) GetDLQ() (map[string][]domain.Record, error) {
	ctx := r.ctx

	keys := r.client.Keys(ctx, r.makeDLQKey("*"))

	if keys.Err() != nil {
		slog.Error("GetDLQ - Error getting keys", "error", keys.Err())
		return nil, keys.Err()
	}

	ret := make(map[string][]domain.Record)
	client := r.getClient()

	for _, key := range keys.Val() {
		result := client.LRange(ctx, key, 0, -1)

		if result.Err() != nil {
			slog.Error("GetDLQ - Error getting key", "error", result.Err())
			return nil, result.Err()
		}

		vals := result.Val()

		items := make([]domain.Record, len(vals))

		for i, v := range vals {
			record := domain.NewObj(r.config.RecordType)
			err := record.FromMsgPack([]byte(v))
			if err != nil {
				slog.Error("Get DLQ - Error decoding record", "error", err, "module", "buffer.redis", "function", "RecoveryData")
				return nil, err
			}

			items[i] = record
		}

		ret[key] = append(ret[key], items...)
	}

	return ret, nil
}

func (r *Redis) CheckLock(key string) bool {
	lockKey := r.makeLockKey(key)
	ttl := time.Duration(r.config.RedisLockTTL) * time.Second
	client := r.getClient()

	createLock := client.SetNX(r.ctx, lockKey, r.instanceId, ttl)

	if createLock.Err() != nil {
		slog.Error("Error setting lock", "error", createLock.Err(), "key", key, "id", r.instanceId)
		return false
	}

	ret := createLock.Val()

	if ret {
		slog.Info("Lock created for this instance", "key", key, "id", r.instanceId, "ttl", ttl)
		return ret
	}

	currentLck := client.Get(r.ctx, lockKey)

	if currentLck.Err() != nil {
		slog.Error("Error getting lock", "error", currentLck.Err())
		return false
	}

	lockOwner := currentLck.Val()

	if lockOwner == r.instanceId {
		slog.Debug("Already locked for this instance, continue to use", "key", key, "id", r.instanceId)
		return true
	}

	slog.Debug("Buffer already locked by another instance", "key", key, "this-id", r.instanceId, "current-id", lockOwner, "CheckLock", ret)

	return ret
}

func (r *Redis) Get(key string) []domain.Record {
	rkey := r.makeDataKey(key)
	client := r.getClient()

	cmd := client.LLen(r.ctx, rkey)

	if cmd.Err() != nil {
		slog.Error("Error getting key", "error", cmd.Err())
		return make([]domain.Record, 0)
	}

	size := cmd.Val()

	if size > int64(r.config.BufferSize) {
		slog.Debug("Buffer size is bigger than config, will get partial data", "key", key, "buffer-size", size, "get-size", r.config.BufferSize)
		size = int64(r.config.BufferSize)
	}

	result := client.LRange(r.ctx, rkey, 0, size-1)

	if result.Err() != nil {
		slog.Error("Error getting key", "error", result.Err())
		return make([]domain.Record, 0)
	}

	ret := make([]domain.Record, size)

	var err error
	for i, v := range result.Val() {
		rec := domain.NewObj(r.config.RecordType)
		err = rec.FromMsgPack([]byte(v))
		if err != nil {
			slog.Error("Error decoding record", "error", err, "module", "buffer.redis", "function", "Get", "record", v)
			return ret
		}
		ret[i] = rec
	}

	slog.Debug("Got buffer", "key", key, "size", size, "records", len(ret))

	return ret
}

func (r *Redis) Clear(key string, size int) error {
	rkey := r.makeDataKey(key)
	client := r.getClient()

	cmd := client.LPopCount(r.ctx, rkey, size)

	if cmd.Err() != nil {
		slog.Error("Error clearing key", "error", cmd.Err())
		return cmd.Err()
	}

	slog.Debug("Cleared buffer", "key", key, "size", size, "module", "buffer.redis", "function", "Clear")
	return nil
}

func (r *Redis) Keys() []string {
	client := r.getClient()
	cmd := client.SMembers(r.ctx, r.config.RedisKeys)

	if cmd.Err() != nil {
		slog.Error("Error getting keys", "error", cmd.Err())
		return []string{}
	}

	keys := cmd.Val()

	slog.Debug("Got keys", "keys", keys, "module", "buffer.redis", "function", "Keys")

	return keys
}

func (r *Redis) IsReady() bool {
	client := r.getClient()
	cmd := client.Ping(r.ctx)

	if cmd.Err() != nil {
		slog.Error("Error pinging redis", "error", cmd.Err())
		r.client = nil
		return false
	}

	return true
}

func (r *Redis) HasRecovery() bool {
	client := r.getClient()
	cmd := client.Keys(r.ctx, r.makeRecoveryKey("*"))

	if cmd.Err() != nil {
		slog.Error("Error getting keys", "error", cmd.Err())
		return false
	}

	return len(cmd.Val()) > 0
}

func (r *Redis) PushRecovery(key string, buf *bytes.Buffer) error {
	slog.Debug("Pushing data to post recovery", "key", key, "module", "buffer.redis", "function", "PushRecovery", "size", buf.Len())
	data := &RecoveryData{
		Key:       key,
		Data:      buf.Bytes(),
		Timestamp: time.Now(),
	}

	if data.Data == nil {
		slog.Error("PushRecovery - No data to push", "key", key)
		return nil
	}
	client := r.getClient()

	cmd := client.LPush(r.ctx, r.makeRecoveryKey(key), data.ToMsgPack())

	if cmd.Err() != nil {
		slog.Error("PushRecovery - Error pushing to recovery", "error", cmd.Err())
		return cmd.Err()
	}

	return nil
}

func (r *Redis) GetRecovery() ([]*RecoveryData, error) {
	client := r.getClient()
	keys := client.Keys(r.ctx, r.makeRecoveryKey("*"))

	if keys.Err() != nil {
		slog.Error("GetRecovery - Error getting keys", "error", keys.Err())
		return []*RecoveryData{}, keys.Err()
	}

	recKeys := keys.Val()
	ret := make([]*RecoveryData, 0)

	for i, key := range recKeys {
		result := client.LRange(r.ctx, key, 0, -1)

		if result.Err() != nil {
			slog.Error("GetRecovery - Error getting key", "error", result.Err(), "key", key)
			return ret, result.Err()
		}

		vals := result.Val()
		item := &RecoveryData{}
		items := make([]*RecoveryData, len(vals))

		for _, v := range vals {
			err := item.FromMsgPack([]byte(v))

			if err != nil {
				slog.Error("Error decoding record", "error", err, "key", key)
				return ret, err
			}

			items[i] = item
		}

		ret = append(ret, items...)
	}

	return ret, nil
}

func (r *Redis) ClearRecoveryData() error {
	client := r.getClient()
	keys := client.Keys(r.ctx, r.makeRecoveryKey("*"))

	if keys.Err() != nil {
		slog.Error("ClearRecoveryData - Error getting keys", "error", keys.Err())
		return keys.Err()
	}

	recKeys := keys.Val()

	popRet := client.Del(r.ctx, recKeys...)

	if popRet.Err() != nil {
		slog.Error("Error deleting key from recovery data", "error", popRet.Err())
		return popRet.Err()
	}

	slog.Debug("Cleared recovery data", "module", "buffer.redis", "function", "ClearRecoveryData")

	return nil
}

func (r *Redis) ClearDLQ() error {
	client := r.getClient()
	keys := client.Keys(r.ctx, r.makeDLQKey("*"))

	if keys.Err() != nil {
		slog.Error("ClearDLQ - Error getting keys", "error", keys.Err())
		return keys.Err()
	}

	recKeys := keys.Val()

	popRet := client.Del(r.ctx, recKeys...)

	if popRet.Err() != nil {
		slog.Error("Error deleting key from DLQ", "error", popRet.Err())
		return popRet.Err()
	}

	slog.Debug("Cleared DLQ", "module", "buffer.redis", "function", "ClearDLQ")

	return nil
}
