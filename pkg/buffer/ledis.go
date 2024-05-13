package buffer

import (
	"data2parquet/pkg/config"
	"data2parquet/pkg/domain"
	"fmt"
	"log/slog"
	"os"

	"github.com/go-redis/redis/v8"
	lediscfg "github.com/ledisdb/ledisdb/config"
	"github.com/ledisdb/ledisdb/server"
)

type Ledis struct {
	config *config.Config
	client Buffer
}

func NewLedis(config *config.Config) Buffer {
	ret := &Ledis{
		config: config,
	}
	ret.client = NewRedisWithClient(config, createLedisClient(config))

	if !ret.IsReady() {
		slog.Error("Ledis is not ready", "module", "buffer", "function", "NewLedis")
		return nil
	}
	slog.Debug("Connected to ledis", "module", "buffer", "function", "NewLedis")
	return ret
}

func createLedisClient(config *config.Config) *redis.Client {
	if len(config.LedisPath) == 0 {
		slog.Error("Redis local path is not set", "module", "buffer.redis", "function", "createClient")
		return nil
	}
	slog.Debug("Creating redis client with local path", "path", config.LedisPath, "module", "buffer.redis", "function", "createClient")

	tmpDir, err := os.MkdirTemp(config.LedisPath, "ledisdb")

	if err != nil {
		slog.Error("Error creating temp dir", "error", err, "module", "buffer.redis", "function", "createClient")
		return nil
	}

	ledisCfg := lediscfg.NewConfigDefault()
	ledisCfg.Addr = "" // use auto-assigned address
	ledisCfg.DataDir = tmpDir

	app, err := server.NewApp(ledisCfg)
	if err != nil {
		slog.Error("Error creating ledisdb app", "error", err, "module", "buffer.redis", "function", "createClient")
		return nil
	}

	return redis.NewClient(&redis.Options{
		Addr: app.Address(),
	})
}

func (l *Ledis) Close() error {
	if l.client != nil {
		err := l.client.Close()

		if err != nil {
			slog.Error("Error closing ledis", "error", err)
			return err
		}
	}

	slog.Debug("Closed ledis", "module", "buffer.ledis", "function", "Close")
	return nil
}

func (l *Ledis) IsReady() bool {
	if l.client != nil {
		return l.client.IsReady()
	}

	return false
}

func (l *Ledis) Push(key string, item *domain.Record) error {
	if l.client != nil {
		return l.client.Push(key, item)
	}

	return fmt.Errorf("Ledis client is not ready")
}

func (l *Ledis) Get(key string) []*domain.Record {
	if l.client != nil {
		return l.client.Get(key)
	}

	return nil
}

func (l *Ledis) Clear(key string, size int) error {
	if l.client != nil {
		return l.client.Clear(key, size)
	}

	return fmt.Errorf("Ledis client is not ready")
}

func (l *Ledis) Len(key string) int {
	if l.client != nil {
		return l.client.Len(key)
	}

	return 0
}

func (l *Ledis) Keys() []string {
	if l.client != nil {
		return l.client.Keys()
	}

	return nil
}
