package buffer

import (
	"context"
	"data2parquet/pkg/config"
	"data2parquet/pkg/domain"
)

// / Buffer interface
type Buffer interface {
	Close() error
	Push(key string, item *domain.Record) error
	PushRecovery(key string, item *domain.Record) error
	RecoveryData() error
	Get(key string) []*domain.Record
	Clear(key string, size int) error
	Len(key string) int
	Keys() []string
	IsReady() bool
}

// / New buffer
// / @param config *config.Config
// / @return Buffer
func New(ctx context.Context, config *config.Config) Buffer {
	switch config.BufferType {
	case "redis":
		return NewRedis(ctx, config)
	default:
		return NewMem(ctx, config)
	}
}
