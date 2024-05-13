package buffer

import (
	"data2parquet/pkg/config"
	"data2parquet/pkg/domain"
)

// / Buffer interface
type Buffer interface {
	Close() error
	Push(key string, item *domain.Record) error
	Get(key string) []*domain.Record
	Clear(key string, size int) error
	Len(key string) int
	Keys() []string
	IsReady() bool
}

// / New buffer
// / @param config *config.Config
// / @return Buffer
func New(config *config.Config) Buffer {
	switch config.BufferType {
	case "redis":
		return NewRedis(config)
	default:
		return NewMem(config)
	}
}
