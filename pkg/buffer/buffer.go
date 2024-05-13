package buffer

import (
	"data2parquet/pkg/config"
	"data2parquet/pkg/domain"
)

type Buffer interface {
	Close() error
	Push(key string, item *domain.Record) error
	Get(key string) []*domain.Record
	Clear(key string, size int) error
	Len(key string) int
	Keys() []string
	IsReady() bool
}

func New(config *config.Config) Buffer {
	switch config.BufferType {
	case "redis":
		return NewRedis(config)
	case "ledis":
		return NewLedis(config)
	case "mem":
		return NewMem(config)
	default:
		return NewMem(config)
	}
}
