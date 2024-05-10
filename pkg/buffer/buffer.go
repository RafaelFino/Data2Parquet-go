package buffer

import (
	"data2parquet/pkg/config"
	"data2parquet/pkg/domain"
)

type Buffer interface {
	Push(key string, item domain.Record) error
	Get(key string, size int) []domain.Record
	Keys() []string
}

func NewBuffer(config *config.Config) Buffer {
	switch config.BufferType {
	case "redis":
		return NewRedis(config)
	default:
		return NewMem(config)
	}
}
