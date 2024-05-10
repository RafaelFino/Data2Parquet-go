package buffer

import (
	"data2parquet/pkg/config"
	"data2parquet/pkg/domain"
)

type Redis struct {
	config *config.Config
}

func NewRedis(config *config.Config) Buffer {
	return &Redis{
		config: config,
	}
}

func (r *Redis) Push(key string, item domain.Record) error {
	return nil
}

func (r *Redis) Get(key string) []domain.Record {
	return nil
}

func (r *Redis) Clear(key string, size int) error {
	return nil
}

func (r *Redis) Keys() []string {
	return nil
}
