package writer

import (
	"data2parquet/pkg/config"
	"data2parquet/pkg/domain"
)

type Writer interface {
	Init(config *config.Config) error
	Write(data []domain.Log) error
	Close() error
}
