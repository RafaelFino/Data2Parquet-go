package writer

import (
	"log2parquet/pkg/config"
	"log2parquet/pkg/domain"
)

type Writer interface {
	Init(config *config.Config) error
	Write(data []domain.Log) error
	Close() error
}
