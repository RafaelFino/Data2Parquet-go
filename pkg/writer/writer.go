package writer

import (
	"data2parquet/pkg/config"
	"data2parquet/pkg/domain"
)

type Writer interface {
	Init() error
	Write(data []domain.Record) error
	Close() error
}

func NewWriter(config *config.Config) Writer {
	switch config.WriterType {
	case "aws-s3":
		return NewS3(config)

	default:
		return NewFile(config)
	}
}
