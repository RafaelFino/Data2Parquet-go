package writer

import (
	"context"
	"data2parquet/pkg/config"
	"io"
)

type Writer interface {
	Init() error
	Write(key string, buf io.Reader) error
	Close() error
	IsReady() bool
}

func New(ctx context.Context, config *config.Config) Writer {
	if ctx == nil {
		ctx = context.Background()
	}

	switch config.WriterType {
	case "aws-s3":
		return NewS3(ctx, config)
	case "file":
		return NewFile(ctx, config)

	default:
		return NewFile(ctx, config)
	}
}
