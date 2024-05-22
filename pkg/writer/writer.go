package writer

import (
	"bytes"
	"context"
	"data2parquet/pkg/config"
)

type Writer interface {
	Init() error
	Write(key string, buf *bytes.Buffer) error
	Close() error
	IsReady() bool
}

func New(ctx context.Context, cfg *config.Config) Writer {
	if ctx == nil {
		ctx = context.Background()
	}

	switch cfg.WriterType {
	case config.WriterTypeAWSS3:
		return NewS3(ctx, cfg)
	case config.WriterTypeFile:
		return NewFile(ctx, cfg)

	default:
		return NewFile(ctx, cfg)
	}
}
