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

const WriterTypeS3 = "aws-s3"
const WriterTypeFile = "file"

func New(ctx context.Context, config *config.Config) Writer {
	if ctx == nil {
		ctx = context.Background()
	}

	switch config.WriterType {
	case WriterTypeS3:
		return NewS3(ctx, config)
	case WriterTypeFile:
		return NewFile(ctx, config)

	default:
		return NewFile(ctx, config)
	}
}
