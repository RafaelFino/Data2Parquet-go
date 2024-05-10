package writer

import (
	"data2parquet/pkg/config"
	"data2parquet/pkg/domain"
	"log/slog"
)

type S3 struct {
	config *config.Config
}

func NewS3(config *config.Config) Writer {
	return &S3{
		config: config,
	}
}

func (s *S3) Init() error {
	slog.Debug("[writer] Initializing S3 writer", "config", s.config.ToString())
	return nil
}

func (s *S3) Write(data []domain.Record) error {
	slog.Debug("[writer] Writing logs", "data", data)
	return nil
}

func (s *S3) Close() error {
	slog.Debug("[writer] Closing S3 writer")
	return nil
}
