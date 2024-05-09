package writer

import (
	"data2parquet/pkg/config"
	"data2parquet/pkg/domain"
	"log/slog"
)

type S3 struct {
}

func (s *S3) Init(config *config.Config) error {
	slog.Debug("[writer] Initializing S3 writer", "config", config.ToString())
	return nil
}

func (s *S3) Write(data []domain.Log) error {
	slog.Debug("[writer] Writing logs", "data", data)
	return nil
}

func (s *S3) Close() error {
	slog.Debug("[writer] Closing S3 writer")
	return nil
}
