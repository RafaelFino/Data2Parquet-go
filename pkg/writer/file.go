package writer

import (
	"log/slog"
	"log2parquet/pkg/config"
	"log2parquet/pkg/domain"
)

type File struct {
}

func (f *File) Init(config *config.Config) error {
	slog.Debug("[writer] Initializing file writer", "config", config.ToString())
	return nil
}

func (f *File) Write(data []domain.Log) error {
	slog.Debug("[writer] Writing logs", "data", data)
	return nil
}

func (f *File) Close() error {
	slog.Debug("[writer] Closing file writer")
	return nil
}
