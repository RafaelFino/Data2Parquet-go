package writer

import (
	"data2parquet/pkg/config"
	"data2parquet/pkg/domain"
	"log/slog"
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
