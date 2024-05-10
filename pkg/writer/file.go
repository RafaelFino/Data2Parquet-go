package writer

import (
	"data2parquet/pkg/config"
	"data2parquet/pkg/domain"
	"log/slog"
)

type File struct {
	config *config.Config
}

func NewFile(config *config.Config) Writer {
	return &File{
		config: config,
	}
}

func (f *File) Init() error {
	slog.Debug("[writer] Initializing file writer", "config", f.config.ToString())
	return nil
}

func (f *File) Write(data []domain.Record) error {
	slog.Debug("[writer] Writing logs", "data", data)
	return nil
}

func (f *File) Close() error {
	slog.Debug("[writer] Closing file writer")
	return nil
}
