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
	slog.Debug("Initializing file writer", "config", f.config.ToString(), "module", "writer.file", "function", "Init")
	return nil
}

func (f *File) Write(data []domain.Record) error {
	slog.Debug("Writing logs", "data", data, "module", "writer.file", "function", "Write")
	return nil
}

func (f *File) Close() error {
	slog.Debug("Closing file writer", "module", "writer.file", "function", "Close")
	return nil
}
