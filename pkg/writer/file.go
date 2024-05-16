package writer

import (
	"bytes"
	"context"
	"data2parquet/pkg/config"
	"log/slog"
	"os"
	"time"
)

type File struct {
	config *config.Config
	ctx    context.Context
}

func NewFile(ctx context.Context, config *config.Config) Writer {
	return &File{
		config: config,
		ctx:    ctx,
	}
}

func (f *File) Init() error {
	slog.Debug("Initializing file writer", "config", f.config.ToString(), "module", "writer.file", "function", "Init")
	return nil
}

func (f *File) Write(key string, buf *bytes.Buffer) error {
	start := time.Now()

	filePath := f.config.WriterFilePath + "/" + key + ".parquet"

	file, err := os.Create(filePath)
	if err != nil {
		slog.Error("Error creating file", "error", err, "module", "writer.file", "function", "Write", "key", key, "file", filePath)
		return err
	}

	defer file.Close()

	data := buf.Bytes()
	err = os.WriteFile(filePath, data, 0644)

	slog.Info("File written", "module", "writer.file", "function", "Write", "key", key, "file", filePath, "duration", time.Since(start), "file-size", len(data))

	return err
}

func (f *File) Close() error {
	slog.Debug("Closing file writer", "module", "writer.file", "function", "Close")
	return nil
}

func (f *File) IsReady() bool {
	return true
}
