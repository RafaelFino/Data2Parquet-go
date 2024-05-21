package writer

import (
	"bytes"
	"context"
	"data2parquet/pkg/config"
	"fmt"
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
	slog.Debug("Initializing file writer", "config", f.config.ToString())
	return nil
}

func (f *File) Write(key string, buf *bytes.Buffer) error {
	start := time.Now()

	filePath := f.makeFilePath(key)
	file, err := os.Create(filePath)
	if err != nil {
		slog.Error("Error creating file", "error", err, "key", key, "file", filePath)
		return err
	}

	defer file.Close()

	l, err := file.ReadFrom(buf)

	if err != nil {
		slog.Error("Error writing to file", "error", err, "key", key, "file", filePath)
		return err
	}

	slog.Info("File written", "key", key, "file", filePath, "duration", time.Since(start), "file-size", l)

	return err
}

func (f *File) Close() error {
	slog.Debug("Closing file writer")
	return nil
}

func (f *File) IsReady() bool {
	return true
}

func (f *File) makeFilePath(key string) string {
	tm := time.Now()
	year, month, day := tm.Date()
	hour, min, sec := tm.Clock()

	return fmt.Sprintf("%s/%d%02d%02d-%02d%02d%02d %s.parquet", f.config.WriterFilePath, year, month, day, hour, min, sec, key)
}
