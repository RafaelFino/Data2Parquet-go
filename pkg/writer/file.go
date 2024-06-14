package writer

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"data2parquet/pkg/config"
	"data2parquet/pkg/domain"
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

	recInfo := domain.NewRecordInfoFromKey(f.config.RecordType, key)
	id := domain.MakeID()
	var hash = ""
	if f.config.UseHash {
		hash = "-" + domain.GetMD5Sum(buf.Bytes())
	}
	filePath := f.config.WriterFilePath + "/" + recInfo.Target(id, hash)

	err := os.MkdirAll(filepath.Dir(filePath), os.ModePerm)

	if err != nil {
		slog.Error("Error creating directory", "error", err, "key", key, "file", filePath)
		return err
	}

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
