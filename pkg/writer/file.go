package writer

import (
	"context"
	"data2parquet/pkg/config"
	"data2parquet/pkg/domain"
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

func (f *File) Write(key string, data []*domain.Record) error {
	start := time.Now()

	slog.Debug("Data splitted, writing records to file", "module", "writer.file", "function", "Write", "records", len(data), "duration", time.Since(start))

	filePath := f.config.WriterFilePath + "/" + key + ".parquet"

	file, err := os.Create(filePath)
	if err != nil {
		slog.Error("Error creating file", "error", err, "module", "writer.file", "function", "Write", "key", key)
		return []*WriterReturn{{Error: err, Records: data}}
	}

	defer file.Close()

	parquetRet := WriteParquet(f.config, key, data, file, f.config.WriterRowGroupSize)

	if CheckWriterError(parquetRet) {
		for _, r := range parquetRet {
			slog.Error("Error writing to file", "error", r.Error, "module", "writer.file", "function", "Write", "key", key)
		}
	}

	slog.Info("File written", "key", key, "module", "writer.file", "function", "WriteRecord", "filePath", filePath, "records", len(data), "duration", time.Since(start))

	return parquetRet
}

func (f *File) Close() error {
	slog.Debug("Closing file writer", "module", "writer.file", "function", "Close")
	return nil
}

func (f *File) IsReady() bool {
	return true
}
