package writer

import (
	"data2parquet/pkg/config"
	"data2parquet/pkg/domain"
	"log/slog"
	"os"

	"github.com/xitongsys/parquet-go/parquet"
)

type File struct {
	config          *config.Config
	compressionType parquet.CompressionCodec
}

func NewFile(config *config.Config) Writer {
	return &File{
		config: config,
	}
}

func (f *File) Init() error {
	slog.Debug("Initializing file writer", "config", f.config.ToString(), "module", "writer.file", "function", "Init")

	switch f.config.WriterCompressionType {
	case CompressionTypeSnappy:
		f.compressionType = parquet.CompressionCodec_SNAPPY
	case CompressionTypeGzip:
		f.compressionType = parquet.CompressionCodec_GZIP
	case CompressionTypeNone:
		f.compressionType = parquet.CompressionCodec_UNCOMPRESSED
	default:
		f.compressionType = parquet.CompressionCodec_SNAPPY
	}

	slog.Debug("Compression type", "compressionType", f.compressionType, "module", "writer.file", "function", "Init")

	return nil
}

func (f *File) Write(data []domain.Record) error {
	slog.Debug("Writing logs", "module", "writer.file", "function", "Write")

	records := make(map[string][]domain.Record)

	for _, record := range data {
		if _, ok := records[record.Key()]; !ok {
			records[record.Key()] = make([]domain.Record, 0, f.config.BufferSize)
		}

		records[record.Key()] = append(records[record.Key()], record)
	}

	for key, records := range records {
		filePath := f.config.WriterFilePath + "/" + key + ".parquet"

		file, err := os.Create(filePath)
		if err != nil {
			slog.Error("Error creating file", "error", err, "module", "writer.file", "function", "Write", "key", key)
			return err
		}

		defer file.Close()

		if err := WriteToFile(key, records, file, f.config.WriterRowGroupSize, f.compressionType); err != nil {
			slog.Error("Error writing to file", "error", err, "module", "writer.file", "function", "Write", "key", key)
			return err
		}

		slog.Info("File written", "key", key, "module", "writer.file", "function", "Write", "filePath", filePath)
	}

	return nil
}

func (f *File) Close() error {
	slog.Debug("Closing file writer", "module", "writer.file", "function", "Close")
	return nil
}
