package writer

import (
	"data2parquet/pkg/config"
	"data2parquet/pkg/domain"
	"io"
	"log/slog"

	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
)

type Writer interface {
	Init() error
	Write(data []domain.Record) error
	Close() error
}

func NewWriter(config *config.Config) Writer {
	switch config.WriterType {
	case "aws-s3":
		return NewS3(config)

	default:
		return NewFile(config)
	}
}

var CompressionTypeSnappy = "snappy"
var CompressionTypeGzip = "gzip"
var CompressionTypeNone = "none"

func WriteToFile(key string, data []domain.Record, w io.Writer, rowGroupSize int64, compressionType parquet.CompressionCodec) error {
	pw, err := writer.NewParquetWriterFromWriter(w, new(domain.Record), 4)
	if err != nil {
		slog.Error("Error creating parquet writer", "error", err, "module", "writer", "function", "writeToFile", "key", key)
		return err
	}

	defer pw.PFile.Close()

	pw.RowGroupSize = rowGroupSize
	pw.CompressionType = compressionType

	for _, record := range data {
		slog.Debug("Writing record", "record", record.ToString(), "module", "writer", "function", "writeToFile", "key", key)

		if err = pw.Write(record); err != nil {
			slog.Error("Error writing parquet file", "error", err, "module", "writer", "function", "writeToFile", "key", key, "record", record)
		}
	}

	if err = pw.WriteStop(); err != nil {
		slog.Error("Error writing parquet file", "error", err, "module", "writer", "function", "writeToFile", "key", key)
		return err
	}

	slog.Debug("Parquet file written", "key", key, "module", "writer", "function", "writeToFile")
	return err
}
