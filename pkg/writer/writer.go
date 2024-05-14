package writer

import (
	"context"
	"data2parquet/pkg/config"
	"data2parquet/pkg/domain"
	"io"
	"log/slog"

	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
)

// / Writer interface
// / @interface Writer
type Writer interface {
	Init() error
	Write(data []*domain.Record) []*WriterReturn
	Close() error
	IsReady() bool
}

type WriterReturn struct {
	Error   error
	Records []*domain.Record
}

// / New writer
// / @param config *config.Config
// / @return Writer
func New(ctx context.Context, config *config.Config) Writer {
	if ctx == nil {
		ctx = context.Background()
	}

	switch config.WriterType {
	case "aws-s3":
		return NewS3(ctx, config)
	case "file":
		return NewFile(config)

	default:
		return NewNone(config)
	}
}

// / Compression types
var CompressionTypeSnappy = "snappy"
var CompressionTypeGzip = "gzip"
var CompressionTypeNone = "none"

func GetCompressionType(compressionType string) parquet.CompressionCodec {
	switch compressionType {
	case CompressionTypeSnappy:
		return parquet.CompressionCodec_SNAPPY
	case CompressionTypeGzip:
		return parquet.CompressionCodec_GZIP
	case CompressionTypeNone:
		return parquet.CompressionCodec_UNCOMPRESSED
	default:
		return parquet.CompressionCodec_SNAPPY
	}
}

func WriteParquet(key string, data []*domain.Record, w io.Writer, rowGroupSize int64, compressionType parquet.CompressionCodec) []*WriterReturn {
	ret := make([]*WriterReturn, 0)
	pw, err := writer.NewParquetWriterFromWriter(w, new(domain.Record), 4)
	if err != nil {
		slog.Error("Error creating parquet writer", "error", err, "module", "writer", "function", "writeToFile", "key", key)
		ret = append(ret, &WriterReturn{Error: err})
		return ret
	}

	defer pw.PFile.Close()

	pw.RowGroupSize = rowGroupSize
	pw.CompressionType = compressionType

	for _, record := range data {
		if err = pw.Write(record); err != nil {
			slog.Error("Error writing parquet file", "error", err, "module", "writer", "function", "writeToFile", "key", key, "record", record)

			ret = append(ret, &WriterReturn{Error: err, Records: []*domain.Record{record}})
		}
	}

	slog.Debug("Stopping parquet writer", "module", "writer", "function", "writeToFile", "key", key)
	err = pw.WriteStop()

	if err != nil {
		slog.Error("Error to try stop parquet writer", "error", err, "module", "writer", "function", "writeToFile", "key", key)
		ret = append(ret, &WriterReturn{Error: err})
		return ret
	}

	slog.Debug("Parquet file written", "key", key, "module", "writer", "function", "writeToFile")
	return ret
}

func (w *WriterReturn) IsError() bool {
	return w.Error != nil
}

func CheckWriterError(w []*WriterReturn) bool {
	if w == nil {
		return false
	}

	for _, v := range w {
		if v.IsError() {
			return true
		}
	}

	return false
}
