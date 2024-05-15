package parquet

import (
	"data2parquet/pkg/config"
	"data2parquet/pkg/domain"
	"io"
	"log/slog"

	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
)

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

type Result struct {
	Key     string
	Error   error
	Records []domain.Record
}

type Converter struct {
	config          *config.Config
	compressionType parquet.CompressionCodec
	rowGroupSize    int64
}

func New(config *config.Config) *Converter {
	return &Converter{
		config:          config,
		compressionType: GetCompressionType(config.WriterCompressionType),
		rowGroupSize:    config.WriterRowGroupSize,
	}
}

func (c *Converter) Write(key string, data []domain.Record, w io.Writer) []*Result {
	ret := make([]*Result, 0)
	if data == nil {
		slog.Debug("No data to write", "module", "writer", "function", "writeToFile", "key", key)
		return ret
	}

	if len(data) == 0 {
		slog.Debug("No data to write", "module", "writer", "function", "writeToFile", "key", key)
		return ret
	}

	pw, err := writer.NewParquetWriterFromWriter(w, domain.NewObj(c.config.RecordType), 4)
	if err != nil {
		slog.Error("Error creating parquet writer", "error", err, "module", "writer", "function", "writeToFile", "key", key)
		ret = append(ret, &Result{Error: err})
		return ret
	}

	defer pw.PFile.Close()

	pw.RowGroupSize = c.rowGroupSize
	pw.CompressionType = c.compressionType

	for _, record := range data {
		if err = pw.Write(record); err != nil {
			slog.Error("Error writing parquet file", "error", err, "module", "writer", "function", "writeToFile", "key", key, "record", record)

			ret = append(ret, &Result{Error: err, Records: []domain.Record{record}})
		}
	}

	slog.Debug("Stopping parquet writer", "module", "writer", "function", "writeToFile", "key", key)
	err = pw.WriteStop()

	if err != nil {
		slog.Error("Error to try stop parquet writer", "error", err, "module", "writer", "function", "writeToFile", "key", key)
		ret = append(ret, &Result{Error: err})
		return ret
	}

	slog.Debug("Parquet file written", "key", key, "module", "writer", "function", "writeToFile")
	return ret
}

func (w *Result) IsError() bool {
	if w == nil {
		return false
	}

	return w.Error != nil
}

func CheckWriterError(w []*Result) bool {
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
