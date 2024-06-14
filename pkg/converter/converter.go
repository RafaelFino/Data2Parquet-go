package converter

import (
	"data2parquet/pkg/config"
	"data2parquet/pkg/domain"
	"data2parquet/pkg/logger" //"log/slog"
	"io"
	"os"

	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
)

var slog = logger.GetLogger()

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
	Key    string
	Error  error
	Record domain.Record
}

type Converter struct {
	config          *config.Config
	compressionType parquet.CompressionCodec
	rowGroupSize    int64
	recordType      string
	jsonSchemaPath  string
	jsonSchemaData  string
	np              int64
}

func New(cfg *config.Config) *Converter {
	ret := &Converter{
		config:          cfg,
		compressionType: GetCompressionType(cfg.WriterCompressionType),
		rowGroupSize:    cfg.WriterRowGroupSize,
		recordType:      cfg.RecordType,
		jsonSchemaPath:  cfg.JsonSchemaPath,
		np:              4,
	}

	if cfg.RecordType == config.RecordTypeDynamic && len(cfg.JsonSchemaPath) != 0 {
		err := ret.loadJsonSchema()

		if err != nil {
			slog.Error("Error loading json schema", "error", err, "module", "converter", "function", "New")
		} else {
			slog.Info("Json schema loaded", "module", "converter", "function", "New", "path", cfg.JsonSchemaPath, "schema", ret.jsonSchemaData)
		}
	}

	return ret
}

func (c *Converter) loadJsonSchema() error {
	if len(c.jsonSchemaPath) == 0 {
		return nil
	}

	data, err := os.ReadFile(c.jsonSchemaPath)

	if err != nil {
		slog.Error("Error reading json schema", "error", err, "module", "converter", "function", "loadJsonSchema", "path", c.jsonSchemaPath)
		return err
	}

	c.jsonSchemaData = string(data)
	slog.Debug("Json schema loaded", "module", "converter", "function", "loadJsonSchema", "path", c.jsonSchemaPath, "schema", c.jsonSchemaData)

	return nil
}

func (c *Converter) createParquetWriter(w io.Writer) (*writer.ParquetWriter, error) {
	var pw *writer.ParquetWriter
	var err error

	if c.config.RecordType == config.RecordTypeDynamic {
		pw, err = writer.NewParquetWriterFromWriter(w, c.jsonSchemaData, c.np)
	} else {
		pw, err = writer.NewParquetWriterFromWriter(w, domain.NewObj(c.config.RecordType), c.np)
	}

	if err != nil {
		slog.Error("Error creating parquet writer", "error", err, "module", "converter", "function", "createParquetWriter", "recordType", c.config.RecordType, "jsonSchemaData", c.jsonSchemaData)
		return nil, err
	}

	pw.RowGroupSize = c.rowGroupSize
	pw.CompressionType = c.compressionType

	return pw, err
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

	pw, err := c.createParquetWriter(w)
	if err != nil {
		slog.Error("Error creating parquet writer", "error", err, "module", "writer", "function", "writeToFile", "key", key)
		ret = append(ret, &Result{Key: key, Error: err})
		return ret
	}

	defer pw.PFile.Close()

	for _, record := range data {
		if err = pw.Write(record); err != nil {
			slog.Error("Error writing parquet file", "error", err, "module", "writer", "function", "writeToFile", "key", key, "record", record.ToJson())

			ret = append(ret, &Result{Key: key, Error: err, Record: record})
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
