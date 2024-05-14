package writer

import (
	"data2parquet/pkg/config"
	"data2parquet/pkg/domain"
	"log/slog"
	"os"
	"time"

	"github.com/xitongsys/parquet-go/parquet"
)

type File struct {
	config          *config.Config
	compressionType parquet.CompressionCodec
}

func NewFile(config *config.Config) Writer {
	return &File{
		config:          config,
		compressionType: GetCompressionType(config.WriterCompressionType),
	}
}

func (f *File) Init() error {
	slog.Debug("Initializing file writer", "config", f.config.ToString(), "module", "writer.file", "function", "Init")
	return nil
}

func (f *File) Write(data []*domain.Record) []*WriterReturn {
	start := time.Now()
	ret := make([]*WriterReturn, 0)

	records := make(map[string][]*domain.Record)

	for _, record := range data {
		if _, ok := records[record.Key()]; !ok {
			records[record.Key()] = make([]*domain.Record, 0, f.config.BufferSize)
		}

		records[record.Key()] = append(records[record.Key()], record)
	}

	for key, records := range records {
		filePath := f.config.WriterFilePath + "/" + key + ".parquet"

		file, err := os.Create(filePath)
		if err != nil {
			slog.Error("Error creating file", "error", err, "module", "writer.file", "function", "Write", "key", key)
			ret = append(ret, &WriterReturn{Error: err})
			return ret
		}

		defer file.Close()

		parquetRet := WriteParquet(key, records, file, f.config.WriterRowGroupSize, f.compressionType)

		if CheckWriterError(parquetRet) {
			for _, r := range parquetRet {
				slog.Error("Error writing to file", "error", r.Error, "module", "writer.file", "function", "Write", "key", key)
			}
			return parquetRet
		}

		slog.Info("File written", "key", key, "module", "writer.file", "function", "Write", "filePath", filePath, "records", len(records), "duration", time.Since(start))
	}

	return nil
}

func (f *File) Close() error {
	slog.Debug("Closing file writer", "module", "writer.file", "function", "Close")
	return nil
}

func (f *File) IsReady() bool {
	return true
}
