package writer

import (
	"data2parquet/pkg/config"
	"data2parquet/pkg/domain"
	"log/slog"
	"os"
	"sync"
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

	records := make(map[string][]*domain.Record)

	for _, record := range data {
		key := record.Key()
		if _, ok := records[key]; !ok {
			records[key] = make([]*domain.Record, 0, len(data))
		}

		records[key] = append(records[key], record)
	}

	slog.Debug("Data splitted, writing records to file", "module", "writer.file", "function", "Write", "records", len(data), "duration", time.Since(start))

	wg := &sync.WaitGroup{}
	wg.Add(len(records))
	results := make(chan *WriterReturn)

	for key, data := range records {
		go f.WriteRecord(key, data, wg, results)
	}

	slog.Debug("Waiting for file writes to complete", "module", "writer.file", "function", "Write", "duration", time.Since(start))
	wg.Wait()

	ret := make([]*WriterReturn, len(records))
	close(results)

	for i := 0; i < len(results); i++ {
		ret[i] = <-results
	}

	return ret
}

func (f *File) WriteRecord(key string, records []*domain.Record, wg *sync.WaitGroup, result chan *WriterReturn) {
	defer wg.Done()
	start := time.Now()

	filePath := f.config.WriterFilePath + "/" + key + ".parquet"

	file, err := os.Create(filePath)
	if err != nil {
		slog.Error("Error creating file", "error", err, "module", "writer.file", "function", "Write", "key", key)
		result <- &WriterReturn{Error: err}
		return
	}

	defer file.Close()

	parquetRet := WriteParquet(key, records, file, f.config.WriterRowGroupSize, f.compressionType)

	if CheckWriterError(parquetRet) {
		for _, r := range parquetRet {
			slog.Error("Error writing to file", "error", r.Error, "module", "writer.file", "function", "Write", "key", key)
			result <- r
		}
	}

	slog.Info("File written", "key", key, "module", "writer.file", "function", "WriteRecord", "filePath", filePath, "records", len(records), "duration", time.Since(start))
}

func (f *File) Close() error {
	slog.Debug("Closing file writer", "module", "writer.file", "function", "Close")
	return nil
}

func (f *File) IsReady() bool {
	return true
}
