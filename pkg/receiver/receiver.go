package receiver

import (
	"data2parquet/pkg/buffer"
	"data2parquet/pkg/config"
	"data2parquet/pkg/domain"
	"data2parquet/pkg/writer"
	"log/slog"
)

type Receiver struct {
	config *config.Config
	writer writer.Writer
	buffer buffer.Buffer
}

func NewReceiver(config *config.Config) *Receiver {
	ret := &Receiver{
		config: config,
		writer: writer.NewWriter(config),
		buffer: buffer.NewBuffer(config),
	}

	go func() {
		for {
			keys := ret.buffer.Keys()
			for _, key := range keys {
				records := ret.buffer.Get(key, config.BufferPageSize)
				ret.writer.Write(records)
			}
		}
	}()

	return ret
}

func (r *Receiver) Write(record domain.Record) {
	slog.Debug("[receiver] Writing record", "record", record.ToString())
	r.buffer.Push(record.Key(), &record)
}

func (r *Receiver) Close() error {
	slog.Debug("[receiver] Closing receiver")
	return nil
}

func (r *Receiver) Healthcheck() error {
	slog.Debug("[receiver] Healthcheck")
	return nil
}
