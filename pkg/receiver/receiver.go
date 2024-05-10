package receiver

import (
	"data2parquet/pkg/buffer"
	"data2parquet/pkg/config"
	"data2parquet/pkg/domain"
	"data2parquet/pkg/writer"
	"log/slog"
	"time"
)

type Receiver struct {
	config  *config.Config
	writer  writer.Writer
	buffer  buffer.Buffer
	running bool
}

func NewReceiver(config *config.Config) *Receiver {
	ret := &Receiver{
		config:  config,
		writer:  writer.NewWriter(config),
		buffer:  buffer.NewBuffer(config),
		running: true,
	}

	slog.Debug("[receiver] Initializing receiver", "config", config.ToString())

	ret.writer.Init()

	go func(rcv *Receiver) {
		for {
			if !rcv.running {
				break
			}
			rcv.Flush()
			<-time.After(time.Duration(rcv.config.FlushInterval) * time.Second)
		}
	}(ret)

	return ret
}

func (r *Receiver) Write(record domain.Record) {
	slog.Debug("[receiver] Writing record", "record", record.ToString())
	r.buffer.Push(record.Key(), record)
}

func (r *Receiver) Flush() {
	slog.Info("[receiver] Flushing buffer")
	keys := r.buffer.Keys()
	for _, key := range keys {
		data := r.buffer.Get(key, r.config.BufferSize)

		slog.Debug("[receiver] Flushing buffer", "key", key, "size", len(data))
		r.writer.Write(data)
	}
}
func (r *Receiver) Close() error {
	slog.Info("[receiver] Closing receiver")
	r.running = false
	return nil
}

func (r *Receiver) Healthcheck() error {
	slog.Debug("[receiver] Healthcheck", "running", r.running)
	return nil
}
