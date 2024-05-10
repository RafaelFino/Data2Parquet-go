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
	last    map[string]time.Time
}

func NewReceiver(config *config.Config) *Receiver {
	ret := &Receiver{
		config:  config,
		writer:  writer.NewWriter(config),
		buffer:  buffer.NewBuffer(config),
		running: true,
		last:    make(map[string]time.Time),
	}

	slog.Debug("Initializing receiver", "config", config.ToString(), "module", "receiver")

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
	slog.Debug("Writing record", "record", record.ToString(), "module", "receiver")
	r.buffer.Push(record.Key(), record)

	if _, ok := r.last[record.Key()]; !ok {
		if time.Since(r.last[record.Key()]) > time.Duration(r.config.FlushInterval)*time.Second {
			r.Flush()
			r.last[record.Key()] = time.Now()
		}
	} else {
		r.last[record.Key()] = time.Now()
	}
}

func (r *Receiver) Flush() {
	slog.Info("Flushing buffer", "module", "receiver")
	keys := r.buffer.Keys()
	for _, key := range keys {
		data := r.buffer.Get(key)

		slog.Debug("Flushing buffer", "key", key, "size", len(data), "module", "receiver")
		err := r.writer.Write(data)

		if err != nil {
			slog.Error("Error writing data", "error", err, "key", key, "size", len(data), "module", "receiver")
			continue
		}

		err = r.buffer.Clear(key, len(data))

		if err != nil {
			slog.Error("Error clearing buffer", "error", err, "key", key, "size", len(data), "module", "receiver")
			continue
		}
	}
}
func (r *Receiver) Close() error {
	slog.Info("Closing receiver", "module", "receiver")
	r.running = false
	return nil
}

func (r *Receiver) Healthcheck() error {
	slog.Debug("Healthcheck", "running", r.running, "module", "receiver")
	return nil
}
