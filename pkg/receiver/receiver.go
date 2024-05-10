package receiver

import (
	"data2parquet/pkg/buffer"
	"data2parquet/pkg/config"
	"data2parquet/pkg/domain"
	"data2parquet/pkg/writer"
	"log/slog"
	"sync"
	"time"
)

type Receiver struct {
	config     *config.Config
	writer     writer.Writer
	buffer     buffer.Buffer
	running    bool
	last       map[string]time.Time
	mu         sync.Mutex
	stopSignal chan bool
}

func NewReceiver(config *config.Config) *Receiver {
	ret := &Receiver{
		config:     config,
		writer:     writer.NewWriter(config),
		buffer:     buffer.NewBuffer(config),
		running:    true,
		last:       make(map[string]time.Time),
		mu:         sync.Mutex{},
		stopSignal: make(chan bool),
	}

	slog.Debug("Initializing receiver", "config", config.ToString(), "module", "receiver", "function", "NewReceiver")

	ret.writer.Init()

	go func(rcv *Receiver) {
		select {
		case <-rcv.stopSignal:
			{
				slog.Info("Stopping receiver", "module", "receiver", "function", "NewReceiver")
				rcv.Flush(true)
				rcv.Close()
			}
		case <-time.After(time.Duration(rcv.config.FlushInterval) * time.Second):
			{
				if rcv.running {
					rcv.Flush(false)
				}
			}
		}
	}(ret)

	return ret
}

func (r *Receiver) Write(record domain.Record) error {
	slog.Debug("Writing record", "record", record.ToString(), "module", "receiver", "function", "Write")
	err := r.buffer.Push(record.Key(), record)

	if err != nil {
		slog.Error("Error pushing record", "error", err, "record", record.ToString(), "module", "receiver", "function", "Write")
	}

	if _, ok := r.last[record.Key()]; !ok {
		if time.Since(r.last[record.Key()]) > time.Duration(r.config.FlushInterval)*time.Second {
			go r.Flush(false)
		}
	} else {
		r.last[record.Key()] = time.Now()
	}

	return err
}

func (r *Receiver) Flush(force bool) error {
	slog.Info("Flushing buffer", "module", "receiver", "function", "Flush", "force", force)

	r.mu.Lock()
	defer r.mu.Unlock()

	keys := r.buffer.Keys()
	for _, key := range keys {
		if !force && r.last[key].Add(time.Duration(r.config.FlushInterval)*time.Second).After(time.Now()) {
			slog.Debug("Skipping buffer flush, interval not reached", "key", key, "module", "receiver", "function", "Flush")
			continue
		}

		data := r.buffer.Get(key)

		if len(data) == 0 {
			continue
		}

		slog.Debug("Flushing buffer", "key", key, "size", len(data), "module", "receiver", "function", "Flush")
		err := r.writer.Write(data)

		if err != nil {
			slog.Error("Error writing data", "error", err, "key", key, "size", len(data), "module", "receiver", "function", "Flush")
			return err
		}

		slog.Debug("Clearing buffer", "key", key, "size", len(data), "module", "receiver", "function", "Flush")
		err = r.buffer.Clear(key, len(data))

		if err != nil {
			slog.Error("Error clearing buffer", "error", err, "key", key, "size", len(data), "module", "receiver")
			return err
		}

		slog.Debug("Buffer flushed", "key", key, "size", len(data), "module", "receiver", "function", "Flush", "last", r.last[key])
		r.last[key] = time.Now()
	}

	return nil
}
func (r *Receiver) Close() error {
	slog.Info("Closing receiver", "module", "receiver", "function", "Close")
	r.running = false
	r.stopSignal <- true
	return nil
}

func (r *Receiver) Healthcheck() error {
	slog.Debug("Healthcheck", "running", r.running, "module", "receiver", "function", "Healthcheck")
	return nil
}
