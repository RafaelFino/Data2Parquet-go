package receiver

import (
	"data2parquet/pkg/buffer"
	"data2parquet/pkg/config"
	"data2parquet/pkg/domain"
	"data2parquet/pkg/writer"
	"errors"
	"log/slog"
	"sync"
	"time"
)

type Receiver struct {
	config     *config.Config
	writer     writer.Writer
	buffer     buffer.Buffer
	running    bool
	control    map[string]*BufferControl
	stopSignal chan bool
}

type BufferControl struct {
	Last  time.Time
	Count int
	mu    *sync.Mutex
}

func NewReceiver(config *config.Config) *Receiver {
	ret := &Receiver{
		config:     config,
		writer:     writer.New(config),
		buffer:     buffer.New(config),
		running:    true,
		control:    make(map[string]*BufferControl),
		stopSignal: make(chan bool),
	}

	slog.Debug("Initializing receiver", "config", config.ToString(), "module", "receiver", "function", "NewReceiver")

	ret.writer.Init()

	return ret
}

func (r *Receiver) Write(record *domain.Record) error {
	err := r.buffer.Push(record.Key(), record)

	if err != nil {
		slog.Error("Error pushing record", "error", err, "record", record.ToString(), "module", "receiver", "function", "Write")
	}

	if c, ok := r.control[record.Key()]; ok {
		c.Count++
		r.control[record.Key()] = c

		if c.Count >= r.config.BufferSize {
			go func() {
				err := r.FlushKey(record.Key())

				if err != nil {
					slog.Error("Error flushing buffer", "error", err, "key", record.Key(), "module", "receiver", "function", "Write")
				}
			}()
		}
	} else {
		r.control[record.Key()] = &BufferControl{
			Last:  time.Now(),
			Count: 1,
			mu:    &sync.Mutex{},
		}

		go func(r *Receiver) {
			for r.running {
				select {
				case <-r.stopSignal:
					{
						slog.Debug("Receiving stop signal from key", "module", "receiver", "function", "Write", "key", record.Key())
						return
					}
				case <-time.After(time.Duration(r.config.FlushInterval) * time.Second):
					{
						err := r.FlushKey(record.Key())

						if err != nil {
							slog.Error("Error flushing buffer", "error", err, "key", record.Key(), "module", "receiver", "function", "Write")
						}
					}
				}
			}
		}(r)
	}

	return err
}

func (r *Receiver) FlushKey(key string) error {
	start := time.Now()

	var ctrl *BufferControl
	ctrl, found := r.control[key]
	if found {
		if time.Since(ctrl.Last) < time.Duration(r.config.FlushInterval)*time.Second {
			slog.Debug("Skipping buffer flush, interval not reached", "key", key, "module", "receiver", "function", "FlushKey")
			return nil
		}
	}

	if !found || ctrl == nil {
		ctrl = &BufferControl{
			mu: &sync.Mutex{},
		}
	}

	ctrl.mu.Lock()
	defer ctrl.mu.Unlock()

	data := r.buffer.Get(key)

	if len(data) == 0 {
		return nil
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

	slog.Debug("Buffer flushed", "key", key, "size", len(data), "module", "receiver", "function", "Flush", "duration", time.Since(start))

	ctrl.Last = time.Now()
	ctrl.Count = 0

	r.control[key] = ctrl

	return nil
}

func (r *Receiver) Close() error {
	slog.Info("Closing receiver", "module", "receiver", "function", "Close")
	r.running = false

	slog.Info("Stopping receiver, flushing buffers", "module", "receiver", "function", "Close")

	for key := range r.control {
		r.stopSignal <- true
		//Change buffer control to force flush
		if ctrl, found := r.control[key]; found {
			ctrl.Count = r.config.BufferSize
			ctrl.Last = time.Now().Add(-time.Duration(r.config.FlushInterval) * time.Second)
			r.control[key] = ctrl
		}

		err := r.FlushKey(key)

		if err != nil {
			slog.Error("Error flushing buffer to close Receiver", "error", err, "key", key, "module", "receiver", "function", "NewReceiver")
			return err
		}
	}

	return nil
}

func (r *Receiver) Healthcheck() error {
	slog.Debug("Healthcheck", "running", r.running, "module", "receiver", "function", "Healthcheck")
	if !r.running {
		return errors.New("receiver is not running")
	}

	if !r.buffer.IsReady() {
		return errors.New("buffer is not ready")
	}

	return nil
}
