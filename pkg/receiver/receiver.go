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
	control    map[string]BufferControl
	mu         sync.Mutex
	stopSignal chan bool
}

type BufferControl struct {
	Last  time.Time
	Count int64
}

func NewReceiver(config *config.Config) *Receiver {
	ret := &Receiver{
		config:     config,
		writer:     writer.New(config),
		buffer:     buffer.New(config),
		running:    true,
		control:    make(map[string]BufferControl),
		mu:         sync.Mutex{},
		stopSignal: make(chan bool),
	}

	slog.Debug("Initializing receiver", "config", config.ToString(), "module", "receiver", "function", "NewReceiver")

	ret.writer.Init()

	go func(rcv *Receiver) {
		for {
			select {
			case <-rcv.stopSignal:
				{
					slog.Info("Stopping receiver", "module", "receiver", "function", "NewReceiver")
					return
				}
			case <-time.After(time.Duration(rcv.config.FlushInterval) * time.Second):
				{
					if rcv.running {
						rcv.Flush(false)
					}
				}
			}
		}
	}(ret)

	return ret
}

func (r *Receiver) Write(record *domain.Record) error {
	err := r.buffer.Push(record.Key(), record)

	if err != nil {
		slog.Error("Error pushing record", "error", err, "record", record.ToString(), "module", "receiver", "function", "Write")
	}

	if c, ok := r.control[record.Key()]; ok {
		c.Count++

		if time.Since(c.Last) > time.Duration(r.config.FlushInterval)*time.Second || c.Count > int64(r.config.BufferSize) {
			go r.Flush(false)
		}

		r.control[record.Key()] = c
	} else {
		r.control[record.Key()] = BufferControl{
			Last:  time.Now(),
			Count: 1,
		}
	}

	return err
}

func (r *Receiver) FlushKey(key string, force bool, wg *sync.WaitGroup) error {
	start := time.Now()
	defer wg.Done()

	if !force {
		if c, ok := r.control[key]; ok {
			if c.Count < int64(r.config.BufferSize) {
				if time.Since(c.Last) < time.Duration(r.config.FlushInterval)*time.Second {
					slog.Debug("Skipping buffer flush, interval not reached", "key", key, "module", "receiver", "function", "FlushKey")
					return nil
				}
			}
		}
	}

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
	r.control[key] = BufferControl{
		Last:  time.Now(),
		Count: 0,
	}

	return nil
}
func (r *Receiver) Flush(force bool) error {
	start := time.Now()

	r.mu.Lock()
	defer r.mu.Unlock()

	keys := r.buffer.Keys()
	wg := &sync.WaitGroup{}
	wg.Add(len(keys))

	for _, key := range keys {

		go r.FlushKey(key, force, wg)
	}

	slog.Debug("Waiting for buffer flush", "module", "receiver", "function", "Flush", "keys", keys)

	wg.Wait()

	slog.Info("Buffer flushed", "module", "receiver", "function", "Flush", "duration", time.Since(start), "keys", len(keys), "force", force)

	return nil
}
func (r *Receiver) Close() error {
	slog.Info("Closing receiver", "module", "receiver", "function", "Close")
	r.running = false
	r.stopSignal <- true
	r.Flush(true)

	return r.Flush(true)
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
