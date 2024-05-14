package receiver

import (
	"context"
	"data2parquet/pkg/buffer"
	"data2parquet/pkg/config"
	"data2parquet/pkg/domain"
	"data2parquet/pkg/writer"
	"errors"
	"log/slog"
	"sync"
	"time"
)

// / Receiver
// / @struct Receiver
type Receiver struct {
	config     *config.Config
	writer     writer.Writer
	buffer     buffer.Buffer
	running    bool
	control    map[string]*BufferControl
	stopSignal chan string
	ctx        context.Context
}

// / BufferControl
// / @struct BufferControl
type BufferControl struct {
	Last    time.Time
	Count   int
	mu      *sync.Mutex
	running bool
}

// / New receiver
// / @param config *config.Config
// / @return Receiver
func NewReceiver(ctx context.Context, config *config.Config) *Receiver {
	if ctx == nil {
		ctx = context.Background()
	}

	ret := &Receiver{
		config:     config,
		writer:     writer.New(ctx, config),
		buffer:     buffer.New(ctx, config),
		running:    true,
		control:    make(map[string]*BufferControl),
		stopSignal: make(chan string),
		ctx:        ctx,
	}

	slog.Debug("Validating receiver buffer", "module", "receiver", "function", "NewReceiver")

	if ret.buffer == nil {
		slog.Error("Error creating buffer", "module", "receiver", "function", "NewReceiver")
		return nil
	}

	if !ret.buffer.IsReady() {
		slog.Error("Buffer is not ready", "module", "receiver", "function", "NewReceiver")
		return nil
	}

	slog.Debug("Initializing receiver", "config", config.ToString(), "module", "receiver", "function", "NewReceiver")

	if ret.writer == nil {
		slog.Error("Error creating writer", "module", "receiver", "function", "NewReceiver")
		return nil
	}

	err := ret.writer.Init()

	if err != nil {
		slog.Error("Error initializing writer", "error", err, "module", "receiver", "function", "NewReceiver")
		return nil
	}

	if !ret.writer.IsReady() {
		slog.Error("Writer is not ready", "module", "receiver", "function", "NewReceiver")
		return nil
	}

	return ret
}

func (r *Receiver) Write(record *domain.Record) error {
	key := record.Key()
	err := r.buffer.Push(key, record)

	if err != nil {
		slog.Error("Error pushing record", "error", err, "record", record.ToString(), "module", "receiver", "function", "Write")
	}

	//Check if key is already in control and increment count
	if c, ok := r.control[key]; ok {
		c.Count++
		r.control[key] = c

		if c.Count >= r.config.BufferSize && !c.running {
			//Call flush on reach buffer size
			err := r.FlushKey(key, nil)

			if err != nil {
				slog.Error("Error flushing buffer", "error", err, "key", key, "module", "receiver", "function", "Write")
			}
		}
	} else {
		//Fisrt record for this key
		r.control[key] = &BufferControl{
			Last:    time.Now(),
			Count:   1,
			mu:      &sync.Mutex{},
			running: false,
		}

		go func(r *Receiver, key string) {
			for r.running {
				select {
				case k := <-r.stopSignal:
					{
						//Soft stop signal
						slog.Debug("Receiving stop signal from key", "module", "receiver", "function", "Write", "key", k)

						return
					}
				case <-time.After(time.Duration(r.config.FlushInterval) * time.Second):
					{
						//Flush on interval
						err := r.FlushKey(key, nil)

						if err != nil {
							slog.Error("Error flushing buffer", "error", err, "key", key, "module", "receiver", "function", "Write")
						}
					}
				}
			}
		}(r, key)
	}

	return err
}

func (r *Receiver) FlushKey(key string, wg *sync.WaitGroup) error {
	if wg != nil {
		defer wg.Done()
	}

	start := time.Now()

	//Get buffer control
	var ctrl *BufferControl
	ctrl, found := r.control[key]
	if found {
		if time.Since(ctrl.Last) < time.Duration(r.config.FlushInterval)*time.Second {
			slog.Debug("Skipping buffer flush, interval not reached", "key", key, "module", "receiver", "function", "FlushKey")
			return nil
		}
	}

	//Create buffer control if not found - should not happen
	if !found || ctrl == nil {
		ctrl = &BufferControl{
			mu: &sync.Mutex{},
		}
	}

	if ctrl.running {
		return nil
	}

	ctrl.running = true
	ctrl.mu.Lock()

	defer func(ctrl *BufferControl) {
		ctrl.running = false
		ctrl.mu.Unlock()
	}(ctrl)

	data := r.buffer.Get(key)

	if len(data) == 0 {
		return nil
	}

	slog.Debug("Starting to flushing buffer", "key", key, "size", len(data), "buffer-size", r.config.BufferSize, "module", "receiver", "function", "Flush", "duration", time.Since(start))
	writerRet := r.writer.Write(data)

	if writer.CheckWriterError(writerRet) {
		slog.Error("Error writing data, trying to recovery data", "key", key, "size", len(data), "module", "receiver", "function", "Flush")
		defer r.RecoveryWriteError(writerRet)
	}

	slog.Debug("Clearing buffer data", "key", key, "size", len(data), "module", "receiver", "function", "Flush", "duration", time.Since(start))
	err := r.buffer.Clear(key, len(data))

	if err != nil {
		slog.Error("Error clearing buffer", "error", err, "key", key, "size", len(data), "module", "receiver")
		return err
	}

	//Reset buffer control
	ctrl.Last = time.Now()
	ctrl.Count = 0

	r.control[key] = ctrl

	slog.Info("Buffer flushed", "key", key, "size", len(data), "module", "receiver", "function", "Flush", "duration", time.Since(start))

	return nil
}

func (r *Receiver) Flush() error {
	start := time.Now()
	slog.Info("Flushing all keys", "module", "receiver", "function", "Flush")

	wg := &sync.WaitGroup{}
	wg.Add(len(r.control))
	results := make(chan error)

	for key := range r.control {
		go func(key string, wg *sync.WaitGroup, results chan error) {
			results <- r.FlushKey(key, wg)
		}(key, wg, results)
	}

	slog.Debug("Waiting for buffer flush to complete", "module", "receiver", "function", "Flush")
	wg.Wait()

	for i := 0; i < len(results); i++ {
		err := <-results
		if err != nil {
			slog.Error("Error flushing buffer", "error", err, "module", "receiver", "function", "Flush")
			return err
		}
	}

	slog.Info("Flush finished", "module", "receiver", "function", "Flush", "duration", time.Since(start))

	return nil
}

func (r *Receiver) RecoveryWriteError(writerRet []*writer.WriterReturn) {
	slog.Info("Recovering from write error", "module", "receiver", "function", "RecoveryWriteError")
	for _, item := range writerRet {
		if item.Error != nil {
			slog.Info("Recovery process: error writing record", "error", item.Error, "module", "receiver", "function", "RecoveryWriteError")

			if item.Records != nil {
				for _, rec := range item.Records {
					slog.Debug("Recovery process: writing record", "record", rec.ToString(), "module", "receiver", "function", "RecoveryWriteError")
					err := r.buffer.PushRecovery(rec.Key(), rec)

					if err != nil {
						slog.Error("Error pushing recovery record", "error", err, "record", rec.ToString(), "module", "receiver", "function", "RecoveryWriteError")
					}
				}
			}

			slog.Debug("Recovery process: clearing buffer", "size", len(item.Records), "source-err", item.Error, "module", "receiver", "function", "RecoveryWriteError")
		}
	}
}

func (r *Receiver) Close() error {
	slog.Info("Closing receiver", "module", "receiver", "function", "Close")
	r.running = false

	slog.Info("Stopping receiver, flushing buffers", "module", "receiver", "function", "Close")

	wg := &sync.WaitGroup{}

	for key := range r.control {
		slog.Info("Flushing key on close receiver", "key", key, "module", "receiver", "function", "Close")
		if ctrl, found := r.control[key]; found {
			ctrl.Count = r.config.BufferSize
			ctrl.Last = time.Now().Add(-time.Duration(r.config.FlushInterval) * time.Second)
			r.control[key] = ctrl
		}

		wg.Add(1)
		err := r.FlushKey(key, wg)

		r.stopSignal <- key

		if err != nil {
			slog.Error("Error flushing buffer to close Receiver", "error", err, "key", key, "module", "receiver", "function", "Close")
			return err
		}
	}

	wg.Wait()
	r.ctx.Done()

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

	if !r.writer.IsReady() {
		return errors.New("writer is not ready")
	}

	return nil
}
