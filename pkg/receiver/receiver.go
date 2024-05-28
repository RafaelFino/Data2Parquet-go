package receiver

import (
	"bytes"
	"context"
	"data2parquet/pkg/buffer"
	"data2parquet/pkg/config"
	"data2parquet/pkg/converter"
	"data2parquet/pkg/domain"
	"data2parquet/pkg/writer"
	"errors"
	"log/slog"
	"sync"
	"time"
)

type Receiver struct {
	config        *config.Config
	writer        writer.Writer
	buffer        buffer.Buffer
	running       bool
	control       map[string]*BufferControl
	converter     *converter.Converter
	ctx           context.Context
	recoveryCount int
	interval      time.Duration
	mu            *sync.Mutex
}

type BufferControl struct {
	Last    time.Time
	Count   int
	running bool
}

func NewReceiver(ctx context.Context, config *config.Config) *Receiver {
	if ctx == nil {
		ctx = context.Background()
	}

	ret := &Receiver{
		config:        config,
		writer:        writer.New(ctx, config),
		buffer:        buffer.New(ctx, config),
		running:       true,
		control:       make(map[string]*BufferControl),
		ctx:           ctx,
		recoveryCount: 0,
		converter:     converter.New(config),
		interval:      time.Duration(config.FlushInterval) * time.Second,
		mu:            &sync.Mutex{},
	}

	if ret.buffer == nil {
		slog.Error("Error creating buffer")
		return nil
	}

	if !ret.buffer.IsReady() {
		slog.Error("Buffer is not ready")
		return nil
	}

	slog.Debug("Initializing receiver", "config", config.ToString())

	if ret.writer == nil {
		slog.Error("Error creating writer")
		return nil
	}

	err := ret.writer.Init()

	if err != nil {
		slog.Error("Error initializing writer", "error", err)
		return nil
	}

	if !ret.writer.IsReady() {
		slog.Error("Writer is not ready")
		return nil
	}

	go func(r *Receiver) {
		var err error
		for r.running {
			time.Sleep(1 * time.Second)

			if !r.running {
				slog.Info("Receiver is not running, stopping healthcheck")
				break
			}

			err = r.Healthcheck()

			if err != nil {
				slog.Error("Error in healthcheck", "error", err)
				break
			}

			slog.Debug("Receiver is running")
		}
	}(ret)

	return ret
}

func (r *Receiver) Write(record domain.Record) error {
	key := record.Key()
	n, err := r.buffer.Push(key, record)

	if err != nil {
		slog.Error("Error pushing record", "error", err, "record", record.ToString(), "module", "receiver", "function", "Write")
		return err
	}

	if c, ok := r.control[key]; ok {
		c.Count = n
		r.control[key] = c

		if n >= r.config.BufferSize && !c.running {
			if !r.buffer.CheckLock(key) {
				slog.Debug("Skipping flush, buffer is locked by other process", "key", key, "CheckLock", false)
				return nil
			}

			slog.Debug("Buffer size reached, checkin to flush buffer", "key", key, "size", n, "buffer-size", r.config.BufferSize)

			err := r.FlushKey(key)

			if err != nil {
				slog.Error("Error flushing buffer", "error", err, "key", key)
			}
		}
	} else {
		//Fisrt record for this key
		r.control[key] = &BufferControl{
			Last:    time.Now(),
			Count:   n,
			running: false,
		}

		go func(r *Receiver, key string) {
			for r.running {
				slog.Debug("Interval reached, trying to flush buffer", "key", key)
				err := r.FlushKey(key)

				if err != nil {
					slog.Error("Error flushing buffer", "error", err, "key", key, "module", "receiver", "function", "Write")
				}

				slog.Debug("Waiting interval to flush buffer", "key", key, "interval", r.interval)
				time.Sleep(r.interval)
			}
		}(r, key)
	}

	return err
}

func (r *Receiver) FlushKey(key string) error {
	start := time.Now()

	r.mu.Lock()
	defer r.mu.Unlock()

	ctrl, found := r.control[key]
	if found {
		if ctrl.Count < r.config.BufferSize {
			if time.Since(ctrl.Last) < r.interval {
				slog.Debug("Skipping buffer flush, interval reached but buffer already reached about size, waiting for next check", "key", key)
				return nil
			}
		}
	}

	//Create buffer control if not found - should not happen
	if !found || ctrl == nil {
		ctrl = &BufferControl{
			Last:    time.Now(),
			Count:   0,
			running: false,
		}
	}

	if ctrl.running {
		slog.Debug("Skipping flush, buffer is already running", "key", key, "running", ctrl.running)
		return nil
	}

	ctrl.Count = 0
	last := ctrl.Last
	ctrl.Last = time.Now()
	r.control[key] = ctrl

	if !r.buffer.CheckLock(key) {
		slog.Info("Skipping flush, buffer is locked by other process", "key", key, "CheckLock", false)
		return nil
	}

	ctrl.running = true

	if last.Add(r.interval).Before(time.Now()) {
		slog.Info("Interval reached, trying to flush buffer", "key", key, "interval", r.interval)
	}

	slog.Debug("Flushing buffer - trying to load data from buffer", "key", key)

	data := r.buffer.Get(key)

	if len(data) == 0 {
		slog.Info("No data to flush here", "key", key)
		return nil
	}

	slog.Debug("Writing buffer data", "key", key, "size", len(data), "page-size", r.config.BufferSize)

	buf := new(bytes.Buffer)
	result := r.converter.Write(key, data, buf)

	errCount := 0

	for _, item := range result {
		if item.Error != nil {
			errCount++
			slog.Error("Error converting data", "error", item.Error, "key", key, "module", "receiver", "function", "Flush")
			err := r.buffer.PushDLQ(item.Key, item.Record)

			if err != nil {
				slog.Error("Error pushing to DLQ Buffer", "error", err, "key", key, "module", "receiver", "function", "Flush")
				panic(err)
			}
		}
	}

	err := r.writer.Write(key, buf)

	if err != nil {
		slog.Error("Error writing data, pushing to DLQ Buffer", "error", err, "key", key, "size", len(data))
		errWr := r.buffer.PushRecovery(key, buf)

		if errWr != nil {
			slog.Error("Error pushing to recovery buffer", "error", errWr, "key", key, "size", len(data), "duration", time.Since(start))
		}

		if r.config.TryAutoRecover && r.config.RecoveryAttempts > r.recoveryCount {
			slog.Error("Recovery limit reached, stopping receiver", "module", "receiver", "function", "Flush")
			go r.TryResendData()
		}
	}

	err = r.buffer.Clear(key, len(data))

	if err != nil {
		slog.Error("Error clearing buffer", "error", err, "key", key, "size", len(data), "module", "receiver")
		return err
	}

	slog.Debug("Data written and removed from buffer", "key", key, "size", len(data), "duration", time.Since(start))

	//Reset buffer control
	ctrl.Count = 0
	ctrl.Last = time.Now()
	ctrl.running = false

	r.control[key] = ctrl

	slog.Info("Buffer flushed!", "key", key, "total-duration", time.Since(start), "size", len(data))

	return nil
}

func (r *Receiver) TryResendData() {
	start := time.Now()
	slog.Debug("Trying to resend data", "module", "receiver", "function", "TryResendData")
	r.recoveryCount++

	if r.recoveryCount > r.config.RecoveryAttempts {
		slog.Error("Recovery limit reached, stopping receiver", "module", "receiver", "function", "TryResendData")
		return
	}

	resendCount := 0

	if r.buffer.HasRecovery() {
		recovery, err := r.buffer.GetRecovery()

		if err != nil {
			slog.Error("Error getting recovery data", "error", err, "module", "receiver", "function", "TryResendData")
			return
		}

		for _, item := range recovery {
			buf := bytes.NewBuffer(item.Data)
			err := r.writer.Write(item.Key, buf)

			if err != nil {
				slog.Error("Error to try write recovery data", "error", err, "key", item.Key)
			} else {
				resendCount++
			}
		}
	}

	slog.Info("Recovery finished", "module", "receiver", "function", "TryResendData", "recovery-count", resendCount, "duration", time.Since(start))
	r.recoveryCount = 0
}

func (r *Receiver) Flush() error {
	start := time.Now()
	slog.Debug("Flushing all keys", "module", "receiver", "function", "Flush")

	for key := range r.control {
		err := r.FlushKey(key)

		if err != nil {
			slog.Error("Error flushing key", "error", err, "key", key, "module", "receiver", "function", "Flush")
			return err
		}
	}

	slog.Info("Flush finished", "module", "receiver", "function", "Flush", "duration", time.Since(start))

	return nil
}

func (r *Receiver) Close() error {
	slog.Debug("Closing receiver", "module", "receiver", "function", "Close")
	r.running = false

	keys := []string{}

	slog.Info("Stopping receiver, trying to flushing remaining data from buffers")

	for key := range r.control {
		slog.Debug("Flushing key on close receiver", "key", key)
		if ctrl, found := r.control[key]; found {
			ctrl.Count = r.config.BufferSize
			ctrl.Last = time.Now().Add(-r.interval)
			ctrl.running = false
			r.control[key] = ctrl
		}

		keys = append(keys, key)
	}

	for _, key := range keys {
		err := r.FlushKey(key)

		if err != nil {
			slog.Error("Error flushing key", "error", err, "key", key, "module", "receiver", "function", "Close")
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

	if !r.writer.IsReady() {
		return errors.New("writer is not ready")
	}

	return nil
}
