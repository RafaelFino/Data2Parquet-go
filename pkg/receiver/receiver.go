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
	recoveryCount map[string]int
	interval      time.Duration
	mu            *sync.Mutex
	update        chan *UpdateItem
}

type BufferControl struct {
	Last    time.Time
	Count   int
	running bool
}

type UpdateItem struct {
	Key   string
	Count int
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
		recoveryCount: make(map[string]int),
		converter:     converter.New(config),
		interval:      time.Duration(config.FlushInterval) * time.Second,
		mu:            &sync.Mutex{},
		update:        make(chan *UpdateItem, 500000),
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
		for r.running {
			item := <-r.update

			if !r.running {
				slog.Debug("Receiver is not running, stopping update channel")
				break
			}

			if c, ok := r.control[item.Key]; ok {
				c.Count = item.Count
				r.control[item.Key] = c

				if item.Count >= r.config.BufferSize && !c.running {
					if !r.buffer.CheckLock(item.Key) {
						slog.Debug("Skipping flush, buffer is locked by other process", "key", item.Key, "CheckLock", false)
						continue
					}

					slog.Debug("Buffer size reached, checkin to flush buffer", "key", item.Key, "size", item.Count, "buffer-size", r.config.BufferSize)

					if !c.running {
						err := r.flushKey(item.Key, c)

						if err != nil {
							slog.Error("Error flushing buffer", "error", err, "key", item.Key)
						}
					}
				}
			} else {
				//Fisrt record for this key
				ctrl := &BufferControl{
					Last:    time.Now(),
					Count:   item.Count,
					running: false,
				}

				r.control[item.Key] = ctrl

				go func(r *Receiver, key string, ctrl *BufferControl) {
					for r.running {
						slog.Debug("Interval reached, trying to flush buffer", "key", key)
						err := r.flushKey(key, ctrl)

						if err != nil {
							slog.Error("Error flushing buffer", "error", err, "key", key, "module", "receiver", "function", "Write")
						}

						slog.Debug("Waiting interval to flush buffer", "key", key, "interval", r.interval)
						time.Sleep(r.interval)
					}
				}(r, item.Key, ctrl)
			}
		}

		slog.Info("Stopping update channel")
	}(ret)

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

	r.update <- &UpdateItem{
		Key:   key,
		Count: n,
	}

	return nil
}

func (r *Receiver) flushKey(key string, ctrl *BufferControl) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	callResend := false

	slog.Debug("Flushing key", "key", key)
	start := time.Now()

	//Create buffer control if not found - should not happen
	if ctrl == nil {
		ctrl = &BufferControl{
			Last:    time.Now(),
			Count:   0,
			running: false,
		}
	}

	if ctrl.Count < r.config.BufferSize {
		if time.Since(ctrl.Last) < r.interval {
			slog.Debug("Skipping buffer flush, interval reached but buffer already reached about size, waiting for next check", "key", key)
			return nil
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
			if r.config.UseDLQ {
				slog.Error("Error converting data, push to DLQ", "error", item.Error, "key", key, "record", item.Record.ToJson())
				err := r.buffer.PushDLQ(item.Key, item.Record)

				if err != nil {
					slog.Error("Error pushing to DLQ Buffer", "error", err, "key", key, "module", "receiver", "function", "Flush")
					panic(err)
				}
			} else {
				slog.Warn("DLQ is disabled, skipping record", "error", item.Error, "key", key, "record", item.Record.ToJson())
			}
		}
	}

	err := r.writer.Write(key, buf)

	if err != nil {
		if !r.config.TryAutoRecover {
			slog.Error("Error writing data, resend is disabled, discarding data", "error", err, "key", key, "size", len(data))
		} else {
			slog.Error("Error writing data, pushing to recovery Buffer", "error", err, "key", key, "size", len(data))
			errWr := r.buffer.PushRecovery(key, buf)

			if errWr != nil {
				slog.Error("Error pushing to recovery buffer", "error", errWr, "key", key, "size", len(data), "duration", time.Since(start))
			}

			if r.config.TryAutoRecover {
				callResend = true
			}
		}
	}

	err = r.buffer.Clear(key, len(data))

	if err != nil {
		slog.Error("Error clearing buffer", "error", err, "key", key, "size", len(data), "module", "receiver")
	}

	//Reset buffer control
	ctrl.Count = 0
	ctrl.Last = time.Now()
	ctrl.running = false

	r.control[key] = ctrl

	slog.Info("Buffer flush process finished", "key", key, "total-duration", time.Since(start), "size", len(data))

	if callResend {
		go r.TryResendData()
	}

	return err
}

func (r *Receiver) TryResendData() {
	start := time.Now()

	if !r.config.TryAutoRecover {
		return
	}

	slog.Debug("Trying to resend data")
	remains := make(map[string]*bytes.Buffer)

	if r.buffer.HasRecovery() {
		slog.Info("Recovery data found, trying to resend")

		r.mu.Lock()
		defer r.mu.Unlock()

		recovery, err := r.buffer.GetRecovery()

		if err != nil {
			slog.Error("Error getting recovery data", "error", err)
			return
		}

		for _, item := range recovery {
			attempts, found := r.recoveryCount[item.Key]

			if !found {
				attempts = 0
			}

			if attempts > r.config.RecoveryAttempts {
				slog.Error("Recovery limit reached, stopping receiver", "key", item.Key)
				continue
			}

			buf := bytes.NewBuffer(item.Data)
			err := r.writer.Write(item.Key, buf)

			if err != nil {
				slog.Error("Error to try write recovery data", "error", err, "key", item.Key)
				remains[item.Key] = buf
				attempts++
				r.recoveryCount[item.Key] = attempts
			} else {
				slog.Info("Recovery data sent", "key", item.Key, "size", len(item.Data))
				delete(r.recoveryCount, item.Key)
			}
		}

		err = r.buffer.ClearRecoveryData()

		if err != nil {
			slog.Error("Error clearing recovery data", "error", err)
		}

		for key, buf := range remains {
			slog.Warn("Recovery data remains, pushing to buffer again", "key", key, "size", buf.Len())
			err = r.buffer.PushRecovery(key, buf)

			if err != nil {
				slog.Error("Error pushing recovery data", "error", err, "key", key)
			}
		}
	} else {
		slog.Debug("No recovery data found")
		return
	}

	if len(remains) > 0 {
		slog.Debug("Recovery finished, some data remains", "duration", time.Since(start))
		return
	}

	slog.Info("Auto recovery proccess finished, no data to resend", "duration", time.Since(start))
}

func (r *Receiver) Flush() error {
	start := time.Now()
	slog.Debug("Flushing all keys", "module", "receiver", "function", "Flush")

	keys := map[string]*BufferControl{}

	r.mu.Lock()
	for key, ctrl := range r.control {
		keys[key] = ctrl
	}
	r.mu.Unlock()

	for key, ctrl := range keys {
		err := r.flushKey(key, ctrl)

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
	close(r.update)

	slog.Info("Stopping receiver, trying to flushing remaining data from buffers")

	r.mu.Lock()
	for key := range r.control {
		slog.Debug("Flushing key on close receiver", "key", key)
		if ctrl, found := r.control[key]; found {
			ctrl.Count = r.config.BufferSize
			ctrl.Last = time.Now().Add(-r.interval)
			ctrl.running = false
			r.control[key] = ctrl
		}
	}

	r.mu.Unlock()

	r.Flush()

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
