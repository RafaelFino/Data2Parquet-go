package receiver

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"data2parquet/pkg/buffer"
	"data2parquet/pkg/config"
	"data2parquet/pkg/converter"
	"data2parquet/pkg/domain"
	"data2parquet/pkg/writer"
)

type Receiver struct {
	config        *config.Config
	writer        writer.Writer
	buffer        buffer.Buffer
	running       bool
	last          map[string]*time.Time
	converter     *converter.Converter
	ctx           context.Context
	recoveryCount map[string]int
	interval      time.Duration
	mu            *sync.RWMutex
	update        chan *UpdateItem
}

type BufferControl struct {
	Last  time.Time
	Count int
}

type UpdateItem struct {
	Key   string
	Count int
}

type FlushReason string

const (
	FlushReasonSize     FlushReason = "buffer-size"
	FlushReasonInterval FlushReason = "interval"
	FlushReasonClose    FlushReason = "close"
)

func NewReceiver(ctx context.Context, config *config.Config) *Receiver {
	if ctx == nil {
		ctx = context.Background()
	}

	ret := &Receiver{
		config:        config,
		writer:        writer.New(ctx, config),
		buffer:        buffer.New(ctx, config),
		running:       true,
		last:          make(map[string]*time.Time),
		ctx:           ctx,
		recoveryCount: make(map[string]int),
		converter:     converter.New(config),
		interval:      time.Duration(config.FlushInterval) * time.Second,
		mu:            &sync.RWMutex{},
		update:        make(chan *UpdateItem, config.BufferSize),
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

	go ret.runHealthchek()
	go ret.processUpdate()

	return ret
}

func (r *Receiver) runHealthchek() {
	for r.running {
		<-time.After(1 * time.Second)
		if !r.running {
			slog.Info("Receiver is not running, stopping healthcheck")
			continue
		}

		err := r.Healthcheck()

		if err != nil {
			slog.Error("Error in healthcheck", "error", err)
			continue
		}

		slog.Debug("Receiver is running")
	}
	slog.Info("Stopping healthcheck process")
}

func (r *Receiver) runInterval(key string) {
	time.Sleep(r.interval)

	for r.running {
		r.mu.Lock()
		last, found := r.last[key]
		if !found {
			slog.Debug("Interval control time not found!", "key", key)
		}
		r.mu.Unlock()

		since := time.Since(*last)

		if since >= r.interval {
			err := r.flushKey(key, FlushReasonInterval)

			if err != nil {
				slog.Error("Error to flush key", "key", key, "error", err)
			}
		}

		time.Sleep(r.interval)
	}
}

func (r *Receiver) processUpdate() {
	for r.running {
		item, updateChannelOk := <-r.update
		if !updateChannelOk {
			slog.Debug("Flush channel is closed, stopping update channel")
			break
		}

		if !r.running {
			slog.Debug("Receiver is not running, stopping update channel")
			continue
		}

		r.mu.Lock()
		if _, found := r.last[item.Key]; !found {
			last := time.Now()
			r.last[item.Key] = &last
			go r.runInterval(item.Key)
		}
		r.mu.Unlock()

		if item.Count >= r.config.BufferSize {
			bfSize := r.buffer.Len(item.Key)

			if bfSize > r.config.BufferSize {
				err := r.flushKey(item.Key, FlushReasonSize)

				if err != nil {
					slog.Error("Error to flush key", "key", item.Key, "error", err)
				}
			}
		}
	}
	slog.Info("Stopping update queue process")
}

func (r *Receiver) Write(record domain.Record) error {
	key := record.Key()
	n, err := r.buffer.Push(key, record)

	if err != nil {
		slog.Error("Error pushing record", "error", err, "record", record.ToString())
		return err
	}

	r.update <- &UpdateItem{
		Key:   key,
		Count: n,
	}

	return nil
}

func (r *Receiver) flushKey(key string, reason FlushReason) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	callResend := false
	start := time.Now()

	last, found := r.last[key]

	if !found {
		last = &start
	}

	if reason == FlushReasonInterval && time.Since(*last) < r.interval {
		slog.Info("Skipping buffer flush, interval time has not yet been reached", "reason", reason, "key", key)
		return nil
	}

	r.last[key] = &start

	if !r.buffer.CheckLock(key) {
		slog.Info("Skipping flush, buffer is locked by other process", "key", key)
		return nil
	}

	data := r.buffer.Get(key)
	size := len(data)

	if reason == FlushReasonSize && size < r.config.BufferSize {
		slog.Info("Skipping buffer flush, buffer size has not yet been reached", "reason", reason, "key", key, "size", size)
		return nil
	}

	if size == 0 {
		slog.Info("Skipping buffer flush, no data to flush here", "reason", reason, "key", key, "size", size)
		return nil
	}

	slog.Info("Flushing key", "reason", reason, "key", key)

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
					slog.Error("Error pushing to DLQ Buffer", "error", err, "key", key)
				}
			} else {
				slog.Warn("DLQ is disabled, skipping record", "error", item.Error, "key", key, "record", item.Record.ToJson())
			}
		}
	}

	err := r.writer.Write(key, buf)

	if err != nil {
		if !r.config.TryAutoRecover {
			slog.Error("Error writing data, resend is disabled, discarding data", "error", err, "key", key, "lines", len(data))
		} else {
			slog.Error("Error writing data, pushing to recovery Buffer", "error", err, "key", key, "lines", len(data))
			errWr := r.buffer.PushRecovery(key, buf)

			if errWr != nil {
				slog.Error("Error pushing to recovery buffer", "error", errWr, "key", key, "lines", len(data), "duration", time.Since(start))
			}

			if r.config.TryAutoRecover {
				callResend = true
			}
		}
	}

	err = r.buffer.Clear(key, len(data))

	if err != nil {
		slog.Error("Error clearing buffer", "error", err, "key", key, "lines", len(data))
	}

	slog.Info("Buffer flush process finished", "key", key, "total-duration", time.Since(start), "lines", len(data))

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
			slog.Warn("Recovery data remains, pushing to buffer again", "key", key, "lines", buf.Len())
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
	slog.Debug("Flushing all keys")

	keys := []string{}

	for !r.mu.TryRLock() {
		slog.Info("Waiting for lock")
		time.Sleep(1 * time.Second)
	}

	for key := range r.last {
		keys = append(keys, key)
	}

	r.mu.RUnlock()

	for _, key := range keys {
		err := r.flushKey(key, FlushReasonClose)

		if err != nil {
			slog.Error("Error flushing key", "error", err, "key", key)
			return err
		}
	}

	slog.Info("Flush finished", "duration", time.Since(start))

	return nil
}

func (r *Receiver) Close() error {
	slog.Debug("Closing receiver")
	r.running = false
	close(r.update)

	slog.Info("Stopping receiver, trying to flushing remaining data from buffers")

	r.Flush()

	return nil
}

func (r *Receiver) Healthcheck() error {
	slog.Debug("Healthcheck", "running", r.running)
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
