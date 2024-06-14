package buffer

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"data2parquet/pkg/config"
	"data2parquet/pkg/domain"
)

type Mem struct {
	config   *config.Config
	data     map[string][]domain.Record
	dlq      map[string][]domain.Record
	recovery []*RecoveryData
	mu       sync.Mutex
	Ready    bool
	ctx      context.Context
}

func NewMem(ctx context.Context, config *config.Config) Buffer {
	ret := &Mem{
		data:     make(map[string][]domain.Record),
		dlq:      make(map[string][]domain.Record),
		recovery: make([]*RecoveryData, 0),
		config:   config,
		ctx:      ctx,
		Ready:    true,
	}

	return ret
}

func (m *Mem) Close() error {
	slog.Debug("Closing buffer", "module", "buffer.mem", "function", "Close")
	m.Ready = false
	return nil
}

func (m *Mem) Len(key string) int {
	slog.Debug("Getting buffer length", "key", key, "module", "buffer.mem", "function", "Len")

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.data == nil {
		return 0
	}

	if _, ok := m.data[key]; !ok {
		return 0
	}

	return len(m.data[key])
}

func (m *Mem) Push(key string, item domain.Record) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(key) == 0 {
		slog.Warn("Key is empty", "module", "buffer.mem", "function", "Push")
		return 0, errors.New("key is empty")
	}

	if item == nil {
		slog.Warn("Item is nil	", "key", key, "module", "buffer.mem", "function", "Push")
		return 0, errors.New("item is nil")
	}

	values, ok := m.data[key]
	if !ok {
		values = make([]domain.Record, 0, m.config.BufferSize)
	}

	values = append(values, item)

	m.data[key] = values

	return len(values), nil
}

func (m *Mem) PushDLQ(key string, item domain.Record) error {
	if item == nil {
		slog.Warn("Item is nil", "key", key, "module", "buffer.mem", "function", "PushDLQ")
		return errors.New("item is nil")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	slog.Debug("Pushing to recovery", "key", key, "record", item, "module", "buffer.mem", "function", "PushDLQ")

	if _, ok := m.dlq[key]; !ok {
		m.dlq[key] = make([]domain.Record, 0, m.config.BufferSize)
	}

	m.dlq[key] = append(m.dlq[key], item)

	return nil
}

func (m *Mem) GetDLQ() (map[string][]domain.Record, error) {
	slog.Debug("GetDLQ data", "module", "buffer.mem", "function", "GetDLQ")
	if m.recovery == nil {
		return make(map[string][]domain.Record), nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	ret := make(map[string][]domain.Record)

	for k, v := range m.dlq {
		ret[k] = v
	}

	return ret, nil
}

func (m *Mem) ClearDLQ() error {
	slog.Debug("Clearing DLQ", "module", "buffer.mem", "function", "ClearDLQ")
	if m.dlq == nil {
		return nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.dlq = make(map[string][]domain.Record)

	return nil
}

func (m *Mem) Get(key string) []domain.Record {
	slog.Debug("Getting buffer", "key", key, "module", "buffer.mem", "function", "Get")

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.data == nil {
		return nil
	}

	if _, ok := m.data[key]; !ok {
		return nil
	}

	l := len(m.data[key])

	if l > m.config.BufferSize {
		return m.data[key][:m.config.BufferSize]
	}

	return m.data[key]
}

func (m *Mem) Clear(key string, size int) error {
	slog.Debug("Clearing buffer", "key", key, "size", size, "module", "buffer.mem", "function", "Clear")
	if m.data == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.data[key]; !ok {
		return nil
	}

	if size == -1 || size > len(m.data[key]) {
		delete(m.data, key)
		return nil
	}

	m.data[key] = m.data[key][size:]

	return nil
}
func (m *Mem) Keys() []string {
	keys := make([]string, 0, len(m.data))

	m.mu.Lock()
	defer m.mu.Unlock()

	for k := range m.data {
		keys = append(keys, k)
	}

	return keys
}

func (m *Mem) IsReady() bool {
	return m.Ready
}

func (m *Mem) HasRecovery() bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	return len(m.recovery) > 0
}

func (m *Mem) PushRecovery(key string, buf *bytes.Buffer) error {
	slog.Debug("Pushing to DLQ", "key", key, "module", "buffer.mem", "function", "PushDLQ")

	m.mu.Lock()
	defer m.mu.Unlock()

	m.recovery = append(m.recovery, &RecoveryData{
		Key:       key,
		Data:      buf.Bytes(),
		Timestamp: time.Now(),
	})

	return nil
}

func (m *Mem) GetRecovery() ([]*RecoveryData, error) {
	slog.Debug("Getting DLQ", "module", "buffer.mem", "function", "GetDLQ")

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.recovery == nil {
		m.recovery = make([]*RecoveryData, 0)
	}

	return append(make([]*RecoveryData, 0, len(m.recovery)), m.recovery...), nil
}

func (m *Mem) ClearRecoveryData() error {
	slog.Debug("Clearing DLQ", "module", "buffer.mem", "function", "ClearDLQ")

	m.mu.Lock()
	defer m.mu.Unlock()

	m.recovery = make([]*RecoveryData, 0)

	return nil
}

func (m *Mem) CheckLock(key string) bool {
	return true
}
