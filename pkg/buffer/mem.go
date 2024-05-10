package buffer

import (
	"data2parquet/pkg/config"
	"data2parquet/pkg/domain"
	"errors"
	"log/slog"
	"sync"
)

type Mem struct {
	config *config.Config
	data   map[string][]*domain.Record
	buff   chan BuffItem
	mu     sync.Mutex
}

type BuffItem struct {
	key  string
	item *domain.Record
}

func NewMem(config *config.Config) Buffer {
	ret := &Mem{
		data:   make(map[string][]*domain.Record),
		config: config,
		buff:   make(chan BuffItem, config.BufferSize),
	}

	ret.buff = make(chan BuffItem, config.BufferSize)

	go ret.run()

	return ret
}

func (m *Mem) Push(key string, item *domain.Record) error {
	if item == nil {
		slog.Warn("Item is nil	", "key", key, "module", "buffer.mem", "function", "Push")
		return errors.New("item is nil")
	}

	m.buff <- BuffItem{
		key:  key,
		item: item,
	}

	return nil
}

func (m *Mem) run() {
	for {
		select {
		case item := <-m.buff:
			m.mu.Lock()
			if _, ok := m.data[item.key]; !ok {
				m.data[item.key] = make([]*domain.Record, 0, m.config.BufferSize)
			}

			m.data[item.key] = append(m.data[item.key], item.item)
			m.mu.Unlock()
		}
	}
}

func (m *Mem) Get(key string) []*domain.Record {
	slog.Debug("Getting buffer", "key", key, "module", "buffer.mem", "function", "Get")

	if m.data == nil {
		return nil
	}

	if _, ok := m.data[key]; !ok {
		return nil
	}

	return m.data[key]
}

func (m *Mem) Clear(key string, size int) error {
	slog.Debug("Clearing buffer", "key", key, "size", size, "module", "buffer.mem", "function", "Clear")
	if m.data == nil {
		return nil
	}

	if _, ok := m.data[key]; !ok {
		return nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if size == -1 || size > len(m.data[key]) {
		delete(m.data, key)
		return nil
	}

	m.data[key] = m.data[key][size:]

	return nil
}
func (m *Mem) Keys() []string {
	keys := make([]string, 0, len(m.data))

	for k := range m.data {
		keys = append(keys, k)
	}

	return keys
}

func (m *Mem) IsReady() bool {
	return true
}
