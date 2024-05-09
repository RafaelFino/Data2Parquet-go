package buffer

import (
	"data2parquet/pkg/domain"
	"errors"
	"log/slog"
)

type Mem struct {
	data map[string][]*domain.Record
}

func NewMem() Buffer {
	return &Mem{}
}

func (m *Mem) Push(key string, item *domain.Record) error {
	if item == nil {
		slog.Warn("[buffer.mem] Item is nil	", "key", key)
		return errors.New("Item is nil")
	}

	slog.Debug("[buffer.mem] Pushing item", "key", key, "item", (*item).ToString())

	if m.data == nil {
		m.data = make(map[string][]*domain.Record)
	}

	if _, ok := m.data[key]; !ok {
		m.data[key] = make([]*domain.Record, 0)
	}

	m.data[key] = append(m.data[key], item)

	return nil
}

func (m *Mem) PushMany(key string, items []*domain.Record) error {
	if items == nil {
		slog.Warn("[buffer.mem] Items is nil", "key", key)
		return errors.New("Items is nil")
	}

	slog.Debug("[buffer.mem] Pushing items", "key", key, "items", len(items))

	if m.data == nil {
		m.data = make(map[string][]*domain.Record)
	}

	if _, ok := m.data[key]; !ok {
		m.data[key] = make([]*domain.Record, 0)
	}

	m.data[key] = append(m.data[key], items...)

	return nil
}

func (m *Mem) Get(key string) []*domain.Record {
	if m.data == nil {
		slog.Debug("[buffer.mem] Data is nil", "key", key)
		return nil
	}

	if _, ok := m.data[key]; !ok {
		slog.Debug("[buffer.mem] Key not found", "key", key)
		return nil
	}

	return m.data[key]
}

func (m *Mem) Keys() []string {
	keys := make([]string, 0, len(m.data))

	for k := range m.data {
		keys = append(keys, k)
	}

	return keys
}
