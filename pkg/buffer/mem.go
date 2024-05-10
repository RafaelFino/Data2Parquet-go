package buffer

import (
	"data2parquet/pkg/config"
	"data2parquet/pkg/domain"
	"errors"
	"log/slog"
)

type Mem struct {
	config *config.Config
	data   map[string][]domain.Record
}

func NewMem(config *config.Config) Buffer {
	return &Mem{
		data:   make(map[string][]domain.Record),
		config: config,
	}
}

func (m *Mem) Push(key string, item domain.Record) error {
	if item == nil {
		slog.Warn("[buffer.mem] Item is nil	", "key", key)
		return errors.New("item is nil")
	}

	slog.Debug("[buffer.mem] Pushing item", "key", key, "item", item.ToString())

	if m.data == nil {
		m.data = make(map[string][]domain.Record)
	}

	if _, ok := m.data[key]; !ok {
		m.data[key] = make([]domain.Record, 0)
	}

	m.data[key] = append(m.data[key], item)

	return nil
}

func (m *Mem) Get(key string, size int) []domain.Record {
	if m.data == nil {
		return []domain.Record{}
	}

	if _, ok := m.data[key]; !ok {
		return []domain.Record{}
	}

	if len(m.data[key]) < size {
		size = len(m.data[key])
	}

	ret := m.data[key][:size]
	m.data[key] = m.data[key][size:]

	return ret
}

func (m *Mem) Keys() []string {
	keys := make([]string, 0, len(m.data))

	for k := range m.data {
		keys = append(keys, k)
	}

	return keys
}
