package writer

import (
	"data2parquet/pkg/config"
	"data2parquet/pkg/domain"
	"log/slog"
)

type None struct {
}

func NewNone(config *config.Config) Writer {
	return &None{}
}

func (n *None) Init() error {
	slog.Info("Waring: None Writer init, no data will be written", "module", "writer.none", "function", "Init")
	return nil
}

func (n *None) Write(data []*domain.Record) []*WriterReturn {
	slog.Debug("Waring: None Writer write, no data will be written", "module", "writer.none", "function", "Write")
	return nil
}

func (n *None) Close() error {
	slog.Info("Waring: None Writer close, no data will be written", "module", "writer.none", "function", "Close")
	return nil
}

func (n *None) IsReady() bool {
	return true
}
