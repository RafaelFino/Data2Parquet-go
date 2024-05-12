package writer

import (
	"data2parquet/pkg/config"
	"data2parquet/pkg/domain"
	"log/slog"
)

type None struct {
	config *config.Config
}

func NewNone(config *config.Config) Writer {
	return &S3{
		config: config,
	}
}

func (n *None) Init() error {
	slog.Debug("[writer] Initializing empty writer", "config", n.config.ToString())
	return nil
}

func (n *None) Write(data []*domain.Record) error {
	slog.Debug("[writer] Writing logs", "data", data)
	return nil
}

func (n *None) Close() error {
	slog.Debug("[writer] Closing S3 writer")
	return nil
}

func (n *None) IsReady() bool {
	return true
}
