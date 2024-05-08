package service

import (
	"log/slog"
	"log2parquet/pkg/config"
	"log2parquet/pkg/domain"
)

type Receiver struct {
}

func NewReceiver(config *config.Config) *Receiver {
	return &Receiver{}
}

func (r *Receiver) Init(config *config.Config) error {
	slog.Debug("[receiver] Initializing receiver", "config", config.ToString())
	return nil
}

func (r *Receiver) Write(data *domain.Log) {
	slog.Debug("[receiver] Writing log", "log", data.ToString())
}

func (r *Receiver) Close() error {
	slog.Debug("[receiver] Closing receiver")
	return nil
}

func (r *Receiver) Healthcheck() error {
	slog.Debug("[receiver] Healthcheck")
	return nil
}
