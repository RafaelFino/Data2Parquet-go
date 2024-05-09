package receiver

import (
	"data2parquet/pkg/config"
	"data2parquet/pkg/domain"
	"log/slog"
)

type Receiver struct {
	config *config.Config
}

func NewReceiver(config *config.Config) *Receiver {
	ret := &Receiver{
		config: config,
	}
	err := ret.init()

	if err != nil {
		slog.Error("[receiver] Error initializing receiver", "error", err)
		return nil
	}

	return ret
}

func (r *Receiver) init() error {
	slog.Debug("[receiver] Initializing receiver", "config", r.config.ToString())
	return nil
}

func (r *Receiver) Write(data domain.Line) {
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
