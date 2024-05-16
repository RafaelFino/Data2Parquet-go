package buffer

import (
	"bytes"
	"context"
	"data2parquet/pkg/config"
	"data2parquet/pkg/domain"
	"log/slog"
	"time"

	msgp "github.com/vmihailenco/msgpack/v5"
)

type Buffer interface {
	Close() error
	Push(key string, item domain.Record) error
	PushDLQ(key string, item domain.Record) error
	GetDLQ() (map[string][]domain.Record, error)
	ClearDLQ() error
	Get(key string) []domain.Record
	Clear(key string, size int) error
	Len(key string) int
	Keys() []string
	IsReady() bool
	HasRecovery() bool
	PushRecovery(key string, buf *bytes.Buffer) error
	GetRecovery() ([]*RecoveryData, error)
	ClearRecoveryData() error
}

func New(ctx context.Context, config *config.Config) Buffer {
	switch config.BufferType {
	case "redis":
		return NewRedis(ctx, config)
	default:
		return NewMem(ctx, config)
	}
}

type RecoveryData struct {
	Key       string    `msg:"key"`
	Data      []byte    `msg:"data"`
	Timestamp time.Time `msg:"timestamp"`
}

func (l *RecoveryData) ToMsgPack() []byte {
	data, err := msgp.Marshal(l)

	if err != nil {
		slog.Error("Error marshalling MsgPack", "error", err)
		return nil
	}

	return data
}

func (l *RecoveryData) FromMsgPack(data []byte) error {
	err := msgp.Unmarshal(data, l)

	if err != nil {
		slog.Error("Error unmarshalling MsgPack", "error", err)
		return err
	}

	return nil
}
