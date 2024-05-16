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

// / Buffer interface
type Buffer interface {
	Close() error
	Push(key string, item domain.Record) error
	PushRecovery(key string, item domain.Record) error
	RecoveryData() error
	Get(key string) []domain.Record
	Clear(key string, size int) error
	Len(key string) int
	Keys() []string
	IsReady() bool
	HasRecovery() bool
	PushDLQ(key string, buf *bytes.Buffer) error
	GetDLQ() []*DLQData
	ClearDLQ() error
}

// / New buffer
// / @param config *config.Config
// / @return Buffer
func New(ctx context.Context, config *config.Config) Buffer {
	switch config.BufferType {
	case "redis":
		return NewRedis(ctx, config)
	default:
		return NewMem(ctx, config)
	}
}

type DLQData struct {
	Key       string    `msg:"key"`
	Data      []byte    `msg:"data"`
	Timestamp time.Time `msg:"timestamp"`
}

func (l *DLQData) ToMsgPack() []byte {
	data, err := msgp.Marshal(l)

	if err != nil {
		slog.Error("Error marshalling MsgPack", "error", err)
		return nil
	}

	return data
}

func (l *DLQData) FromMsgPack(data []byte) error {
	err := msgp.Unmarshal(data, l)

	if err != nil {
		slog.Error("Error unmarshalling MsgPack", "error", err)
		return err
	}

	return nil
}
