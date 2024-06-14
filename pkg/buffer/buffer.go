package buffer

import (
	"bytes"
	"context"
	"data2parquet/pkg/config"
	"data2parquet/pkg/domain"
	"data2parquet/pkg/logger" //"log/slog"
	"time"

	msgp "github.com/vmihailenco/msgpack/v5"
)

var slog = logger.GetLogger()

type Buffer interface {
	Close() error
	Push(key string, item domain.Record) (int, error)
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
	CheckLock(key string) bool
}

func New(ctx context.Context, cfg *config.Config) Buffer {
	var ret Buffer
	switch cfg.BufferType {
	case config.BufferTypeRedis:
		ret = NewRedis(ctx, cfg, nil)
		if ret == nil {
			slog.Error("Error creating Redis buffer, using memory buffer instead")
			ret = NewMem(ctx, cfg)
		}
	case config.BufferTypeMem:
		ret = NewMem(ctx, cfg)
	default:
		ret = NewMem(ctx, cfg)
	}

	return ret
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
