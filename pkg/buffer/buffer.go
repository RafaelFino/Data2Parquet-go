package buffer

import "data2parquet/pkg/domain"

type Buffer interface {
	Push(key string, item *domain.Record) error
	PushMany(key string, items []*domain.Record) error
	Get(key string) []*domain.Record
	Keys() []string
}
