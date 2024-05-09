package buffer

import "data2parquet/pkg/domain"

type Buffer interface {
	Push(key string, item *domain.Line) error
	PushMany(key string, items []*domain.Line) error
	Get(key string) []*domain.Line
	Keys() []string
}
