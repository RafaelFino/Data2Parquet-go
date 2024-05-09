package buffer

import "data2parquet/pkg/domain"

type Redis struct {
}

func NewRedis() Buffer {
	return &Redis{}
}

func (r *Redis) Push(key string, item *domain.Line) error {
	return nil
}

func (r *Redis) PushMany(key string, items []*domain.Line) error {
	return nil
}

func (r *Redis) Get(key string) []*domain.Line {
	return nil
}

func (r *Redis) Keys() []string {
	return nil
}
