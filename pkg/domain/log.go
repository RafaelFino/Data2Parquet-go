package domain

import "fmt"

type Log struct {
}

func NewLog() Record {
	return &Log{}
}

func (l *Log) SetSchema(schema string) error {
	return nil
}

func (l *Log) ToString() string {
	return fmt.Sprintf("%+v", l)
}

func (l *Log) Decode(data map[interface{}]interface{}) error {
	return nil
}

func (l *Log) Headers() []string {
	return nil
}

func (l *Log) Key() string {
	return ""
}
