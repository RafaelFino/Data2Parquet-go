package domain

import "fmt"

type Log struct {
}

func (l *Log) ToString() string {
	return fmt.Sprintf("%+v", l)
}
