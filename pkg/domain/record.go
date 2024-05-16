package domain

import (
	"fmt"
	"strings"
	"time"

	"golang.org/x/exp/slog"
)

type Record interface {
	Key() string
	ToJson() string
	FromJson(data string) error
	ToString() string
	ToMsgPack() []byte
	FromMsgPack(data []byte) error
}

var RecordTypeLog = "log"
var RecordTypeDynamic = "dynamic"

var RecordTypes = map[string]int{
	RecordTypeLog:     1,
	RecordTypeDynamic: 2,
}

func NewRecord(recordType string, data map[interface{}]interface{}) Record {
	var ret Record
	switch strings.ToLower(recordType) {
	case RecordTypeDynamic:
		ret = NewDynamic(data)
	default:
		ret = NewLog(data)
	}

	return ret
}

func NewObj(t string) Record {
	switch t {
	case RecordTypeDynamic:
		return &Dynamic{}
	default:
		return &Log{}
	}
}

func GetStringP(s interface{}) *string {
	if s == nil {
		return nil
	}

	ret := fmt.Sprintf("%s", s)

	if len(ret) == 0 {
		return nil
	}

	return &ret
}

func TryParseRecordTime(v any) time.Time {
	ret := time.Now()

	if v == nil {
		return ret
	}

	val := fmt.Sprint(v)

	parsed, err := time.Parse(time.RFC3339Nano, val)
	if err == nil {
		ret = parsed
		return ret
	}

	parsed, err = time.Parse(time.RFC3339, val)
	if err == nil {
		ret = parsed
		return ret
	}

	parsed, err = time.Parse(time.UnixDate, val)
	if err == nil {
		ret = parsed
		return ret
	}

	parsed, err = time.Parse(time.Stamp, val)
	if err == nil {
		ret = parsed
		return ret
	}

	parsed, err = time.Parse(time.StampMilli, val)
	if err == nil {
		ret = parsed
		return ret
	}

	parsed, err = time.Parse(time.StampMicro, val)
	if err == nil {
		ret = parsed
		return ret
	}

	parsed, err = time.Parse(time.StampNano, val)
	if err == nil {
		ret = parsed
		return ret
	}

	parsed, err = time.Parse(time.RFC1123, val)
	if err == nil {
		ret = parsed
		return ret
	}

	parsed, err = time.Parse(time.RFC1123Z, val)
	if err == nil {
		ret = parsed
		return ret
	}

	parsed, err = time.Parse(time.RFC822, val)
	if err == nil {
		ret = parsed
		return ret
	}

	parsed, err = time.Parse(time.RFC822Z, val)
	if err == nil {
		ret = parsed
		return ret
	}

	parsed, err = time.Parse(time.RFC850, val)
	if err == nil {
		ret = parsed
		return ret
	}

	parsed, err = time.Parse(time.RubyDate, val)
	if err == nil {
		ret = parsed
		return ret
	}

	parsed, err = time.Parse(time.Kitchen, val)
	if err == nil {
		ret = parsed
		return ret
	}

	slog.Error("Error parsing time", "time", val)
	return ret
}
