package domain

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/oklog/ulid"

	"data2parquet/pkg/config"
	"data2parquet/pkg/logger"
)

var slog = logger.GetLogger()

const KeySeparator = "-"

var IgnoredFields = make(map[string]any)

type Record interface {
	GetInfo() RecordInfo
	Decode(data map[string]interface{})
	Key() string
	ToJson() string
	FromJson(data string) error
	ToString() string
	ToMsgPack() []byte
	FromMsgPack(data []byte) error
	GetData() map[string]interface{}
	UpdateInfo()
}

type RecordInfo interface {
	RecordType() string
	Key() string
	Service() string
	Domain() string
	Capability() string
	Target(id string, hash string) string
}

func NewRecordInfoFromKey(recordType string, key string) RecordInfo {
	if strings.Contains(key, config.RecordTypeDynamic) {
		return NewDynamicInfoFromKey(key)
	}

	return NewLogInfoFromKey(key)
}

func NewRecord(recordType string, data map[string]interface{}) Record {
	var ret Record
	switch strings.ToLower(recordType) {
	case config.RecordTypeDynamic:
		ret = NewDynamic(data)
	default:
		ret = NewLog(data)
	}

	return ret
}

func NewObj(t string) Record {
	switch t {
	case config.RecordTypeDynamic:
		return &Dynamic{}
	default:
		return &Log{}
	}
}

var emptyString string = ""

func GetStringP(s interface{}) *string {
	if s == nil {
		return &emptyString
	}

	ret := fmt.Sprintf("%s", s)

	if len(ret) == 0 {
		return &emptyString
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

func GetInt64(n any) int64 {
	if n == nil {
		return 0
	}

	switch v := n.(type) {
	case int64:
		return v
	case int:
		return int64(v)
	case float64:
		return int64(v)
	case uint64:
		return int64(v)
	case uint:
		return int64(v)
	case int32:
		return int64(v)
	case uint32:
		return int64(v)
	case int16:
		return int64(v)
	case uint16:
		return int64(v)
	case int8:
		return int64(v)
	case uint8:
		return int64(v)
	case float32:
		return int64(v)
	default:
		return 0
	}
}

var entropy = rand.New(rand.NewSource(time.Now().UnixNano()))

func MakeID() string {
	ret := ulid.MustNew(ulid.Timestamp(time.Now()), entropy).String()
	return ret
}

func GetMD5Sum(data []byte) string {
	hash := md5.Sum(data)
	return hex.EncodeToString(hash[:])
}
