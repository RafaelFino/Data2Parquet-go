package main

import (
	"C"
	"context"
	"fmt"
	"strings"
	"time"
	"unsafe"

	"github.com/fluent/fluent-bit-go/output"

	"data2parquet/pkg/config"
	"data2parquet/pkg/domain"
	"data2parquet/pkg/logger" // "log/slog"
	"data2parquet/pkg/receiver"
)
import "runtime/debug"

var cfg = &config.Config{}
var rcv *receiver.Receiver
var ctx = context.Background()
var slog = logger.GetLogger()

func main() {
	slog.Info("Starting plugin")
}

//export FLBPluginRegister
func FLBPluginRegister(def unsafe.Pointer) int {
	slog.Info("Registering plugin")

	return output.FLBPluginRegister(def, "out_parquet", "Fluent Bit Parquet Output")
}

//export FLBPluginInit
func FLBPluginInit(plugin unsafe.Pointer) int {
	slog.Info("Initializing plugin")

	cfgMap := make(map[string]string, 0)

	for _, key := range cfg.GetKeys() {
		val := output.FLBPluginConfigKey(plugin, key)
		if len(val) != 0 {
			cfgMap[key] = val
		}
	}

	err := cfg.Set(cfgMap)

	if err != nil {
		slog.Error("Error setting config", "error", err)
		return output.FLB_ERROR
	}

	if cfg.Debug {
		slog.Info("Debug mode enabled")
		slog.SetLogLoggerLevel(logger.LevelDebug)
	}

	rcv = receiver.NewReceiver(ctx, cfg)

	slog.Info("Plugin initialized")
	return output.FLB_OK
}

//export FLBPluginFlushCtx
func FLBPluginFlushCtx(ctx, data unsafe.Pointer, length C.int, tag *C.char) int {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("Recovered in FLBPluginFlushCtx", "result", r, "stack", string(debug.Stack()))
		}
	}()

	var ret int
	var ts interface{}
	var record map[interface{}]interface{}

	dec := output.NewDecoder(data, int(length))

	if dec == nil {
		slog.Error("error to create fluent decoder, aborting flush process")
		return output.FLB_ERROR
	}

	for {
		ret, ts, record = output.GetRecord(dec)

		if ret != 0 {
			slog.Debug("no records to process", "ret", ret)
			break
		}

		var timestamp time.Time
		switch t := ts.(type) {
		case output.FLBTime:
			timestamp = ts.(output.FLBTime).Time
		case uint64:
			timestamp = time.Unix(int64(t), 0)
		default:
			slog.Warn("time provided invalid, defaulting to now.")
			timestamp = time.Now()
		}

		logData := CreateDataMap(record, timestamp, C.GoString(tag))

		if !IsLogRecord(logData) {
			slog.Warn("Invalid log record", "record", logData)
			continue
		}

		record := domain.NewLog(logData)

		err := rcv.Write(record)

		if err != nil {
			slog.Error("Error writing record", "error", err)
			return output.FLB_ERROR
		}
	}

	return output.FLB_OK
}

//export FLBPluginExit
func FLBPluginExit() int {
	slog.Info("Exiting plugin")
	err := rcv.Close()

	if err != nil {
		slog.Error("Error on try close receiver", "err", err)
		return output.FLB_ERROR
	}

	return output.FLB_OK
}

func IsLogRecord(data map[string]interface{}) bool {
	var ret bool = true

	if ret {
		_, okMsg := data["msg"]
		_, okMessage := data["message"]
		_, okLog := data["log"]

		ret = okMsg || okMessage || okLog
	}

	if ret {
		_, okLevel := data["level"]
		_, okLvl := data["lvl"]

		ret = ret && (okLevel || okLvl)
	}

	if ret {
		_, okTime := data["time"]
		_, okTimestamp := data["timestamp"]
		_, okWhen := data["when"]

		ret = ret && (okTime || okTimestamp || okWhen)
	}

	return ret
}

func CreateDataMap(data map[interface{}]interface{}, tm time.Time, tag string) map[string]any {
	logData := make(map[string]any)

	logData["fluent-time"] = tm.Format(time.RFC3339Nano)
	logData["fluent-tag"] = tag

	for k, v := range data {
		key := strings.ToLower(strings.ReplaceAll(fmt.Sprint(k), "_", "-"))

		var value interface{}

		switch fmt.Sprintf("%T", v) {
		case "[]uint8":
			value = string(v.([]byte))
		case "bool":
			value = v.(bool)
		case "uint64":
			value = v.(uint64)
		case "int64":
			value = v.(int64)
		case "float64":
			value = v.(float64)
		case "string":
			value = v.(string)
		case "int":
			value = v.(int)
		case "map[interface {}]interface {}":
			if key == "args" ||
				key == "context" ||
				key == "fields" ||
				key == "metadata" ||
				key == "properties" ||
				key == "trace" ||
				key == "data" ||
				key == "details" ||
				key == "trace-attributes" {
				args := make(map[string]any)
				for arg_key, arg_val := range v.(map[interface{}]interface{}) {
					args[fmt.Sprint(arg_key)] = arg_val
				}
				value = args
			} else {
				value = fmt.Sprintf("%v", v)
			}
		case "[]interface {}":
			tags := make([]string, len(v.([]interface{})))
			for _, tag := range v.([]interface{}) {
				item := string(tag.([]byte))
				if len(item) > 0 {
					tags = append(tags, item)
				}
			}
			value = tags
		default:
			value = fmt.Sprintf("%s", v)
		}
		logData[key] = value
	}

	return logData
}
