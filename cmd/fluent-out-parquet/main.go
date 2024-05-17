package main

import (
	"C"
	"log/slog"
	"time"
	"unsafe"

	"github.com/fluent/fluent-bit-go/output"

	"data2parquet/pkg/config"
	"data2parquet/pkg/receiver"

	"context"
	"fmt"
	"os"
	"strings"

	"github.com/phsym/console-slog"
)
import "data2parquet/pkg/domain"

var cfg = &config.Config{}
var rcv *receiver.Receiver
var ctx = context.Background()

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
	logLevel := slog.LevelInfo.Level()

	logHandler := console.NewHandler(os.Stderr, &console.HandlerOptions{Level: logLevel})
	logger := slog.New(logHandler)

	logger.Info("Initializing plugin")

	cfgMap := make(map[string]string, 0)

	for _, key := range cfg.GetKeys() {
		val := output.FLBPluginConfigKey(plugin, key)
		if len(val) != 0 {
			cfgMap[key] = val
		}
	}

	err := cfg.Set(cfgMap)

	if err != nil {
		logger.Error("Error setting config", "error", err)
		return output.FLB_ERROR
	}

	if cfg.Debug {
		slog.SetLogLoggerLevel(slog.LevelDebug.Level())
	} else {
		slog.SetLogLoggerLevel(slog.LevelInfo.Level())
	}

	slog.SetDefault(logger)

	rcv = receiver.NewReceiver(ctx, cfg)

	// Gets called only once for each instance you have configured.
	return output.FLB_OK
}

//export FLBPluginFlushCtx
func FLBPluginFlushCtx(ctx, data unsafe.Pointer, length C.int, tag *C.char) int {
	slog.Debug("Flushing context")

	var ret int
	var ts interface{}
	var record map[interface{}]interface{}

	dec := output.NewDecoder(data, int(length))

	for {
		ret, ts, record = output.GetRecord(dec)
		if ret != 0 {
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
	_, ok := data["message"]
	ret = ok && ret

	_, ok = data["level"]
	ret = ok && ret

	_, ok = data["time"]
	return ok && ret
}

func CreateDataMap(data map[interface{}]interface{}, tm time.Time, tag string) map[string]any {
	logData := make(map[string]any)

	logData["fluent_timestamp"] = tm
	logData["fluent_tag"] = tag

	for k, v := range data {
		key := strings.ToLower(fmt.Sprintf("%v", k))

		var value interface{}

		t := fmt.Sprintf("%T", v)

		switch t {
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
			if key == "args" {
				args := make(map[string]string)
				for arg_key, arg_val := range v.(map[interface{}]interface{}) {
					args[arg_key.(string)] = string(arg_val.([]byte))
				}
				value = args
			} else {
				value = fmt.Sprintf("%v", v)
			}
		case "[]interface {}":
			if key == "tags" || key == "trace_ip" {
				tags := make([]string, 0)
				for _, tag := range v.([]interface{}) {
					tags = append(tags, string(tag.([]byte)))
				}
				value = tags
			} else {
				value = fmt.Sprintf("%v", v)
			}
		default:
			value = fmt.Sprintf("%s", v)
		}
		logData[key] = value
	}

	return logData
}
