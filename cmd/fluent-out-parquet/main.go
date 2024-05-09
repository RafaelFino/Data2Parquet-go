package main

import (
	"C"
	"log/slog"
	"time"
	"unsafe"

	"github.com/fluent/fluent-bit-go/output"

	"data2parquet/pkg/config"
	"data2parquet/pkg/domain"
	"data2parquet/pkg/receiver"
)

var cfg = &config.Config{}
var rcv *receiver.Receiver

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

	cfg = &config.Config{}
	cfgMap := make(map[string]string, 0)

	for _, key := range cfg.GetKeys() {
		val := output.FLBPluginConfigKey(plugin, key)
		if len(val) != 0 {
			cfgMap[key] = val
		} else {
			slog.Debug("Key not found", "key", key)
		}
	}

	cfg.Set(cfgMap)

	slog.Debug("Config loaded", "config", cfg.ToString())

	rcv = receiver.NewReceiver(cfg)

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
		// Extract Record
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

		record["fluent_timestamp"] = timestamp
		record["fluent_tag"] = C.GoString(tag)

		line := domain.NewLog()
		line.Decode(record)

		rcv.Write(line)
	}

	return output.FLB_OK
}

//export FLBPluginExit
func FLBPluginExit() int {
	slog.Info("Exiting plugin")
	return output.FLB_OK
}
