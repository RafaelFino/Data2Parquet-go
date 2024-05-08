package main

import (
	"log/slog"
	"unsafe"

	"github.com/fluent/fluent-bit-go/output"
)

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

	// Gets called only once for each instance you have configured.
	return output.FLB_OK
}

//export FLBPluginFlushCtx
func FLBPluginFlushCtx(ctx, data unsafe.Pointer, length C.int, tag *C.char) int {
	slog.Debug("Flushing context")

	// Gets called with a batch of records to be written to an instance.
	return output.FLB_OK
}

//export FLBPluginExit
func FLBPluginExit() int {
	slog.Info("Exiting plugin")
	return output.FLB_OK
}
