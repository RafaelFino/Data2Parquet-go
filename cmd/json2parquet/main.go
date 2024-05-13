package main

import (
	"data2parquet/pkg/config"
	"data2parquet/pkg/domain"
	"data2parquet/pkg/receiver"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/lmittmann/tint"
	"github.com/mattn/go-isatty"
)

func main() {
	start := time.Now()
	PrintLogo()

	var logLevel = slog.LevelInfo

	if len(os.Args) < 3 {
		fmt.Printf("Usage: json2parquet <config_file> <input.json>\n")
		os.Exit(1)
	}

	configFile := os.Args[1]
	cfg, err := config.ConfigClientFromFile(configFile)
	if err != nil {
		fmt.Printf("Error loading config file, %s", err)
		os.Exit(1)
	}

	if cfg.Debug {
		logLevel = slog.LevelDebug.Level()
	}

	logHandler := tint.NewHandler(os.Stdout, &tint.Options{
		NoColor:    !isatty.IsTerminal(os.Stdout.Fd()),
		Level:      logLevel,
		TimeFormat: time.RFC3339Nano,
		AddSource:  cfg.Debug,
	})

	logger := slog.New(logHandler)
	slog.SetDefault(logger)

	slog.Info("Starting...")

	file, err := os.Open(os.Args[2])

	if err != nil {
		slog.Error("Error opening file", "error", err, "file", os.Args[2])
		os.Exit(1)
	}

	defer file.Close()

	records, err := ReadJSON(file)

	slog.Info("Read records", "count", len(records), "duration", time.Since(start))
	start = time.Now()

	if err != nil {
		slog.Error("Error reading JSON file", "error", err)
		os.Exit(1)
	}

	rcv := receiver.NewReceiver(cfg)

	if rcv == nil {
		slog.Error("Error creating receiver")
		os.Exit(1)
	}

	for _, record := range records {
		err := rcv.Write(record)

		if err != nil {
			slog.Error("Error writing record", "error", err, "record", record)
		}
	}

	slog.Info("Records sent", "duration", time.Since(start), "count", len(records))
	start = time.Now()

	err = rcv.Flush()

	if err != nil {
		slog.Error("Error flushing records", "error", err)
	}

	err = rcv.Close()

	if err != nil {
		slog.Error("Error closing receiver", "error", err)
	}

	slog.Info("Finished", "duration", time.Since(start))

	os.Exit(0)
}

func PrintLogo() {
	fmt.Print(`
###############################
#                             #
#  Data2Parquet - Converter   #
#                             #
###############################
 
`)
}

func ReadJSON(file *os.File) ([]*domain.Record, error) {
	decoder := json.NewDecoder(file)

	data := map[string]interface{}{}

	if err := decoder.Decode(&data); err != nil {
		slog.Error("Error decoding JSON", "error", err)
		return nil, err
	}

	ret := make([]*domain.Record, 0)

	if lines, ok := data["logs"]; ok {
		records := lines.([]interface{})

		for _, r := range records {
			line := make(map[interface{}]interface{})
			for k, v := range r.(map[string]interface{}) {
				line[k] = v
			}

			ret = append(ret, domain.NewRecord(line))
		}
	}

	return ret, nil
}
