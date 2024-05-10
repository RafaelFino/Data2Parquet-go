package main

import (
	"data2parquet/pkg/config"
	"data2parquet/pkg/server"
	"fmt"
	"io"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	rotatelogs "github.com/lestrrat/go-file-rotatelogs"
)

func main() {
	PrintLogo()
	if len(os.Args) < 2 {
		fmt.Print("Usage: http-server <config_file>\n")
		os.Exit(1)
	}

	configFile := os.Args[1]

	cfg, err := config.ConfigClientFromFile(configFile)
	if err != nil {
		fmt.Printf("Error loading config file: %s", err)
		os.Exit(1)
	}

	err = initLogger(cfg.LogPath)
	if err != nil {
		fmt.Printf("Error opening log file: %s, using stdout", err)
		log.SetOutput(os.Stdout)
	}

	slog.Debug("Starting", "config", cfg.ToString(), "module", "main", "function", "main")

	fmt.Printf("Starting...")

	server := server.NewServer(cfg)
	go server.Run()

	quitChannel := make(chan os.Signal, 1)
	signal.Notify(quitChannel, syscall.SIGINT, syscall.SIGTERM)
	<-quitChannel

	slog.Info("Stopping...")
}

func initLogger(path string) error {
	if err := os.Mkdir(path, 0755); !os.IsExist(err) {
		fmt.Printf("Error creating directory %s: %s", path, err)
		return err
	}

	writer, err := rotatelogs.New(
		fmt.Sprintf("%s/products-%s.log", path, "%Y%m%d"),
		rotatelogs.WithMaxAge(24*time.Hour),
		rotatelogs.WithRotationTime(time.Hour),
		rotatelogs.WithRotationCount(30), //30 days
	)

	if err != nil {
		fmt.Printf("Failed to Initialize Log File %s", err)
		return err
	}

	multi := io.MultiWriter(writer, os.Stdout)
	logger := slog.New(slog.NewJSONHandler(multi, nil))

	slog.SetDefault(logger)

	return nil
}

func PrintLogo() {
	fmt.Print(`
###############################
#                             #
#  Log2Parquet - HTTP Server  #
#                             #
###############################
 
`)
}
