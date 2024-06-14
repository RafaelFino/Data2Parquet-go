package main

import (
	"context"
	"data2parquet/pkg/config"
	"data2parquet/pkg/logger" // "log/slog"
	"data2parquet/pkg/server"
	"fmt"
	"os"
	"os/signal"
	"syscall"
)

var slog = logger.GetLogger()

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

	slog.Debug("Starting", "config", cfg.ToString(), "module", "main", "function", "main")

	fmt.Printf("Starting...")

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	server := server.NewServer(ctx, cfg)
	err = server.Run()

	if err != nil {
		slog.Error("Error running server", "module", "main", "function", "main", "error", err)
	}

	slog.Info("Stopping...")
}

func PrintLogo() {
	fmt.Print(`
################################
#                              #
#  Data2Parquet - HTTP Server  #
#                              #
################################
 
`)
}
