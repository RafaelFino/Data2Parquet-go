package config_test

import (
	"data2parquet/pkg/config"
	"testing"
)

func TestNewConfig(t *testing.T) {
	t.Log("Testing NewConfig")
	cfg := config.NewConfig()

	if cfg == nil {
		t.Error("Config is nil")
		return
	}

	if cfg.BufferType != "mem" {
		t.Error("BufferType should be mem")
	}

	if cfg.BufferSize != 1000 {
		t.Error("BufferSize should be 1000")
	}

	if cfg.WriterType != "file" {
		t.Error("WriterType should be file")
	}

	if cfg.WriterFilePath == "" {
		t.Error("WriterFilePath should not be empty")
	}

	if cfg.WriterCompressionType != "snappy" {
		t.Error("WriterCompressionType should be snappy")
	}

	if cfg.WriterRowGroupSize == 0 {
		t.Error("WriterRowGroupSize should greater than 0")
	}

	if cfg.RedisKeys != "keys" {
		t.Error("RedisKeys should be keys")
	}

	json := cfg.ToJSON()

	if json == "" {
		t.Error("ToJSON should not return empty string")
	}

	if cfg.ToString() == "" {
		t.Error("ToString should not return empty string")
	}

	keys := cfg.GetKeys()

	if len(keys) == 0 {
		t.Error("GetKeys should not return empty slice")
	}

	if err := cfg.WriteToFile("/tmp/config.json"); err != nil {
		t.Error("WriteToFile should not return error")
	}

	cfg2, err := config.ConfigClientFromFile("/tmp/config.json")

	if err != nil {
		t.Error("ConfigClientFromFile should not return error")
	}

	if cfg2 == nil || cfg == nil || cfg2.ToJSON() != cfg.ToJSON() {
		t.Error("ToJSON should be same")
	}

	if cfg2 == nil || cfg == nil || cfg2.ToString() != cfg.ToString() {
		t.Error("ToString should be same")
	}

	if cfg2 == nil || cfg == nil || len(cfg2.GetKeys()) != len(cfg.GetKeys()) {
		t.Error("GetKeys should be same")
	}
}
