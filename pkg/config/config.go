package config

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
)

type Config struct {
	Debug                 bool   `json:"debug,omitempty"`
	LogPath               string `json:"log_path"`
	WriterType            string `json:"writer_type"`
	BufferType            string `json:"buffer_type"`
	BufferSize            int    `json:"buffer_size"`
	FlushInterval         int    `json:"flush_interval"`
	WriterFilePath        string `json:"writer_file_path,omitempty"`
	WriterCompressionType string `json:"writer_compression_type,omitempty"`
	WriterRowGroupSize    int64  `json:"writer_row_group_size,omitempty"`
	Address               string `json:"address,omitempty"`
	Port                  int    `json:"port,omitempty"`
}

var keys = []string{"Debug", "LogPath", "WriterType", "BufferType", "BufferSize", "FlushInterval", "WriterFilePath", "WriterCompressionType", "WriterRowGroupSize"}

func ConfigFromJSON(data string) (*Config, error) {
	config := &Config{}
	err := json.Unmarshal([]byte(data), config)
	if err != nil {
		return nil, err
	}

	config.SetDefaults()

	slog.Debug("Loaded", "data", config.ToString(), "module", "config", "function", "ConfigFromJSON")

	return config, nil
}

func ConfigClientFromFile(filename string) (*Config, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	return ConfigFromJSON(string(data))
}

func (c *Config) ToJSON() string {
	data, err := json.MarshalIndent(c, "", " ")
	if err != nil {
		return ""
	}
	return string(data)
}

func (c *Config) ToString() string {
	return fmt.Sprintf("%+v", c)
}

func (c *Config) GetKeys() []string {
	return keys
}

func (c *Config) WriteToFile(filename string) error {
	data := c.ToJSON()
	err := os.WriteFile(filename, []byte(data), 0644)
	if err != nil {
		slog.Error("Error writing to file", "error", err, "module", "config", "function", "WriteToFile")
		return err
	}
	return nil
}
func (c *Config) Set(cfg map[string]string) error {
	for key, value := range cfg {
		switch key {
		case "debug":
			c.Debug = value == "true"
		case "log_path":
			c.LogPath = value
		case "writer_type":
			c.WriterType = value
		case "buffer_type":
			c.BufferType = value
		case "flush_interval":
			c.FlushInterval = 60
			fmt.Sscanf(value, "%d", &c.FlushInterval)
		case "buffer_size":
			c.BufferSize = 1000
			fmt.Sscanf(value, "%d", &c.BufferSize)
		case "writer_file_path":
			c.WriterFilePath = value
		case "writer_compression_type":
			c.WriterCompressionType = value
		default:
			slog.Warn("Unknown key", "key", key, "value", value, "module", "config", "function", "Set")
		}
	}
	return nil
}

func (c *Config) SetDefaults() {
	if c.LogPath == "" {
		c.LogPath = "./logs"
	}

	if c.WriterType == "" {
		c.WriterType = "file"
	}

	if c.BufferType == "" {
		c.BufferType = "mem"
	}

	if c.BufferSize < 1 {
		c.BufferSize = 1000
	}

	if c.FlushInterval < 60 {
		c.FlushInterval = 60
	}

	if c.WriterFilePath == "" {
		c.WriterFilePath = "./out"
	}

	if c.WriterCompressionType == "" {
		c.WriterCompressionType = "snappy"
	}

	if c.WriterRowGroupSize < 1024 {
		c.WriterRowGroupSize = 128 * 1024 * 1024 //128M
	}
}
