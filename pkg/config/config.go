package config

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
)

type Config struct {
	Debug          bool   `json:"debug,omitempty"`
	LogPath        string `json:"log_path"`
	WriterType     string `json:"writer_type"`
	BufferType     string `json:"buffer_type"`
	BufferSize     int    `json:"buffer_size"`
	FlushInterval  int    `json:"flush_interval"`
	Address        string `json:"address,omitempty"`
	Port           int    `json:"port,omitempty"`
	WriterFilePath string `json:"writer_file_path,omitempty"`
}

var keys = []string{"debug", "log_path", "writer_type", "address", "port", "buffer_type", "buffer_size", "flush_interval", "writer_file_path"}

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

func (c *Config) Set(cfg map[string]string) error {
	for key, value := range cfg {
		switch key {
		case "debug":
			c.Debug = value == "true"
		case "log_path":
			c.LogPath = value
		case "writer_type":
			c.WriterType = value
		case "address":
			c.Address = value
		case "port":
			c.Port = 0
			fmt.Sscanf(value, "%d", &c.Port)
		case "buffer_type":
			c.BufferType = value
		case "flush_interval":
			c.FlushInterval = 60
			fmt.Sscanf(value, "%d", &c.FlushInterval)
		case "buffer_size":
			c.BufferSize = 1000
			fmt.Sscanf(value, "%d", &c.BufferSize)
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

	if c.BufferSize == 0 {
		c.BufferSize = 1000
	}

	if c.FlushInterval == 0 {
		c.FlushInterval = 1
	}

	if c.WriterFilePath == "" {
		c.WriterFilePath = "./out"
	}
}
