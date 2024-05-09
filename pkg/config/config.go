package config

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
)

type Config struct {
	Debug          bool              `json:"debug,omitempty"`
	LogPath        string            `json:"log_path"`
	WriterConfig   map[string]string `json:"writer_config"`
	WriterName     string            `json:"writer_name"`
	BufferName     string            `json:"buffer_name"`
	BufferConfig   map[string]string `json:"buffer_config"`
	BufferPageSize int               `json:"buffer_page_size"`
	Address        string            `json:"address,omitempty"`
	Port           int               `json:"port,omitempty"`
}

var keys = []string{"debug", "log_path", "writer_config", "writer_name", "address", "port", "buffer_name", "buffer_config"}

func ConfigFromJSON(data string) (*Config, error) {
	config := &Config{}
	err := json.Unmarshal([]byte(data), config)
	if err != nil {
		return nil, err
	}

	slog.Debug("[config] loaded", "data", config.ToString())

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
		case "writer_config":
			c.WriterConfig = map[string]string{}
			err := json.Unmarshal([]byte(value), &c.WriterConfig)
			if err != nil {
				return err
			}
		case "writer_name":
			c.WriterName = value
		case "address":
			c.Address = value
		case "port":
			c.Port = 0
			fmt.Sscanf(value, "%d", &c.Port)
		case "buffer_name":
			c.BufferName = value
		case "buffer_config":
			c.BufferConfig = map[string]string{}
			err := json.Unmarshal([]byte(value), &c.BufferConfig)
			if err != nil {
				return err
			}
		case "buffer_page_size":
			c.BufferPageSize = 0
			fmt.Sscanf(value, "%d", &c.BufferPageSize)
		default:
			slog.Warn("[config] Unknown key", "key", key)
		}
	}
	return nil
}
