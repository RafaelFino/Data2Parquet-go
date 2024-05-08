package config

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
)

type Config struct {
	Debug        bool              `json:"debug,omitempty"`
	LogPath      string            `json:"log_path"`
	WriterConfig map[string]string `json:"writer_config"`
	WriterName   string            `json:"writer_name"`
	ServerConfig *Server           `json:"server_config,omitempty"`
}

func ConfigFromJSON(data string) (*Config, error) {
	config := &Config{}
	err := json.Unmarshal([]byte(data), config)
	if err != nil {
		return nil, err
	}

	slog.Debug("[config] loaded", "data", config.ToString())

	return config, nil
}

func (c *Config) ToJSON() string {
	data, err := json.MarshalIndent(c, "", " ")
	if err != nil {
		return ""
	}
	return string(data)
}

func ConfigClientFromFile(filename string) (*Config, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	return ConfigFromJSON(string(data))
}

func (c *Config) ToString() string {
	return fmt.Sprintf("%+v", c)
}
