package config

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strings"
)

type Config struct {
	Address               string `json:"address,omitempty"`
	BufferSize            int    `json:"buffer_size"`
	BufferType            string `json:"buffer_type"`
	Debug                 bool   `json:"debug,omitempty"`
	FlushInterval         int    `json:"flush_interval"`
	LogPath               string `json:"log_path"`
	Port                  int    `json:"port,omitempty"`
	RedisDataPrefix       string `json:"redis_data_prefix,omitempty"`
	RedisDB               int    `json:"redis_db,omitempty"`
	RedisHost             string `json:"redis_host,omitempty"`
	RedisKeys             string `json:"redis_keys,omitempty"`
	LedisPath             string `json:"ledis_path,omitempty"`
	RedisPassword         string `json:"redis_password,omitempty"`
	RedisSkipFlush        bool   `json:"redis_skip_flush,omitempty"`
	WriterCompressionType string `json:"writer_compression_type,omitempty"`
	WriterFilePath        string `json:"writer_file_path,omitempty"`
	WriterRowGroupSize    int64  `json:"writer_row_group_size,omitempty"`
	WriterType            string `json:"writer_type"`
}

var keys = []string{
	"BufferSize",
	"BufferType",
	"Debug",
	"FlushInterval",
	"LedisPath",
	"LogPath",
	"RedisDataPrefix",
	"RedisDB",
	"RedisHost",
	"RedisKeys",
	"RedisPassword",
	"RedisSkipFlush",
	"WriterCompressionType",
	"WriterFilePath",
	"WriterRowGroupSize",
	"WriterType",
}

func NewConfig() *Config {
	ret := &Config{}

	ret.SetDefaults()

	return ret
}

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
			c.Debug = strings.ToLower(value) == "true"
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
		case "writer_row_group_size":
			c.WriterRowGroupSize = 128 * 1024 * 1024
			fmt.Sscanf(value, "%d", &c.WriterRowGroupSize)
		case "address":
			c.Address = value
		case "port":
			c.Port = 0
			fmt.Sscanf(value, "%d", &c.Port)
		case "redis_host":
			c.RedisHost = value
		case "redis_password":
			c.RedisPassword = value
		case "redis_db":
			c.RedisDB = 0
			fmt.Sscanf(value, "%d", &c.RedisDB)
		case "redis_flush":
			c.RedisSkipFlush = strings.ToLower(value) == "true"
		case "redis_data_prefix":
			c.RedisDataPrefix = value
		case "redis_keys":
			c.RedisKeys = value
		case "ledis_path":
			c.LedisPath = value

		default:
			slog.Warn("Unknown key", "key", key, "value", value, "module", "config", "function", "Set")
		}
	}
	return nil
}

func (c *Config) Get() map[string]interface{} {
	ret := make(map[string]interface{})

	ret["Address"] = c.Address
	ret["BufferSize"] = c.BufferSize
	ret["BufferType"] = c.BufferType
	ret["Debug"] = c.Debug
	ret["FlushInterval"] = c.FlushInterval
	ret["LogPath"] = c.LogPath
	ret["LedisPath"] = c.LedisPath
	ret["Port"] = c.Port
	ret["RedisDataPrefix"] = c.RedisDataPrefix
	ret["RedisDB"] = c.RedisDB
	ret["RedisHost"] = c.RedisHost
	ret["RedisKeys"] = c.RedisKeys
	ret["RedisPassword"] = c.RedisPassword
	ret["RedisSkipFlush"] = c.RedisSkipFlush
	ret["WriterCompressionType"] = c.WriterCompressionType
	ret["WriterFilePath"] = c.WriterFilePath
	ret["WriterRowGroupSize"] = c.WriterRowGroupSize
	ret["WriterType"] = c.WriterType

	return ret
}

func (c *Config) SetDefaults() {
	if c.LogPath == "" {
		c.LogPath = "./logs"
	}

	if c.WriterType == "" {
		c.WriterType = "file"
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

	if c.BufferType == "" {
		c.BufferType = "mem"
	}

	if c.BufferSize < 1 {
		c.BufferSize = 1000
	}

	if c.FlushInterval < 60 {
		c.FlushInterval = 60
	}

	if len(c.RedisKeys) == 0 {
		c.RedisKeys = "keys"
	}

	if c.RedisDB < 0 {
		c.RedisDB = 0
	}

	if len(c.RedisDataPrefix) == 0 {
		c.RedisDataPrefix = "data"
	}
}
