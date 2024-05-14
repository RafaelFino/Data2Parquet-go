package config

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strings"
)

/// Config is the configuration for the application
/// This struct is used to store the configuration for the application
/// The configuration can be loaded from a file or from a map
/// The configuration can be saved to a file
/// The configuration can be converted to a map
/// The configuration can be converted to a JSON string
/// The configuration can be converted to a string
/// The configuration can be set to the default values
/// The configuration can be set from a map
/// The configuration can be set from a JSON string
/// The configuration can be written to a file

type Config struct {
	// Address is the address to listen on. Default is "". Json tag is "address"
	Address string `json:"address,omitempty"`
	// BufferSize is the size of the buffer. Default is 1000. Json tag is "buffer_size"
	BufferSize int `json:"buffer_size"`
	// BufferType is the type of buffer to use. Can be "mem" or "redis". Default is "mem". Json tag is "buffer_type"
	BufferType string `json:"buffer_type"`
	// Debug is the debug flag. Default is false. Json tag is "debug"
	Debug bool `json:"debug,omitempty"`
	// FlushInterval is the interval to flush the buffer. Default is 60. This value is in seconds. Json tag is "flush_interval"
	FlushInterval int `json:"flush_interval"`
	// LogPath is the path to the log files. Default is "./logs". Json tag is "log_path"
	LogPath string `json:"log_path"`
	// Port is the port to listen on. Default is 0. Json tag is "port"
	Port int `json:"port,omitempty"`
	// RedisDataPrefix is the prefix to use for the redis keys. Default is "data". Json tag is "redis_data_prefix"
	RedisDataPrefix string `json:"redis_data_prefix,omitempty"`
	// RedisDB is the redis database to use. Default is 0. Json tag is "redis_db"
	RedisDB int `json:"redis_db,omitempty"`
	// RedisRecoveryKey is the key to use for the dead letter queue. Default is "". Json tag is "redis_dlq_key"
	RedisRecoveryKey string `json:"redis_recovery_key,omitempty"`
	// RedisHost is the redis host to connect to. Default is "". Json tag is "redis_host"
	RedisHost string `json:"redis_host,omitempty"`
	// RedisKeys is the keys to use for the redis buffer. Default is "keys". Json tag is "redis_keys"
	RedisKeys string `json:"redis_keys,omitempty"`
	// RedisPassword is the redis password to use. Default is "". Json tag is "redis_password"
	RedisPassword string `json:"redis_password,omitempty"`
	// RedisSkipFlush is the flag to skip flushing the redis buffer. Default is false. Json tag is "redis_skip_flush"
	RedisSkipFlush bool `json:"redis_skip_flush,omitempty"`
	// WriterCompressionType is the compression type to use for the writer. Default is "snappy". This field can be "snappy", "gzip", or "none". Json tag is "writer_compression_type"
	WriterCompressionType string `json:"writer_compression_type,omitempty"`
	// WriterFilePath is the path to write the files to. Default is "./out". Json tag is "writer_file_path"
	WriterFilePath string `json:"writer_file_path,omitempty"`
	// WriterRowGroupSize is the size of the row group. Default is 128M. This value is in bytes. Json tag is "writer_row_group_size"
	WriterRowGroupSize int64 `json:"writer_row_group_size,omitempty"`
	// WriterType is the type of writer to use. Default is "file". This field can be "file" or "s3". Json tag is "writer_type"
	WriterType string `json:"writer_type"`
	// S3AccessKey is the access key to use for S3. Default is "". Json tag is "s3_access_key"
	S3BuketName string `json:"s3_bucket_name"`
	// S3BucketName is the bucket name to use for S3. Default is "". Json tag is "s3_bucket_name"
	S3Region string `json:"s3_region"`
	// S3Region is the region to use for S3. Default is "". Json tag is "s3_region"
	S3StorageClass string `json:"s3_storage_class"`
}

var keys = []string{
	"BufferSize",
	"BufferType",
	"Debug",
	"FlushInterval",
	"LogPath",
	"RedisDataPrefix",
	"RedisDB",
	"RedisRecoveryKey",
	"RedisHost",
	"RedisKeys",
	"RedisPassword",
	"RedisSkipFlush",
	"WriterCompressionType",
	"WriterFilePath",
	"WriterRowGroupSize",
	"WriterType",
	"S3BucketName",
	"S3Region",
	"S3StorageClass",
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
		case "redis_recovery_key":
			c.RedisRecoveryKey = value
		case "redis_flush":
			c.RedisSkipFlush = strings.ToLower(value) == "true"
		case "redis_data_prefix":
			c.RedisDataPrefix = value
		case "redis_keys":
			c.RedisKeys = value
		case "s3_bucket_name":
			c.S3BuketName = value
		case "s3_region":
			c.S3Region = value
		case "s3_storage_class":
			c.S3StorageClass = value

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
	ret["Port"] = c.Port
	ret["RedisDataPrefix"] = c.RedisDataPrefix
	ret["RedisDB"] = c.RedisDB
	ret["RedisRecoveryKey"] = c.RedisRecoveryKey
	ret["RedisHost"] = c.RedisHost
	ret["RedisKeys"] = c.RedisKeys
	ret["RedisPassword"] = c.RedisPassword
	ret["RedisSkipFlush"] = c.RedisSkipFlush
	ret["WriterCompressionType"] = c.WriterCompressionType
	ret["WriterFilePath"] = c.WriterFilePath
	ret["WriterRowGroupSize"] = c.WriterRowGroupSize
	ret["WriterType"] = c.WriterType
	ret["S3BucketName"] = c.S3BuketName
	ret["S3Region"] = c.S3Region
	ret["S3StorageClass"] = c.S3StorageClass

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

	if c.BufferSize < 1000 {
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

	if len(c.RedisRecoveryKey) == 0 {
		c.RedisRecoveryKey = "recovery"
	}

	if len(c.S3StorageClass) == 0 {
		c.S3StorageClass = "STANDARD"
	}
}
