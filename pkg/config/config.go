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
	// BufferSize is the size of the buffer. Default is 1000. Json tag is "buffer_size"
	// BufferType is the type of buffer to use. Can be "mem" or "redis". Default is "mem". Json tag is "buffer_type"
	// Debug is the debug flag. Default is false. Json tag is "debug"
	// FlushInterval is the interval to flush the buffer. Default is 60. This value is in seconds. Json tag is "flush_interval"
	// LogPath is the path to the log files. Default is "./logs". Json tag is "log_path"
	// Port is the port to listen on. Default is 0. Json tag is "port"
	// RecoveryAttempts is the number of recovery attempts. Default is 0. Json tag is "recovery_attempts", dependency on TryAutoRecover
	// RedisDataPrefix is the prefix to use for the redis keys. Default is "data". Json tag is "redis_data_prefix"
	// RedisDB is the redis database to use. Default is 0. Json tag is "redis_db"
	// RedisHost is the redis host to connect to. Default is "". Json tag is "redis_host"
	// RedisKeys is the keys to use for the redis buffer. Default is "keys". Json tag is "redis_keys"
	// RedisPassword is the redis password to use. Default is "". Json tag is "redis_password"
	// RedisRecoveryKey is the key to use for the dead letter queue. Default is "". Json tag is "redis_dlq_key"
	// RedisSkipFlush is the flag to skip flushing the redis buffer. Default is false. Json tag is "redis_skip_flush"
	// S3AccessKey is the access key to use for S3. Default is "". Json tag is "s3_access_key"
	// S3BucketName is the bucket name to use for S3. Default is "". Json tag is "s3_bucket_name"
	// S3Region is the region to use for S3. Default is "". Json tag is "s3_region"
	// TryAutoRecover is the flag to try to auto recover. Default is false. Json tag is "try_auto_recover"
	// WriterCompressionType is the compression type to use for the writer. Default is "snappy". This field can be "snappy", "gzip", or "none". Json tag is "writer_compression_type"
	// WriterFilePath is the path to write the files to. Default is "./out". Json tag is "writer_file_path"
	// WriterRowGroupSize is the size of the row group. Default is 128M. This value is in bytes. Json tag is "writer_row_group_size"
	// WriterType is the type of writer to use. Default is "file". This field can be "file" or "s3". Json tag is "writer_type"
	Address               string `json:"address,omitempty"`
	BufferSize            int    `json:"buffer_size"`
	BufferType            string `json:"buffer_type"`
	Debug                 bool   `json:"debug,omitempty"`
	FlushInterval         int    `json:"flush_interval"`
	LogPath               string `json:"log_path"`
	JsonSchemaPath        string `json:"json_schema_path,omitempty"`
	Port                  int    `json:"port,omitempty"`
	RecordType            string `json:"record_type"`
	RecoveryAttempts      int    `json:"recovery_attempts,omitempty"`
	RedisDataPrefix       string `json:"redis_data_prefix,omitempty"`
	RedisDB               int    `json:"redis_db,omitempty"`
	RedisHost             string `json:"redis_host,omitempty"`
	RedisKeys             string `json:"redis_keys,omitempty"`
	RedisPassword         string `json:"redis_password,omitempty"`
	RedisRecoveryKey      string `json:"redis_recovery_key,omitempty"`
	RedisSkipFlush        bool   `json:"redis_skip_flush,omitempty"`
	RedisDLQPrefix        string `json:"redis_dlq_prefix,omitempty"`
	S3BuketName           string `json:"s3_bucket_name"`
	S3Region              string `json:"s3_region"`
	S3StorageClass        string `json:"s3_storage_class"`
	TryAutoRecover        bool   `json:"try_auto_recover,omitempty"`
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
	"LogPath",
	"JsonSchemaPath",
	"RecordType",
	"RecoveryAttempts",
	"RedisDataPrefix",
	"RedisDB",
	"RedisHost",
	"RedisKeys",
	"RedisPassword",
	"RedisRecoveryKey",
	"RedisSQLPrefix",
	"RedisSkipFlush",
	"S3BucketName",
	"S3Region",
	"S3StorageClass",
	"TryAutoRecover",
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
		case "try_auto_recover":
			c.TryAutoRecover = strings.ToLower(value) == "true"
		case "recovery_attempts":
			c.RecoveryAttempts = 0
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
		case "json_schema_path":
			c.JsonSchemaPath = value
		case "record_type":
			c.RecordType = value
		case "redis_dlq_prefix":
			c.RedisDLQPrefix = value

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
	ret["JsonSchemaPath"] = c.JsonSchemaPath
	ret["LogPath"] = c.LogPath
	ret["Port"] = c.Port
	ret["RecordType"] = c.RecordType
	ret["RecoveryAttempts"] = c.RecoveryAttempts
	ret["RedisDataPrefix"] = c.RedisDataPrefix
	ret["RedisDB"] = c.RedisDB
	ret["RedisHost"] = c.RedisHost
	ret["RedisKeys"] = c.RedisKeys
	ret["RedisPassword"] = c.RedisPassword
	ret["RedisRecoveryKey"] = c.RedisRecoveryKey
	ret["RedisSQLPrefix"] = c.RedisDLQPrefix
	ret["RedisSkipFlush"] = c.RedisSkipFlush
	ret["S3BucketName"] = c.S3BuketName
	ret["S3Region"] = c.S3Region
	ret["S3StorageClass"] = c.S3StorageClass
	ret["TryAutoRecover"] = c.TryAutoRecover
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

	if c.BufferSize < 100 {
		c.BufferSize = 100
	}

	if c.FlushInterval < 60 {
		c.FlushInterval = 60
	}

	if len(c.RedisKeys) == 0 {
		c.RedisKeys = "keys"
	}

	if len(c.RedisDLQPrefix) == 0 {
		c.RedisDLQPrefix = "dlq"
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

	if c.RecoveryAttempts < 0 {
		c.RecoveryAttempts = 0
	}

	c.RecordType = strings.ToLower(c.RecordType)
}
