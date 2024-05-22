package config

import (
	"data2parquet/pkg/domain"
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
	JsonSchemaPath        string `json:"json_schema_path,omitempty"`
	LogPath               string `json:"log_path"`
	Port                  int    `json:"port,omitempty"`
	RecordType            string `json:"record_type"`
	RecoveryAttempts      int    `json:"recovery_attempts,omitempty"`
	RedisDataPrefix       string `json:"redis_data_prefix,omitempty"`
	RedisDB               int    `json:"redis_db,omitempty"`
	RedisDLQPrefix        string `json:"redis_dlq_prefix,omitempty"`
	RedisHost             string `json:"redis_host,omitempty"`
	RedisKeys             string `json:"redis_keys,omitempty"`
	RedisLockPrefix       string `json:"redis_lock_prefix,omitempty"`
	RedisPassword         string `json:"redis_password,omitempty"`
	RedisRecoveryKey      string `json:"redis_recovery_key,omitempty"`
	S3BuketName           string `json:"s3_bucket_name"`
	S3Region              string `json:"s3_region"`
	S3RoleName            string `json:"s3_role_name,omitempty"`
	S3STSEndpoint         string `json:"s3_sts_endpoint,omitempty"`
	S3Endpoint            string `json:"s3_endpoint,omitempty"`
	S3Account             string `json:"s3_account,omitempty"`
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
	"RedisLockPrefix",
	"RedisPassword",
	"RedisRecoveryKey",
	"RedisSQLPrefix",
	"S3BucketName",
	"S3Region",
	"S3RoleName",
	"S3STSEndpoint",
	"S3Endpoint",
	"S3Account",
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
		case "Debug":
			c.Debug = strings.ToLower(value) == "true"
		case "LogPath":
			c.LogPath = value
		case "TryAutoRecover":
			c.TryAutoRecover = strings.ToLower(value) == "true"
		case "RecoveryAttempts":
		case "WriterType":
			c.WriterType = value
		case "BufferType":
			c.BufferType = value
		case "FlushInterval":
			fmt.Sscanf(value, "%d", &c.FlushInterval)
		case "BufferSize":
			fmt.Sscanf(value, "%d", &c.BufferSize)
		case "WriterFilePath":
			c.WriterFilePath = value
		case "WriterCompression_type":
			c.WriterCompressionType = value
		case "WriterRowGroupSize":
			fmt.Sscanf(value, "%d", &c.WriterRowGroupSize)
		case "Address":
			c.Address = value
		case "Port":
			c.Port = 0
			fmt.Sscanf(value, "%d", &c.Port)
		case "RedisHost":
			c.RedisHost = value
		case "RedisPassword":
			c.RedisPassword = value
		case "RedisDB":
			fmt.Sscanf(value, "%d", &c.RedisDB)
		case "RedisRecoveryKey":
			c.RedisRecoveryKey = value
		case "RedisDataPrefix":
			c.RedisDataPrefix = value
		case "RedisKeys":
			c.RedisKeys = value
		case "S3BucketName":
			c.S3BuketName = value
		case "S3Region":
			c.S3Region = value
		case "S3RoleName":
			c.S3RoleName = value
		case "S3STSEndpoint":
			c.S3STSEndpoint = value
		case "S3Endpoint":
			c.S3Endpoint = value
		case "S3Account":
			c.S3Account = value
		case "JsonSchemaPath":
			c.JsonSchemaPath = value
		case "RecordType":
			c.RecordType = value
		case "RedisDLQPrefix":
			c.RedisDLQPrefix = value
		case "RedisLockPrefix":
			c.RedisLockPrefix = value

		default:
			slog.Warn("Unknown key", "key", key, "value", value, "module", "config", "function", "Set")
		}
	}

	c.SetDefaults()

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
	ret["RedisLockPrefix"] = c.RedisLockPrefix
	ret["RedisPassword"] = c.RedisPassword
	ret["RedisRecoveryKey"] = c.RedisRecoveryKey
	ret["RedisSQLPrefix"] = c.RedisDLQPrefix
	ret["S3BucketName"] = c.S3BuketName
	ret["S3Region"] = c.S3Region
	ret["S3RoleName"] = c.S3RoleName
	ret["S3STSEndpoint"] = c.S3STSEndpoint
	ret["S3Endpoint"] = c.S3Endpoint
	ret["S3Account"] = c.S3Account
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

	if c.BufferSize < 10 {
		c.BufferSize = 10
	}

	if c.FlushInterval < 10 {
		c.FlushInterval = 10
	}

	if len(c.RedisKeys) == 0 {
		c.RedisKeys = "keys"
	}

	if len(c.RedisLockPrefix) == 0 {
		c.RedisLockPrefix = "lock"
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

	if c.RecoveryAttempts < 0 {
		c.RecoveryAttempts = 0
	}

	if len(c.RecordType) == 0 {
		c.RecordType = domain.RecordTypeLog
	}

	c.RecordType = strings.ToLower(c.RecordType)
}
