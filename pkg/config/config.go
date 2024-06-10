package config

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strings"
)

type Config struct {
	//Address: HTTP server Address configuration tag, describe the address of the server, its an optional field only used for HTTP server. The default value is empty.
	//BufferSize: BufferSize configuration tag, describe the size of the buffer, its an important field for control buffer and page size to flush data. The default value is `100`.
	//BufferType: BufferType configuration tag, describe the type of the buffer, this fields accepte two values, `mem` or `redis`. The default value is `mem`.
	//Debug: Debug configuration tag, describe the debug mode, its an optional field. The debug mode will generate a lot of information. The default value is `false`.
	//FlushInterval: FlushInterval configuration tag, describe the interval to flush data in seconds, its an important field to control the time to flush data. The default value is `5`.
	//JsonSchemaPath: JsonSchemaPath configuration tag, describe the path to the JSON schema file, its an optional field. The default value is empty. *This feature is not implemented yet.
	//Port: Port configuration tag, describe the port of the server, its an optional field only used for HTTP server. The default value is `8080``.
	//RecordType: RecordType configuration tag, describe the type of the record, this fields accepte two values, `log` or `dynamic``. The default value is log. *Dynamic type is not implemented yet.
	//RecoveryAttempts: RecoveryAttempts configuration tag, describe the number of attempts to recover data, its an optional field. The default value is `0``.
	//RedisDataPrefix: RedisDataPrefix configuration tag, describe the prefix of the data key in Redis, its an optional field. The default value is `data`.
	//RedisDB: RedisDB configuration tag, describe the database number in Redis, its an optional field. The default value is `0`.
	//RedisHost: RedisHost configuration tag, describe the host of the Redis server, its an optional field if you use 'BufferType` as `mem`, but became required if `BufferType` is `redis`. The default value is empty but need to be set if `BufferType` is `redis`.
	//RedisKeys: RedisKeys configuration tag, describe the keys of the Redis server, its an optional field. The default value is `keys`.
	//RedisLockPrefix: RedisLockPrefix configuration tag, describe the prefix of the lock key in Redis, its an optional field. The default value is `lock`.
	//RedisPassword: RedisPassword configuration tag, describe the password of the Redis server, its an optional field. The default value is empty.
	//RedisRecoveryKey: RedisRecoveryKey configuration tag, describe the recovery key in Redis, its an optional field. The default value is `recovery`.
	//RedisDLQPrefix: RedisDLQPrefix configuration tag, describe the prefix of the DLQ key in Redis, its an optional field. The default value is `dlq`.
	//RedisLockTTL: RedisLockTTL configuration tag, describe the TTL of the lock key in Redis, its an optional field. The default value is `1.5x` 'FlushInterval` value.
	//RedisLockInstanceName: RedisLockInstanceName configuration tag, describe the instance name of the lock key in Redis, its an optional field. The default value is empty and in this case, instance hostname will be considered.
	//RedisTimeout: RedisTimeout configuration tag, describe the timeout of the Redis server, its an optional field. The default value is empty, in this case, `0` will be the value (Redis defaults).
	//S3BucketName: S3BucketName configuration tag, describe the bucket name in S3, its an optional field. The default value is empty but need to be set if you use `aws-s3` as a writer.
	//S3Region: S3Region configuration tag, describe the region of the S3 server, its an optional field. The default value is empty but need to be set if you use `aws-s3` as a writer.
	//S3RoleARN: S3RoleName configuration tag, describe the role name of the S3 server, its an optional field. The default value is empty but need to be set if you use `aws-s3` as a writer.
	//S3STSEndpoint: S3STSEndpoint configuration tag, describe the endpoint of the STS server, its an optional field. The default value is empty but need to be set if you use `aws-s3` as a writer.
	//S3Endpoint: S3Endpoint configuration tag, describe the endpoint of the S3 server, its an optional field. The default value is empty but need to be set if you use `aws-s3` as a writer.
	//TryAutoRecover: TryAutoRecover configuration tag, describe the auto recover mode, its an optional field. The default value is `false`. If set to `true` the system will try to recover the data that failed to write after flash, using recovery cache.
	//UseDLQ: UseDLQ configuration tag, describe the use of DLQ, its an optional field. The default value is `false`. If set to `true` the system will use the DLQ to store the data that failed to write after flash.
	//WriterCompressionType: WriterCompressionType configuration tag, describe the compression type of the writer, its an optional field. The default and recommended value is `snappy`. This fields accepte two values, `snappy`, `gzip` or `none`.
	//WriterFilePath: WriterFilePath configuration tag, describe the file path of the writer, its an optional field. The default value is `./out`.
	//WriterRowGroupSize: WriterRowGroupSize configuration tag, describe the row group size of the writer, its an optional field. The default value is `134217728` (128M).
	//WriterType: WriterType configuration tag, describe the type of the writer, this fields accepte two values, `file` or `aws-s3`. The default value is `file`.

	Address               string `json:"address,omitempty"`
	BufferSize            int    `json:"buffer_size"`
	BufferType            string `json:"buffer_type"`
	Debug                 bool   `json:"debug,omitempty"`
	FlushInterval         int    `json:"flush_interval"`
	JsonSchemaPath        string `json:"json_schema_path,omitempty"`
	Port                  int    `json:"port,omitempty"`
	RecordType            string `json:"record_type"`
	RecoveryAttempts      int    `json:"recovery_attempts,omitempty"`
	RedisDataPrefix       string `json:"redis_data_prefix,omitempty"`
	RedisDB               int    `json:"redis_db,omitempty"`
	RedisDLQPrefix        string `json:"redis_dlq_prefix,omitempty"`
	RedisHost             string `json:"redis_host,omitempty"`
	RedisKeys             string `json:"redis_keys,omitempty"`
	RedisLockPrefix       string `json:"redis_lock_prefix,omitempty"`
	RedisLockTTL          int    `json:"redis_lock_ttl,omitempty"`
	RedisLockInstanceName string `json:"redis_lock_instance_name,omitempty"`
	RedisPassword         string `json:"redis_password,omitempty"`
	RedisRecoveryKey      string `json:"redis_recovery_key,omitempty"`
	RedisTimeout          int    `json:"redis_timeout,omitempty"`
	S3BuketName           string `json:"s3_bucket_name"`
	S3Endpoint            string `json:"s3_endpoint,omitempty"`
	S3Region              string `json:"s3_region"`
	S3RoleARN             string `json:"s3_role_arn,omitempty"`
	S3STSEndpoint         string `json:"s3_sts_endpoint,omitempty"`
	S3DefaultCapability   string `json:"s3_default_capability,omitempty"`
	TryAutoRecover        bool   `json:"try_auto_recover,omitempty"`
	UseDLQ                bool   `json:"use_dlq,omitempty"`
	WriterCompressionType string `json:"writer_compression_type,omitempty"`
	WriterFilePath        string `json:"writer_file_path,omitempty"`
	WriterRowGroupSize    int64  `json:"writer_row_group_size,omitempty"`
	WriterType            string `json:"writer_type"`
}

const BufferTypeMem = "mem"
const BufferTypeRedis = "redis"

var BufferTypes = map[string]int{
	BufferTypeMem:   1,
	BufferTypeRedis: 2,
}

const WriterTypeAWSS3 = "aws-s3"
const WriterTypeFile = "file"

var WriterTypes = map[string]int{
	WriterTypeFile:  1,
	WriterTypeAWSS3: 2,
}

const RecordTypeLog = "log"
const RecordTypeLogLegacy = "log_legacy"
const RecordTypeDynamic = "dynamic"

var RecordTypes = map[string]int{
	RecordTypeLog:       1,
	RecordTypeDynamic:   2,
	RecordTypeLogLegacy: 3,
}

var keys = []string{
	"BufferSize",
	"BufferType",
	"Debug",
	"FlushInterval",
	"JsonSchemaPath",
	"RecordType",
	"RecoveryAttempts",
	"RedisDataPrefix",
	"RedisDB",
	"RedisHost",
	"RedisKeys",
	"RedisLockPrefix",
	"RedisLockTTL",
	"RedisLockInstanceName",
	"RedisPassword",
	"RedisRecoveryKey",
	"RedisSQLPrefix",
	"RedisTimeout",
	"S3BucketName",
	"S3Region",
	"S3RoleARN",
	"S3STSEndpoint",
	"S3Endpoint",
	"S3DefaultCapability",
	"UseDLQ",
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
		case "TryAutoRecover":
			c.TryAutoRecover = strings.ToLower(value) == "true"
		case "RecoveryAttempts":
		case "WriterType":
			c.WriterType = value
		case "BufferType":
			c.BufferType = value
		case "FlushInterval":
			_, err := fmt.Sscanf(value, "%d", &c.FlushInterval)
			if err != nil {
				slog.Warn("Error parsing FlushInterval", "error", err)
				c.FlushInterval = 5
			}
		case "BufferSize":
			_, err := fmt.Sscanf(value, "%d", &c.BufferSize)
			if err != nil {
				slog.Warn("Error parsing BufferSize", "error", err)
				c.BufferSize = 100
			}
		case "WriterFilePath":
			c.WriterFilePath = value
		case "WriterCompression_type":
			c.WriterCompressionType = value
		case "WriterRowGroupSize":
			_, err := fmt.Sscanf(value, "%d", &c.WriterRowGroupSize)
			if err != nil {
				slog.Warn("Error parsing WriterRowGroupSize", "error", err)
				c.WriterRowGroupSize = 128 * 1024 * 1024
			}
		case "Address":
			c.Address = value
		case "Port":
			_, err := fmt.Sscanf(value, "%d", &c.Port)
			if err != nil {
				slog.Warn("Error parsing Port", "error", err)
				c.Port = 8080
			}
		case "RedisHost":
			c.RedisHost = value
		case "RedisPassword":
			c.RedisPassword = value
		case "RedisDB":
			_, err := fmt.Sscanf(value, "%d", &c.RedisDB)
			if err != nil {
				slog.Warn("Error parsing RedisDB", "error", err)
				c.RedisDB = 0
			}
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
		case "S3RoleARN":
			c.S3RoleARN = value
		case "S3STSEndpoint":
			c.S3STSEndpoint = value
		case "S3Endpoint":
			c.S3Endpoint = value
		case "JsonSchemaPath":
			c.JsonSchemaPath = value
		case "RecordType":
			c.RecordType = value
		case "RedisDLQPrefix":
			c.RedisDLQPrefix = value
		case "RedisLockPrefix":
			c.RedisLockPrefix = value
		case "RedisLockTTL":
			_, err := fmt.Sscanf(value, "%d", &c.RedisLockTTL)
			if err != nil {
				slog.Warn("Error parsing RedisLockTTL", "error", err)
				c.RedisLockTTL = int(c.FlushInterval + c.FlushInterval/2)
			}
		case "RedisLockInstanceName":
			c.RedisLockInstanceName = value
		case "RedisTimeout":
			_, err := fmt.Sscanf(value, "%d", &c.RedisTimeout)
			if err != nil {
				slog.Warn("Error parsing RedisTimeout", "error", err)
				c.RedisTimeout = 0
			}
		case "S3DefaultCapability":
			c.S3DefaultCapability = value

		case "UseDLQ":
			c.UseDLQ = strings.ToLower(value) == "true"

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
	ret["Port"] = c.Port
	ret["RecordType"] = c.RecordType
	ret["RecoveryAttempts"] = c.RecoveryAttempts
	ret["RedisDataPrefix"] = c.RedisDataPrefix
	ret["RedisDB"] = c.RedisDB
	ret["RedisHost"] = c.RedisHost
	ret["RedisKeys"] = c.RedisKeys
	ret["RedisLockPrefix"] = c.RedisLockPrefix
	ret["RedisLockTTL"] = c.RedisLockTTL
	ret["RedisLockInstanceName"] = c.RedisLockInstanceName
	ret["RedisPassword"] = c.RedisPassword
	ret["RedisRecoveryKey"] = c.RedisRecoveryKey
	ret["RedisDLQPrefix"] = c.RedisDLQPrefix
	ret["RedisTimeout"] = c.RedisTimeout
	ret["S3BucketName"] = c.S3BuketName
	ret["S3Region"] = c.S3Region
	ret["S3RoleARN"] = c.S3RoleARN
	ret["S3STSEndpoint"] = c.S3STSEndpoint
	ret["S3Endpoint"] = c.S3Endpoint
	ret["S3DefaultCapability"] = c.S3DefaultCapability
	ret["TryAutoRecover"] = c.TryAutoRecover
	ret["UseDLQ"] = c.UseDLQ
	ret["WriterCompressionType"] = c.WriterCompressionType
	ret["WriterFilePath"] = c.WriterFilePath
	ret["WriterRowGroupSize"] = c.WriterRowGroupSize
	ret["WriterType"] = c.WriterType

	return ret
}

func (c *Config) SetDefaults() {
	if c.Port < 1 {
		c.Port = 8080
	}

	if c.WriterType == "" {
		slog.Debug("Writer type is empty, setting to file")
		c.WriterType = "file"
	}

	if c.WriterFilePath == "" {
		slog.Debug("Writer file path is empty, setting to ./out")
		c.WriterFilePath = "./out"
	}

	if c.WriterCompressionType == "" {
		slog.Debug("Writer compression type is empty, setting to snappy")
		c.WriterCompressionType = "snappy"
	}

	if c.WriterRowGroupSize < 1024 {
		slog.Debug("Writer row group size is less than 1024, setting to 128M")
		c.WriterRowGroupSize = 128 * 1024 * 1024 //128M
	}

	if c.BufferType == "" {
		slog.Debug("Buffer type is empty, setting to mem")
		c.BufferType = BufferTypeMem
	}

	if c.BufferSize < 100 {
		slog.Debug("Buffer size is less than 100, setting to 100")
		c.BufferSize = 100
	}

	if c.FlushInterval < 5 {
		slog.Debug("Flush interval is less than 5 seconds, setting to 5")
		c.FlushInterval = 5
	}

	if len(c.RedisKeys) == 0 {
		slog.Debug("Redis keys is empty, setting to keys")
		c.RedisKeys = "keys"
	}

	if len(c.RedisLockPrefix) == 0 {
		slog.Debug("Redis lock prefix is empty, setting to lock")
		c.RedisLockPrefix = "lock"
	}

	if len(c.RedisDLQPrefix) == 0 {
		slog.Debug("Redis DLQ prefix is empty, setting to dlq")
		c.RedisDLQPrefix = "dlq"
	}

	if len(c.RedisDataPrefix) == 0 {
		slog.Debug("Redis data prefix is empty, setting to data")
		c.RedisDataPrefix = "data"
	}

	if len(c.RedisRecoveryKey) == 0 {
		slog.Debug("Redis recovery key is empty, setting to recovery")
		c.RedisRecoveryKey = "recovery"
	}

	if c.RecoveryAttempts < 0 {
		slog.Debug("Recovery attempts is less than 0, setting to 0")
		c.RecoveryAttempts = 0
	}

	if len(c.RecordType) == 0 {
		slog.Debug("Record type is empty, setting to log")
		c.RecordType = RecordTypeLog
	}

	c.RecordType = strings.ToLower(c.RecordType)

	if c.BufferType == BufferTypeRedis {
		if c.RedisLockTTL < int(c.FlushInterval+c.FlushInterval/2) {
			slog.Debug("Redis lock TTL is less than 1.5 times the flush interval, setting to 1.5 times the flush interval")
		}

		if len(c.RedisLockInstanceName) == 0 {
			slog.Debug("Redis lock instance name is empty, setting with hostname")
			host, err := os.Hostname()

			if err != nil {
				slog.Debug("Error getting hostname", "error", err)
				host = "d2p"
			}
			c.RedisLockInstanceName = host
		}

		if c.RedisDB < 0 {
			slog.Debug("Redis DB is less than 0, setting to 0")
			c.RedisDB = 0
		}

		if len(c.RedisHost) == 0 {
			slog.Error("Redis host is empty, please set it")
		}
	}

	if len(c.S3DefaultCapability) == 0 {
		slog.Debug("S3 default capability is empty, setting to empty")
		c.S3DefaultCapability = "undefined"
	}

	if c.UseDLQ {
		slog.Info("DLQ is enabled")
		if len(c.RedisDLQPrefix) == 0 {
			slog.Warn("DLQ is enabled but DLQ prefix is empty, using default value")
		}
	}
}
