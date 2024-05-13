# Data2Parquet-go
A go data converter to Apache Parquet

## Bin
### Data Generator
Simple data creator to simulate workloads to json2parquet.
### Json2Parquet
Worker that can receive a file with json data (records - log), process and create parquet files splited with keys.
### Http Server
A HTTP-Server that offer a HTTP Rest API to send data and manage Flush process.
### FluentBit Parquet Output Plugin
A shared object built to works with FluentBit as an Output plugin.

## Receiver
This is the core for this service, responsable for receive data, buffering, enconde, decode and handle pages to Writers

### The Record Type
``` golang
type Record struct {
	Time                        string            `json:"time" parquet:"name=time, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY" msg:"time"`
	Level                       string            `json:"level" parquet:"name=level, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY" msg:"level"`
	CorrelationId               *string           `json:"correlation_id,omitempty" parquet:"name=correlation_id, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY" msg:"correlation_id"`
	SessionId                   *string           `json:"session_id,omitempty" parquet:"name=session_id, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY" msg:"session_id"`
	MessageId                   *string           `json:"message_id,omitempty" parquet:"name=message_id, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY" msg:"message_id"`
	PersonId                    *string           `json:"person_id,omitempty" parquet:"name=person_id, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY" msg:"person_id"`
	UserId                      *string           `json:"user_id,omitempty" parquet:"name=user_id, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY" msg:"user_id"`
	DeviceId                    *string           `json:"device_id,omitempty" parquet:"name=device_id, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY" msg:"device_id"`
	Message                     string            `json:"message" parquet:"name=message, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY" msg:"message"`
	BusinessCapability          string            `json:"business_capability" parquet:"name=business_capability, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY" msg:"business_capability"`
	BusinessDomain              string            `json:"business_domain" parquet:"name=business_domain, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY" msg:"business_domain"`
	BusinessService             string            `json:"business_service" parquet:"name=business_service, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY" msg:"business_service"`
	ApplicationService          string            `json:"application_service" parquet:"name=application_service, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY" msg:"application_service"`
	Audit                       *bool             `json:"audit,omitempty" parquet:"name=audit, type=BOOLEAN" msg:"audit"`
	ResourceType                *string           `json:"resource_type" parquet:"name=resource_type, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY" msg:"resource_type"`
	CloudProvider               *string           `json:"cloud_provider" parquet:"name=cloud_provider, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY" msg:"cloud_provider"`
	SourceId                    *string           `json:"source_id,omitempty" parquet:"name=source_id, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY" msg:"source_id"`
	HTTPResponse                *int64            `json:"http_response,omitempty" parquet:"name=http_response, type=INT32" msg:"http_response"`
	ErrorCode                   *string           `json:"error_code,omitempty" parquet:"name=error_code, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY" msg:"error_code"`
	StackTrace                  *string           `json:"stack_trace,omitempty" parquet:"name=stack_trace, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY" msg:"stack_trace"`
	Duration                    *int64            `json:"duration,omitempty" parquet:"name=duration, type=INT64, convertedtype=UINT_64" msg:"duration"`
	TraceIP                     []string          `json:"trace_ip,omitempty" parquet:"name=trace_ip, type=MAP, convertedtype=LIST, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8" msg:"trace_ip"`
	Region                      *string           `json:"region,omitempty" parquet:"name=region, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY" msg:"region"`
	AZ                          *string           `json:"az,omitempty" parquet:"name=az, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY" msg:"az"`
	Tags                        []string          `json:"tags,omitempty" parquet:"name=tags, type=MAP, convertedtype=LIST, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8" msg:"tags"`
	Args                        map[string]string `json:"args,omitempty" parquet:"name=args, type=MAP, convertedtype=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8, valuetype=BYTE_ARRAY" msg:"args"`
	TransactionMessageReference *string           `json:"transaction_message_reference,omitempty" parquet:"name=transaction_message_reference, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY" msg:"transaction_message_reference"`
	Ttl                         *int64            `json:"ttl,omitempty" parquet:"name=ttl, type=INT64" msg:"ttl"`
	AutoIndex                   *bool             `json:"auto_index,omitempty" parquet:"name=auto_index, type=BOOLEAN" msg:"auto_index"`
	LoggerName                  *string           `json:"logger_name,omitempty" parquet:"name=logger_name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY" msg:"logger_name"`
	ThreadName                  *string           `json:"thread_name,omitempty" parquet:"name=thread_name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY" msg:"thread_name"`
	ExtraFields                 map[string]string `json:"extra_fields,omitempty" parquet:"name=extra_fields, type=MAP, convertedtype=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8, valuetype=BYTE_ARRAY" msg:"extra_fields"`
}
```
## Buffers
Using the key `BufferType` you can choose the storage to make data buffer, before writer work. You can configure `BufferSize` and `FlushInterval` to manage data.
### Mem (`BufferType` = `mem`)
Use a local memory structure to stora temporaly data before Writer receive data. This option should be more faster, but doesn't offer resilience in disaster case.
### Redis (`BufferType` = `redis`)
Use a redis instance to store temporaly data before Writer receive data. This offer a more secure way to store buffer data, but requires an external resource (Redis).

Some parameters can be changed to handle Redis keys, such as `RedisKeys` and `RedisDataPrefix`, they will change how Writer make store keys.

The Works also can be configure just to receive data and never flush it, it is specialy important if you want to have more than one worker receiving data in a cluster, scanling worloads. It's very recommended that only one instance made Flush for each kind of key. To do that, use `RedisSkipFlush` key as `true`

## Writers
Using the key `WriterType` you can choose the writer to write parquet data.
### File (`WriterType` = `file`)
Write data in a local file, use the tag `WriterFilePath` to choose path to store data
### AWS-S3 (`WriterType` = `aws-s3`)

## Config
``` golang

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
}
```

#### Example to json2Parquet with Redis buffer and file writer:

``` json
{
    "buffer_size": 1000,
    "buffer_type": "redis",
    "debug": false,
    "flush_interval": 60,
    "log_path": "./logs",
    "redis_data_prefix": "data",
    "redis_db": 0,
    "redis_host": "0.0.0.0:6379",
    "redis_keys": "keys",
    "redis_password": "",
    "redis_skip_flush": false,
    "writer_compression_type": "snappy",
    "writer_file_path": "./data",
    "writer_row_group_size": 134217728,
    "writer_type": "file"
}
```

#### Example to json2Parquet with memory buffer and file writer:

``` json
{
    "buffer_type": "mem",
    "flush_interval": 60,
    "writer_compression_type": "snappy",
    "writer_file_path": "./data",
    "writer_row_group_size": 134217728,
    "writer_type": "file"
}
```

#### Fluent Out Parquet config keys
To FluentBit, use the main key name, example: `WriterType` instead `writer_type`