# Data2Parquet-go
A go data converter to Apache Parquet

## Concepts
We need to receive a large amount of json data and create structured parquet files. Considering that we will receive data line by line and it will be necessary to create a buffer to obtain better performance from the files. To do this, we will use a kind of buffer to temporarily store this data and we will create an asynchronous process, triggered by this buffer size or with a time interval.

## Flows

### Receiving data (Redis example)
```mermaid
sequenceDiagram
	autonumber
    participant Client
    participant Receiver    

    Client->>Receiver: Send Record
	create participant Buffer
    Receiver->>Buffer: Store data on Buffer
	
	create participant Redis
	Buffer->>Redis: Store data on external buffer
    
	Receiver->>Buffer: Check Buffer Size
	alt BufferSize reach
		Receiver->>Receiver: Start Flush
	else Trigger by Flush Interval
		Receiver->>Receiver: Start Flush
    end
```

### Flush process (Redis example)
```mermaid
sequenceDiagram
	autonumber
	participant Receiver
    participant Buffer   		
	participant Redis
		
	critical
		note over Receiver, Redis: Lock verify
		Receiver->>Buffer: Check Lock		
		alt No lock
			Buffer->>Redis: Create a lock
			Buffer->>Receiver: Continue with flush
		else Lock is mine
			Redis->>Buffer: Get data to flush		
			Buffer->>Receiver: Continue with flush
		end
		break Lock belongs to another instance
			Buffer->>Receiver: Skip flush this time and end process
		end
	end
		
	critical
		note over Receiver, Redis: Request buffer data
		Receiver->>Buffer: Request data to Flush
		Buffer->>Redis: Request current flush data
		Buffer->>Receiver: Return data to flush		
	end

	critical	
		create participant Converter
		note over Receiver, Converter: Try to convert data into a parquet stream						
		Receiver->>Converter: Try convert all data to Parquet stream	
		alt Fail to convert
			Converter->>Receiver: Return invalid data
			Receiver->>Buffer: Put data on DLQ
			Buffer->>Redis: Put data on DLQ Bucket
		else
			Converter->>Receiver: Receive processed data
		end
	end 

	opt 
		note over Receiver, Redis: Resend recovered data if needed
		Receiver->>Buffer: Ask for recovery data if exists
		Buffer->>Redis: Check if exists recovery data
		Redis->>Buffer: Return recovery data
		Buffer->>Receiver: Get data to write
	end

	critical
		create participant Writer	
		note over Receiver, Writer: Write converted data on final target	
		Receiver->>Writer: Send data to store

		note over Receiver, Redis: Write converted data on final target	
		
		alt Fail to store processed data
			Receiver->>Buffer: Send data to recovery bucket
			Buffer->>Redis: Send data to recovery bucket
		end

		Receiver->>Buffer: Clear data already stored from current bucket
		Buffer->>Redis: Clear data already stored from current bucket
	end
```

## Components

``` mermaid
---
title: Data 2 Parquet
---
classDiagram
	Fluent-Out-Parquet <-- Receiver: use
	Fluent-Out-Parquet <-- Config: use
	Fluent-Out-Parquet <-- Record: use
	
	Receiver <-- Record: use
	Receiver <-- Config: use
	Receiver <-- Writer: use
	Receiver <-- Buffer: use

	Record *-- RecordInfo

	Log <|-- Record: implement
	Log o-- RecordInfo: compose
	Dynamic <|-- Record: implement
	Dynamic o-- RecordInfo: compose

	Mem <-- Config: use
	Mem <|-- Buffer: implement

	Redis <-- Config: use
	Redis <|-- Buffer: implement

	File <-- Config: use
	File <-- RecordInfo: use
	File <|-- Writer: implement
	
	AWS-S3 <-- Config: use
	AWS-S3 <-- RecordInfo: use
	AWS-S3 <|-- Writer: implement

	namespace Records{
		class RecordInfo{
			+ string key
			+ string Key()
			+ string Target()
			+ string Capability()
			+ string Domain()
			+ sring Service()
			+ string Application()
		}

		class Record{
			<<interface>>
			GetInfo() : RecordInfo
			Decode(data map[string]any)
			ToJson(): string
			FromJson(data string)
			ToString(): string
			ToMsgPack() []byte
			FromMsgPack(data []byte)		
		}

		class Dynamic["Record::Dynamic"]{
			- Data map[string]any

			+ GetInfo(): RecordInfo
			+ Decode(data map[string]any)
			+ ToJson(): string
			+ FromJson(data string)
			+ ToString(): string
			+ ToMsgPack(): []byte
			+ FromMsgPack(data []byte)		
		}	

		class Log["Record::Log"]{
			+ Time                        string           
			+ Level                       string           
			+ CorrelationId               *string          
			+ SessionId                   *string          
			+ MessageId                   *string          
			+ PersonId                    *string          
			+ UserId                      *string          
			+ DeviceId                    *string          
			+ Message                     string           
			+ BusinessCapability          string           
			+ BusinessDomain              string           
			+ BusinessService             string           
			+ ApplicationService          string           
			+ Audit                       *bool            
			+ ResourceType                *string          
			+ CloudProvider               *string          
			+ SourceId                    *string          
			+ HTTPResponse                *string          
			+ ErrorCode                   *string          
			+ Error                       *string          
			+ StackTrace                  *string          
			+ Duration                    *string          
			+ TraceIP                     []string         
			+ Region                      *string          
			+ AZ                          *string          
			+ Tags                        []string         
			+ Args                        map[string]string
			+ TransactionMessageReference *string          
			+ Ttl                         *string          
			+ AutoIndex                   *bool            
			+ LoggerName                  *string          
			+ ThreadName                  *string          
			+ ExtraFields                 map[string]string

			+ GetInfo(): RecordInfo
			+ Decode(data map[string]any)
			+ ToJson(): string
			+ FromJson(data string)
			+ ToString(): string
			+ ToMsgPack(): []byte
			+ FromMsgPack(data []byte)
		}
	}

	namespace Fluent{
		class Fluent-Out-Parquet["Fluent Out Parquet Plugin"]{
			<<service>>
			- config Config
			+ Register()
			+ Init()
			+ Flush()
			+ Close()
		}
	}

	namespace Manager{
		class Receiver{
			<<service>>
			- config config.Config
			- writer Writer
			- buffer Buffer
			+ New(config Config)
			+ Write(record Record): error
			+ Flush(): error
			+ FlushKey(key string): error
			+ TryResendData(): error
			+ Close(): error
			+ HealthCheck(): error
		}

		class Config{
			+ BufferSize            int   
			+ BufferType            string
			+ Debug                 bool  
			+ FlushInterval         int   
			+ JsonSchemaPath        string
			+ RecordType            string
			+ RecoveryAttempts      int   
			+ RedisDataPrefix       string
			+ RedisDB               int   
			+ RedisDLQPrefix        string
			+ RedisHost             string
			+ RedisKeys             string
			+ RedisLockPrefix       string
			+ RedisLockTTL          int   
			+ RedisLockInstanceName string
			+ RedisPassword         string
			+ RedisRecoveryKey      string
			+ RedisTimeout          int   
			+ S3BuketName           string
			+ S3Endpoint            string
			+ S3Region              string
			+ S3RoleARN             string
			+ S3STSEndpoint         string
			+ S3DefaultCapability   string
			+ TryAutoRecover        bool  
			+ WriterCompressionType string
			+ WriterFilePath        string
			+ WriterRowGroupSize    int64 
			+ WriterType            string
			+ FromJson(json string)
			+ Set(data map[string]any)
		}
	}

	namespace Buffers{
		class Buffer{
			<<interface>>
			Close() error
			Push(key string, item domain.Record) (int, error)
			PushDLQ(key string, item domain.Record) error
			GetDLQ() (map[string][]domain.Record, error)
			ClearDLQ() error
			Get(key string) []domain.Record
			Clear(key string, size int) error
			Len(key string) int
			Keys() []string
			IsReady() bool
			HasRecovery() bool
			PushRecovery(key string, buf *bytes.Buffer) error
			GetRecovery() ([]*RecoveryData, error)
			ClearRecoveryData() error
			CheckLock(key string) bool		
		}

		class Mem["Buffer::Mem"]{
			- config Config
			+ New(config Config)
			+ Close(): error
			+ Push(key string, item domain.Record): (int, error)
			+ PushDLQ(key string, item domain.Record): error
			+ GetDLQ() (map[string][]domain.Record, error)
			+ ClearDLQ(): error
			+ Get(key string): []domain.Record
			+ Clear(key string, size int): error
			+ Len(key string): int
			+ Keys(): []string
			+ IsReady(): bool
			+ HasRecovery() bool
			+ PushRecovery(key string, buf *bytes.Buffer): error
			+ GetRecovery(): ([]*RecoveryData, error)
			+ ClearRecoveryData(): error
			+ CheckLock(key string): bool
		}

		class Redis["Buffer::Redis"]{
			- config Config
			+ New(config Config)
			+ Close(): error
			+ Push(key string, item domain.Record): (int, error)
			+ PushDLQ(key string, item domain.Record): error
			+ GetDLQ() (map[string][]domain.Record, error)
			+ ClearDLQ(): error
			+ Get(key string): []domain.Record
			+ Clear(key string, size int): error
			+ Len(key string): int
			+ Keys(): []string
			+ IsReady(): bool
			+ HasRecovery() bool
			+ PushRecovery(key string, buf *bytes.Buffer): error
			+ GetRecovery(): ([]*RecoveryData, error)
			+ ClearRecoveryData(): error
			+ CheckLock(key string): bool
		}
	}

	namespace Writers{
		class Writer{
			<<interface>>
			Init() error
			Write(key string, buf *bytes.Buffer) error
			Close() error
			IsReady() bool
		}

		class File["Writer::File"]{
			- config Config
			+ New(config Config)
			+ Init() error
			+ Write(key string, buf *bytes.Buffer) error
			+ Close() error
			+ IsReady() bool
		}

		class AWS-S3["Writer::AWS-S3"]{
			- config Config
			+ New(config Config)
			+ Init() error
			+ Write(key string, buf *bytes.Buffer) error
			+ Close() error
			+ IsReady() bool
		}
	}
```

## Applications (/cmd)
### [Data Generator](https://github.com/RafaelFino/Data2Parquet-go/blob/main/cmd/data-generator/main.go)
Simple data creator to simulate workloads to json2parquet.
### [Json2Parquet](https://github.com/RafaelFino/Data2Parquet-go/blob/main/cmd/json2parquet/main.go)
Worker that can receive a file with json data (records - log), process and create parquet files splited with keys.
### [Http Server](https://github.com/RafaelFino/Data2Parquet-go/blob/main/cmd/http-server/main.go)
A HTTP-Server that offer a HTTP Rest API to send data and manage Flush process.
### [FluentBit Parquet Output Plugin](https://github.com/RafaelFino/Data2Parquet-go/blob/main/cmd/fluent-out-parquet/main.go)
A shared object built to works with FluentBit as an Output plugin.

### The [Record Type](https://github.com/RafaelFino/Data2Parquet-go/blob/main/pkg/domain/record.go) (/pkg/domain)
``` golang
type Log struct {
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
	HTTPResponse                *string           `json:"http_response,omitempty" parquet:"name=http_response, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY" msg:"http_response"`
	ErrorCode                   *string           `json:"error_code,omitempty" parquet:"name=error_code, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY" msg:"error_code"`
	StackTrace                  *string           `json:"stack_trace,omitempty" parquet:"name=stack_trace, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY" msg:"stack_trace"`
	Duration                    *string           `json:"duration,omitempty" parquet:"name=duration, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY" msg:"duration"`
	TraceIP                     []string          `json:"trace_ip,omitempty" parquet:"name=trace_ip, type=MAP, convertedtype=LIST, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8" msg:"trace_ip"`
	Region                      *string           `json:"region,omitempty" parquet:"name=region, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY" msg:"region"`
	AZ                          *string           `json:"az,omitempty" parquet:"name=az, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY" msg:"az"`
	Tags                        []string          `json:"tags,omitempty" parquet:"name=tags, type=MAP, convertedtype=LIST, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8" msg:"tags"`
	Args                        map[string]string `json:"args,omitempty" parquet:"name=args, type=MAP, convertedtype=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8, valuetype=BYTE_ARRAY" msg:"args"`
	TransactionMessageReference *string           `json:"transaction_message_reference,omitempty" parquet:"name=transaction_message_reference, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY" msg:"transaction_message_reference"`
	Ttl                         *string           `json:"ttl,omitempty" parquet:"name=ttl, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY" msg:"ttl"`
	AutoIndex                   *bool             `json:"auto_index,omitempty" parquet:"name=auto_index, type=BOOLEAN" msg:"auto_index"`
	LoggerName                  *string           `json:"logger_name,omitempty" parquet:"name=logger_name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY" msg:"logger_name"`
	ThreadName                  *string           `json:"thread_name,omitempty" parquet:"name=thread_name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY" msg:"thread_name"`
	ExtraFields                 map[string]string `json:"extra_fields,omitempty" parquet:"name=extra_fields, type=MAP, convertedtype=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8, valuetype=BYTE_ARRAY" msg:"extra_fields"`
}
```

## [Buffers](https://github.com/RafaelFino/Data2Parquet-go/blob/main/pkg/buffer/buffer.go) (/pkg/buffer)
Using the key `BufferType` you can choose the storage to make data buffer, before writer work. You can configure `BufferSize` and `FlushInterval` to manage data.
### [Mem](https://github.com/RafaelFino/Data2Parquet-go/blob/main/pkg/buffer/mem.go) (`BufferType` = `mem`)
Use a local memory structure to stora temporaly data before Writer receive data. This option should be more faster, but doesn't offer resilience in disaster case.
### [Redis](https://github.com/RafaelFino/Data2Parquet-go/blob/main/pkg/buffer/redis.go) (`BufferType` = `redis`)
Use a redis instance to store temporaly data before Writer receive data. This offer a more secure way to store buffer data, but requires an external resource (Redis).

Some parameters can be changed to handle Redis keys, such as `RedisKeys` and `RedisDataPrefix`, they will change how Writer make store keys.

The Works also can be configure just to receive data and never flush it, it is specialy important if you want to have more than one worker receiving data in a cluster, scanling worloads. It's very recommended that only one instance made Flush for each kind of key. To do that, use `RedisSkipFlush` key as `true`

## [Receiver](https://github.com/RafaelFino/Data2Parquet-go/blob/main/pkg/receiver/receiver.go) (/pkg/receiver)
This is the core for this service, responsable for receive data, buffering, enconde, decode and handle pages to Writers

## [Writers](https://github.com/RafaelFino/Data2Parquet-go/blob/main/pkg/writer/writer.go) (/pkg/writer)
Using the key `WriterType` you can choose the writer to write parquet data.
### [File](https://github.com/RafaelFino/Data2Parquet-go/blob/main/pkg/writer/file.go) (`WriterType` = `file`)
Write data in a local file, use the tag `WriterFilePath` to choose path to store data
### [AWS-S3](https://github.com/RafaelFino/Data2Parquet-go/blob/main/pkg/writer/aws-s3.go) (`WriterType` = `aws-s3`)

## [Config](https://github.com/RafaelFino/Data2Parquet-go/blob/main/pkg/config/config.go) (/pkg/config)
- **Address**: HTTP server Address configuration tag, describe the address of the server, its an optional field only used for HTTP server. The default value is empty.
- **BufferSize**: BufferSize configuration tag, describe the size of the buffer, its an important field for control buffer and page size to flush data. The default value is `100`.
- **BufferType**: BufferType configuration tag, describe the type of the buffer, this fields accepte two values, `mem` or `redis`. The default value is `mem`.
- **Debug**: Debug configuration tag, describe the debug mode, its an optional field. The debug mode will generate a lot of information. The default value is `false`.
- **FlushInterval**: FlushInterval configuration tag, describe the interval to flush data in seconds, its an important field to control the time to flush data. The default value is `5`.
- **JsonSchemaPath**: JsonSchemaPath configuration tag, describe the path to the JSON schema file, its an optional field. The default value is empty. *This feature is not implemented yet.
- Port: Port configuration tag, describe the port of the server, its an optional field only used for HTTP server. The default value is `8080`.
- **RecordType**: RecordType configuration tag, describe the type of the record, this fields accepte two values, `log` or `dynamic`. The default value is log. *Dynamic type is not implemented yet.
- **RecoveryAttempts**: RecoveryAttempts configuration tag, describe the number of attempts to recover data, its an optional field. The default value is `0`.
- **RedisDataPrefix**: RedisDataPrefix configuration tag, describe the prefix of the data key in Redis, its an optional field. The default value is `data`.
- **RedisDB**: RedisDB configuration tag, describe the database number in Redis, its an optional field. The default value is `0`.
- **RedisHost**: RedisHost configuration tag, describe the host of the Redis server, its an optional field if you use `BufferType` as `mem`, but became required if `BufferType` is `redis`. The default value is empty but need to be set if `BufferType` is `redis`.
- **RedisKeys**: RedisKeys configuration tag, describe the keys of the Redis server, its an optional field. The default value is `keys`.
- **RedisLockPrefix**: RedisLockPrefix configuration tag, describe the prefix of the lock key in Redis, its an optional field. The default value is `lock`.
- **RedisPassword**: RedisPassword configuration tag, describe the password of the Redis server, its an optional field. The default value is empty.
- **RedisRecoveryKey**: RedisRecoveryKey configuration tag, describe the recovery key in Redis, its an optional field. The default value is `recovery`.
- **RedisDLQPrefix**: RedisDLQPrefix configuration tag, describe the prefix of the DLQ key in Redis, its an optional field. The default value is `dlq`.
- **RedisLockTTL**: RedisLockTTL configuration tag, describe the TTL of the lock key in Redis, its an optional field. The default value is `1.5x` 'FlushInterval` value.
- **RedisLockInstanceName**: RedisLockInstanceName configuration tag, describe the instance name of the lock key in Redis, its an optional field. The default value is empty and in this case, instance hostname will be considered.
- **RedisTimeout**: RedisTimeout configuration tag, describe the timeout of the Redis server, its an optional field. The default value is empty, in this case, `0` will be the value (Redis defaults).
- **S3BucketName**: S3BucketName configuration tag, describe the bucket name in S3, its an optional field. The default value is empty but need to be set if you use `aws-s3` as a writer.
- **S3Region**: S3Region configuration tag, describe the region of the S3 server, its an optional field. The default value is empty but need to be set if you use `aws-s3` as a writer.
- **S3RoleARN**: S3RoleARN configuration tag, describe the role name of the S3 server, its an optional field. The default value is empty but need to be set if you use `aws-s3` as a writer.
- **S3STSEndpoint**: S3STSEndpoint configuration tag, describe the endpoint of the STS server, its an optional field. The default value is empty but need to be set if you use `aws-s3` as a writer.
- **S3Endpoint**: S3Endpoint configuration tag, describe the endpoint of the S3 server, its an optional field. The default value is empty but need to be set if you use `aws-s3` as a writer.
- **TryAutoRecover**: TryAutoRecover configuration tag, describe the auto recover mode, its an optional field. The default value is `false`. If set to `true` the system will try to recover the data that failed to write after flash, using recovery cache.
- **WriterCompressionType**: WriterCompressionType configuration tag, describe the compression type of the writer, its an optional field. The default and recommended value is `snappy`. This fields accepte two values, `snappy`, `gzip` or `none`.
- **WriterFilePath**: WriterFilePath configuration tag, describe the file path of the writer, its an optional field. The default value is `./out`.
- **WriterRowGroupSize**: WriterRowGroupSize configuration tag, describe the row group size of the writer, its an optional field. The default value is `134217728` (128M).
- **WriterType**: WriterType configuration tag, describe the type of the writer, this fields accepte two values, `file` or `aws-s3`. The default value is `file`.

``` golang
type Config struct {
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
	TryAutoRecover        bool   `json:"try_auto_recover,omitempty"`
	WriterCompressionType string `json:"writer_compression_type,omitempty"`
	WriterFilePath        string `json:"writer_file_path,omitempty"`
	WriterRowGroupSize    int64  `json:"writer_row_group_size,omitempty"`
	WriterType            string `json:"writer_type"`
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

##### Fluent output config example
``` ini
[OUTPUT]
  Name  out_parquet
  Match *
  BufferSize 1000
  FlushInterval 12
  BufferType redis
  Debug true
  WriterFilePath /home/fino/git/Data2Parquet-go/data
  WriterType aws-s3
  S3BucketName data2parquet
  S3Region us-east-2
  S3RoleARN arn:aws:iam::localstack:role/localhost:4566
  S3STSEndpoint http://localhost:4566
  S3Endpoint http://localhost:4566
```

##### Keys to Fluent-Bit Output
``` golang
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
	"TryAutoRecover",
	"WriterCompressionType",
	"WriterFilePath",
	"WriterRowGroupSize",
	"WriterType",
}
```
