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
## Buffers
Using the key BufferType you can choose the storage to make data buffer, before writer work. You can configure BufferSize and FlushInterval to manage data.
### Mem (BufferType = "mem")
Use a local memory structure to stora temporaly data before Writer receive data. This option should be more faster, but doesn't offer resilience in disaster case.
### Redis (BufferType = "redis")
Use a redis instance to store temporaly data before Writer receive data. This offer a more secure way to store buffer data, but requires an external resource (Redis).

Some parameters can be changed to handle Redis keys, such as RedisKeys and RedisDataPrefix, they will change how Writer make store keys.

The Works also can be configure just to receive data and never flush, it is specialy important if you want to have more than one worker receiving data in a cluster, scanling worloads. It's very recommended that only one instance made Flush for each kind of key.

## Writers
Using the key WriterType you can choose the writer to write parquet data.
### File (WriterType = "file")
Write data in a local file, use the tag WriterFilePath to choose path to store data
### AWS-S3 (WriterType = "aws-s3")

## Config
    - Address is the address to listen on. Default is ""
	- BufferSize is the size of the buffer. Default is 1000
	- BufferType is the type of buffer to use. Can be "mem" or "redis"
	- Debug is the debug flag. Default is false
	- FlushInterval is the interval to flush the buffer. Default is 60. This value is in seconds
	- LogPath is the path to the log files. Default is "./logs"
	- Port is the port to listen on. Default is 0
	- RedisDataPrefix is the prefix to use for the redis keys. Default is "data"
	- RedisDB is the redis database to use. Default is 0
	- RedisHost is the redis host to connect to. Default is ""
	- RedisKeys is the keys to use for the redis buffer. Default is "keys"
	- RedisPassword is the redis password to use. Default is ""
	- RedisSkipFlush is the flag to skip flushing the redis buffer. Default is false
	- WriterCompressionType is the compression type to use for the writer. Default is "snappy". This field can be "snappy", "gzip", or "none"
	- WriterFilePath is the path to write the files to. Default is "./out"
	- WriterRowGroupSize is the size of the row group. Default is 128M
	- WriterType is the type of writer to use. Default is "file". This field can be "file" or "s3"
	