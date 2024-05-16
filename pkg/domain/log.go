package domain

import (
	"encoding/json"
	"fmt"
	"strings"

	msgp "github.com/vmihailenco/msgpack/v5"
	"golang.org/x/exp/slog"
)

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

// / CloudProviderAWS is the AWS cloud provider
var CloudProviderAWS = "aws"
var CloudProviderGCP = "gcp"
var CloudProviderOCI = "oci"
var CloudProviderAzure = "azure"

// / CloudProviderType is the type of cloud provider
var CloudProviderType = map[string]int{
	CloudProviderAWS:   0,
	CloudProviderGCP:   1,
	CloudProviderOCI:   2,
	CloudProviderAzure: 3,
}

// / ResourceType is the type of resource
var ResK8s = "k8s"
var ResVM = "vm"
var ResServerless = "serverless"
var ResSaas = "saas"
var ResCloudService = "cloudservice"
var ResVendor = "vendor"

var ResourceType = map[string]int{
	ResK8s:          0,
	ResVM:           1,
	ResServerless:   2,
	ResSaas:         3,
	ResCloudService: 4,
	ResVendor:       5,
}

// / LevelEmergency is the emergency level
var LevelEmergency = "emergency"
var LevelAlert = "alert"
var LevelCritical = "critical"
var LevelError = "error"
var LevelWarning = "warning"
var LevelInfo = "info"
var LevelDebug = "debug"

var LogLevel = map[string]int{
	LevelEmergency: 0,
	LevelAlert:     1,
	LevelCritical:  2,
	LevelError:     3,
	LevelWarning:   4,
	LevelInfo:      5,
	LevelDebug:     6,
}

func NewLog(data map[string]interface{}) Record {
	ret := &Log{
		ExtraFields: make(map[string]string),
		TraceIP:     make([]string, 0),
		Tags:        make([]string, 0),
		Args:        make(map[string]string),
		Level:       LevelInfo,
		Audit:       new(bool),
		AutoIndex:   new(bool),
	}

	ret.Decode(data)

	return ret
}

func (l *Log) ToString() string {
	return fmt.Sprintf("%+v", l)
}

func GetInt64(n any) int64 {
	if n == nil {
		return 0
	}

	switch v := n.(type) {
	case int64:
		return v
	case int:
		return int64(v)
	case float64:
		return int64(v)
	case uint64:
		return int64(v)
	case uint:
		return int64(v)
	case int32:
		return int64(v)
	case uint32:
		return int64(v)
	case int16:
		return int64(v)
	case uint16:
		return int64(v)
	case int8:
		return int64(v)
	case uint8:
		return int64(v)
	case float32:
		return int64(v)
	default:
		return 0
	}
}

func (l *Log) Decode(data map[string]interface{}) {
	for k, v := range data {
		key := strings.ToLower(fmt.Sprintf("%v", k))

		switch key {
		case "time":
			l.Time = v.(string)
		case "level":
			l.Level = v.(string)
		case "correlation_id":
			l.CorrelationId = GetStringP(v)
		case "session_id":
			l.SessionId = GetStringP(v)
		case "message_id":
			l.MessageId = GetStringP(v)
		case "person_id":
			l.PersonId = GetStringP(v)
		case "user_id":
			l.UserId = GetStringP(v)
		case "device_id":
			l.DeviceId = GetStringP(v)
		case "message":
			l.Message = v.(string)
		case "business_capability":
			l.BusinessCapability = v.(string)
		case "business_domain":
			l.BusinessDomain = v.(string)
		case "business_service":
			l.BusinessService = v.(string)
		case "application_service":
			l.ApplicationService = v.(string)
		case "audit":
			val := v.(bool)
			l.Audit = &val
		case "resource_type":
			l.ResourceType = GetStringP(v)
		case "cloud_provider":
			l.CloudProvider = GetStringP(v)
		case "source_id":
			l.SourceId = GetStringP(v)
		case "http_response":
			val := GetInt64(v)
			l.HTTPResponse = &val
		case "error_code":
			l.ErrorCode = GetStringP(v)
		case "stack_trace":
			l.StackTrace = GetStringP(v)
		case "duration":
			val := GetInt64(v)
			l.Duration = &val
		case "trace_ip":
			switch valueType := v.(type) {
			case []string:
				l.TraceIP = append(l.TraceIP, valueType...)
			case []interface{}:
				for _, ip := range valueType {
					l.TraceIP = append(l.TraceIP, ip.(string))
				}
			}
		case "region":
			l.Region = GetStringP(v)
		case "az":
			l.AZ = GetStringP(v)
		case "tags":
			switch valueType := v.(type) {
			case []string:
				l.Tags = append(l.Tags, valueType...)
			case []interface{}:
				for _, tag := range valueType {
					l.Tags = append(l.Tags, tag.(string))
				}
			}
		case "args":
			switch valueType := v.(type) {
			case map[string]string:
				for arg_key, arg_val := range valueType {
					l.Args[arg_key] = arg_val
				}
			case map[interface{}]interface{}:
				for arg_key, arg_val := range valueType {
					l.Args[arg_key.(string)] = arg_val.(string)
				}
			case map[string]interface{}:
				for arg_key, arg_val := range valueType {
					l.Args[arg_key] = arg_val.(string)
				}
			}
		case "transaction_message_reference":
			l.TransactionMessageReference = GetStringP(v)
		case "ttl":
			val := GetInt64(v)
			l.Ttl = &val
		case "auto_index":
			val := v.(bool)
			l.AutoIndex = &val
		case "logger_name":
			l.LoggerName = GetStringP(v)
		case "thread_name":
			l.ThreadName = GetStringP(v)
		default:
			l.ExtraFields[k] = fmt.Sprintf("%s", v)
		}
	}
}

func (l *Log) Key() string {
	return fmt.Sprintf("%s-%s-%s", l.BusinessCapability, l.BusinessDomain, l.BusinessService)
}

func (l *Log) ToJson() string {
	data, err := json.Marshal(l)

	if err != nil {
		slog.Error("Error marshalling JSON", "error", err)
		return ""
	}

	return string(data)
}

func (l *Log) FromJson(data string) error {
	err := json.Unmarshal([]byte(data), l)

	if err != nil {
		slog.Error("Error unmarshalling JSON", "error", err)
		return err
	}

	return nil
}

func (l *Log) ToMsgPack() []byte {
	data, err := msgp.Marshal(l)

	if err != nil {
		slog.Error("Error marshalling MsgPack", "error", err)
		return nil
	}

	return data
}

func (l *Log) FromMsgPack(data []byte) error {
	err := msgp.Unmarshal(data, l)

	if err != nil {
		slog.Error("Error unmarshalling MsgPack", "error", err)
		return err
	}

	return nil
}
