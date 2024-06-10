package domain

import (
	"encoding/json"
	"fmt"
	"strings"

	msgp "github.com/vmihailenco/msgpack/v5"
	"golang.org/x/exp/slog"
)

type Log struct {
	Info                        *LogInfo          `json:"info,omitempty" msg:"info"`
	Time                        string            `json:"time" parquet:"name=time, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY" msg:"time"`
	Level                       string            `json:"level" parquet:"name=level, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY" msg:"level"`
	Message                     string            `json:"message" parquet:"name=message, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY" msg:"message"`
	BusinessCapability          string            `json:"business-capability" parquet:"name=business-capability, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY" msg:"business-capability"`
	BusinessDomain              string            `json:"business-domain" parquet:"name=business-domain, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY" msg:"business-domain"`
	BusinessService             string            `json:"business-service" parquet:"name=business-service, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY" msg:"business-service"`
	ApplicationService          string            `json:"application-service" parquet:"name=application-service, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY" msg:"application-service"`
	ExtraFields                 map[string]string `json:"extra-fields,omitempty" parquet:"name=extra-fields, type=MAP, convertedtype=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8, valuetype=BYTE_ARRAY" msg:"extra-fields"`
	CorrelationId               *string           `json:"correlation-id,omitempty" parquet:"name=correlation-id, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY" msg:"correlation-id"`
	SessionId                   *string           `json:"session-id,omitempty" parquet:"name=session-id, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY" msg:"session-id"`
	MessageId                   *string           `json:"message-id,omitempty" parquet:"name=message-id, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY" msg:"message-id"`
	PersonId                    *string           `json:"person-id,omitempty" parquet:"name=person-id, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY" msg:"person-id"`
	UserId                      *string           `json:"user-id,omitempty" parquet:"name=user-id, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY" msg:"user-id"`
	DeviceId                    *string           `json:"device-id,omitempty" parquet:"name=device-id, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY" msg:"device-id"`
	Audit                       *bool             `json:"audit,omitempty" parquet:"name=audit, type=BOOLEAN" msg:"audit"`
	ResourceType                *string           `json:"resource-type" parquet:"name=resource-type, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY" msg:"resource-type"`
	CloudProvider               *string           `json:"cloud-provider" parquet:"name=cloud-provider, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY" msg:"cloud-provider"`
	SourceId                    *string           `json:"source-id,omitempty" parquet:"name=source-id, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY" msg:"source-id"`
	HTTPResponse                *string           `json:"http-response,omitempty" parquet:"name=http-response, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY" msg:"http-response"`
	ErrorCode                   *string           `json:"error-code,omitempty" parquet:"name=error-code, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY" msg:"error-code"`
	Error                       *string           `json:"error,omitempty" parquet:"name=error, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY" msg:"error"`
	StackTrace                  *string           `json:"stack-trace,omitempty" parquet:"name=stack-trace, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY" msg:"stack-trace"`
	Duration                    *string           `json:"duration,omitempty" parquet:"name=duration, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY" msg:"duration"`
	TraceIP                     []string          `json:"trace-ip,omitempty" parquet:"name=trace-ip, type=MAP, convertedtype=LIST, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8" msg:"trace-ip"`
	Region                      *string           `json:"region,omitempty" parquet:"name=region, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY" msg:"region"`
	AZ                          *string           `json:"az,omitempty" parquet:"name=az, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY" msg:"az"`
	Tags                        []string          `json:"tags,omitempty" parquet:"name=tags, type=MAP, convertedtype=LIST, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8" msg:"tags"`
	Args                        map[string]string `json:"args,omitempty" parquet:"name=args, type=MAP, convertedtype=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8, valuetype=BYTE_ARRAY" msg:"args"`
	TransactionMessageReference *string           `json:"transaction-message-reference,omitempty" parquet:"name=transaction-message-reference, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY" msg:"transaction-message-reference"`
	Ttl                         *string           `json:"ttl,omitempty" parquet:"name=ttl, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY" msg:"ttl"`
	AutoIndex                   *bool             `json:"auto-index,omitempty" parquet:"name=auto-index, type=BOOLEAN" msg:"auto-index"`
	LoggerName                  *string           `json:"logger-name,omitempty" parquet:"name=logger-name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY" msg:"logger-name"`
	ThreadName                  *string           `json:"thread-name,omitempty" parquet:"name=thread-name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY" msg:"thread-name"`
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
		Audit:       new(bool),
		AutoIndex:   new(bool),
		Level:       LevelInfo,
		Message:     "",
	}

	ret.Decode(data)
	ret.UpdateInfo()

	return ret
}

func (l *Log) UpdateInfo() {
	ret := &LogInfo{
		BusinessCapability: l.BusinessCapability,
		BusinessDomain:     l.BusinessDomain,
		BusinessService:    l.BusinessService,
		ApplicationService: l.ApplicationService,
	}

	ret.makeKey()

	l.Info = ret
}

func (l *Log) GetData() map[string]interface{} {
	ret := make(map[string]interface{})

	ret["time"] = l.Time
	ret["level"] = l.Level
	ret["correlation-id"] = l.CorrelationId
	ret["session-id"] = l.SessionId
	ret["message-id"] = l.MessageId
	ret["person-id"] = l.PersonId
	ret["user-id"] = l.UserId
	ret["device-id"] = l.DeviceId
	ret["message"] = l.Message
	ret["business-capability"] = l.BusinessCapability
	ret["business-domain"] = l.BusinessDomain
	ret["business-service"] = l.BusinessService
	ret["application-service"] = l.ApplicationService
	ret["audit"] = l.Audit
	ret["resource-type"] = l.ResourceType
	ret["cloud-provider"] = l.CloudProvider
	ret["source-id"] = l.SourceId
	ret["http-response"] = l.HTTPResponse
	ret["error-code"] = l.ErrorCode
	ret["error"] = l.ErrorCode
	ret["stack-trace"] = l.StackTrace
	ret["duration"] = l.Duration
	ret["trace-ip"] = l.TraceIP
	ret["region"] = l.Region
	ret["az"] = l.AZ
	ret["tags"] = l.Tags
	ret["args"] = l.Args
	ret["transaction-message-reference"] = l.TransactionMessageReference
	ret["ttl"] = l.Ttl
	ret["auto-index"] = l.AutoIndex
	ret["logger-name"] = l.LoggerName
	ret["thread-name"] = l.ThreadName
	ret["extra-fields"] = l.ExtraFields

	return ret
}

func (l *Log) Decode(data map[string]interface{}) {
	for k, v := range data {
		key := strings.ReplaceAll(strings.ToLower(fmt.Sprintf("%v", k)), "_", "-")

		switch key {
		case "time":
			l.Time = v.(string)
		case "timestamp":
			l.Time = v.(string)
		case "level":
			l.Level = v.(string)
		case "lvl":
			l.Level = v.(string)
		case "correlation-id":
			l.CorrelationId = GetStringP(v)
		case "session-id":
			l.SessionId = GetStringP(v)
		case "message-id":
			l.MessageId = GetStringP(v)
		case "person-id":
			l.PersonId = GetStringP(v)
		case "user-id":
			l.UserId = GetStringP(v)
		case "device-id":
			l.DeviceId = GetStringP(v)
		case "message":
			l.Message = v.(string)
		case "msg":
			l.Message = v.(string)
		case "log":
			l.Message = v.(string)
		case "business-capability":
			l.BusinessCapability = v.(string)
		case "business-domain":
			l.BusinessDomain = v.(string)
		case "business-service":
			l.BusinessService = v.(string)
		case "application-service":
			l.ApplicationService = v.(string)
		case "audit":
			val := v.(bool)
			l.Audit = &val
		case "resource-type":
			l.ResourceType = GetStringP(v)
		case "cloud-provider":
			l.CloudProvider = GetStringP(v)
		case "source-id":
			l.SourceId = GetStringP(v)
		case "http-response":
			l.HTTPResponse = GetStringP(v)
		case "error-code":
			l.ErrorCode = GetStringP(v)
		case "error":
			l.ErrorCode = GetStringP(v)
		case "error-message":
			l.ErrorCode = GetStringP(v)
		case "error-msg":
			l.ErrorCode = GetStringP(v)
		case "stack-trace":
			l.StackTrace = GetStringP(v)
		case "duration":
			l.Duration = GetStringP(v)
		case "elapsed":
			l.Duration = GetStringP(v)
		case "elapsed-time":
			l.Duration = GetStringP(v)
		case "trace-ip":
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
		case "transaction-message-reference":
			l.TransactionMessageReference = GetStringP(v)
		case "ttl":
			l.Ttl = GetStringP(v)
		case "auto-index":
			val := v.(bool)
			l.AutoIndex = &val
		case "logger-name":
			l.LoggerName = GetStringP(v)
		case "thread-name":
			l.ThreadName = GetStringP(v)
		default:
			l.ExtraFields[k] = fmt.Sprintf("%s", v)
		}
	}
}

func (l *Log) GetInfo() RecordInfo {
	if l.Info == nil {
		l.UpdateInfo()
	}
	return l.Info
}

func (l *Log) ToString() string {
	return fmt.Sprintf("%+v", l)
}

func (l *Log) Key() string {
	i := l.GetInfo()
	return i.Key()
}

func (l *Log) ToJson() string {
	data, err := json.MarshalIndent(l, "", "\t")

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

	l.UpdateInfo()

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

	l.UpdateInfo()

	return nil
}
