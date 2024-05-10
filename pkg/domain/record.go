package domain

import (
	"encoding/json"
	"fmt"
	"strings"

	"golang.org/x/exp/slog"
)

type Record struct {
	Time                        string            `json:"time" parquet:"name=time, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Level                       string            `json:"level" parquet:"name=level, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	CorrelationId               *string           `json:"correlation_id,omitempty" parquet:"name=correlation_id, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	SessionId                   *string           `json:"session_id,omitempty" parquet:"name=session_id, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	MessageId                   *string           `json:"message_id,omitempty" parquet:"name=message_id, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	PersonId                    *string           `json:"person_id,omitempty" parquet:"name=person_id, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	UserId                      *string           `json:"user_id,omitempty" parquet:"name=user_id, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	DeviceId                    *string           `json:"device_id,omitempty" parquet:"name=device_id, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Message                     string            `json:"message" parquet:"name=message, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	BusinessCapability          string            `json:"business_capability" parquet:"name=business_capability, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	BusinessDomain              string            `json:"business_domain" parquet:"name=business_domain, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	BusinessService             string            `json:"business_service" parquet:"name=business_service, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	ApplicationService          string            `json:"application_service" parquet:"name=application_service, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Audit                       *bool             `json:"audit,omitempty" parquet:"name=audit, type=BOOLEAN"`
	ResourceType                *string           `json:"resource_type" parquet:"name=resource_type, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	CloudProvider               *string           `json:"cloud_provider" parquet:"name=cloud_provider, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	SourceId                    *string           `json:"source_id,omitempty" parquet:"name=source_id, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	HTTPResponse                *int64            `json:"http_response,omitempty" parquet:"name=http_response, type=INT32"`
	ErrorCode                   *string           `json:"error_code,omitempty" parquet:"name=error_code, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	StackTrace                  *string           `json:"stack_trace,omitempty" parquet:"name=stack_trace, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Duration                    *int64            `json:"duration,omitempty" parquet:"name=duration, type=INT64, convertedtype=UINT_64"`
	TraceIP                     []string          `json:"trace_ip,omitempty" parquet:"name=trace_ip, type=MAP, convertedtype=LIST, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8"`
	Region                      *string           `json:"region,omitempty" parquet:"name=region, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	AZ                          *string           `json:"az,omitempty" parquet:"name=az, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Tags                        []string          `json:"tags,omitempty" parquet:"name=tags, type=MAP, convertedtype=LIST, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8"`
	Args                        map[string]string `json:"args,omitempty" parquet:"name=args, type=MAP, convertedtype=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8, valuetype=BYTE_ARRAY"`
	TransactionMessageReference *string           `json:"transaction_message_reference,omitempty" parquet:"name=transaction_message_reference, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Ttl                         *int64            `json:"ttl,omitempty" parquet:"name=ttl, type=INT64"`
	AutoIndex                   *bool             `json:"auto_index,omitempty" parquet:"name=auto_index, type=BOOLEAN"`
	LoggerName                  *string           `json:"logger_name,omitempty" parquet:"name=logger_name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	ThreadName                  *string           `json:"thread_name,omitempty" parquet:"name=thread_name, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	ExtraFields                 map[string]string `json:"extra_fields,omitempty" parquet:"name=extra_fields, type=MAP, convertedtype=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8, valuetype=BYTE_ARRAY"`
}

var CloudProviderAWS = "aws"
var CloudProviderGCP = "gcp"
var CloudProviderOCI = "oci"
var CloudProviderAzure = "azure"

var CloudProviderType = map[string]int{
	CloudProviderAWS:   0,
	CloudProviderGCP:   1,
	CloudProviderOCI:   2,
	CloudProviderAzure: 3,
}

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

func NewLog(data map[interface{}]interface{}) *Record {
	ret := &Record{
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

func (r *Record) ToString() string {
	return fmt.Sprintf("%+v", r)
}

func GetStringP(s interface{}) *string {
	if s == nil {
		return nil
	}

	ret := fmt.Sprintf("%s", s)

	if len(ret) == 0 {
		return nil
	}

	return &ret
}

func (r *Record) Decode(data map[interface{}]interface{}) {
	for k, v := range data {

		key := strings.ToLower(fmt.Sprintf("%v", k))

		switch key {
		case "time":
			r.Time = v.(string)
		case "level":
			r.Level = v.(string)
		case "correlation_id":
			r.CorrelationId = GetStringP(v)
		case "session_id":
			r.SessionId = GetStringP(v)
		case "message_id":
			r.MessageId = GetStringP(v)
		case "person_id":
			r.PersonId = GetStringP(v)
		case "user_id":
			r.UserId = GetStringP(v)
		case "device_id":
			r.DeviceId = GetStringP(v)
		case "message":
			r.Message = v.(string)
		case "business_capability":
			r.BusinessCapability = v.(string)
		case "business_domain":
			r.BusinessDomain = v.(string)
		case "business_service":
			r.BusinessService = v.(string)
		case "application_service":
			r.ApplicationService = v.(string)
		case "audit":
			val := v.(bool)
			r.Audit = &val
		case "resource_type":
			r.ResourceType = GetStringP(v)
		case "cloud_provider":
			r.CloudProvider = GetStringP(v)
		case "source_id":
			r.SourceId = GetStringP(v)
		case "http_response":
			val := int64(v.(float64))
			r.HTTPResponse = &val
		case "error_code":
			r.ErrorCode = GetStringP(v)
		case "stack_trace":
			r.StackTrace = GetStringP(v)
		case "duration":
			val := int64(v.(float64))
			r.Duration = &val
		case "trace_ip":
			r.TraceIP = make([]string, len(v.([]interface{})))

			for p, ip := range v.([]interface{}) {
				r.TraceIP[p] = ip.(string)
			}
		case "region":
			r.Region = GetStringP(v)
		case "az":
			r.AZ = GetStringP(v)
		case "tags":
			r.Tags = make([]string, len(v.([]interface{})))

			for p, tag := range v.([]interface{}) {
				r.Tags[p] = tag.(string)
			}
		case "args":
			for arg_key, arg_val := range v.(map[string]interface{}) {
				r.Args[arg_key] = arg_val.(string)
			}
		case "transaction_message_reference":
			r.TransactionMessageReference = GetStringP(v)
		case "ttl":
			val := int64(v.(float64))
			r.Ttl = &val
		case "auto_index":
			val := v.(bool)
			r.AutoIndex = &val
		case "logger_name":
			r.LoggerName = GetStringP(v)
		case "thread_name":
			r.ThreadName = GetStringP(v)
		default:
			r.ExtraFields[k.(string)] = v.(string)
		}
	}
}

func (r *Record) Key() string {
	return fmt.Sprintf("%s-%s-%s", r.BusinessCapability, r.BusinessDomain, r.BusinessService)
}

func (r *Record) ToJson() string {
	data, err := json.Marshal(r)

	if err != nil {
		slog.Error("Error marshalling JSON", "error", err)
		return ""
	}

	return string(data)
}

func (r *Record) FromJson(data string) error {
	err := json.Unmarshal([]byte(data), r)

	if err != nil {
		slog.Error("Error unmarshalling JSON", "error", err)
		return err
	}

	return nil
}
