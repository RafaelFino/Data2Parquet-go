package domain

import "fmt"

type Log struct {
	Time                        string            `json:"time" parquet:"name=time, type=TIMESTAMP_MILLIS"`
	Level                       string            `json:"level" parquet:"name=level, type=UTF8 string size=9"`
	CorrelationId               *string           `json:"correlation_id,omitempty" parquet:"name=correlation_id, type=UTF8 type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY size=48"`
	SessionId                   *string           `json:"session_id,omitempty" parquet:"name=session_id, type=UTF8 type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY size=26"`
	MessageId                   *string           `json:"message_id,omitempty" parquet:"name=message_id, type=UTF8 type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY size=26"`
	PersonId                    *string           `json:"person_id,omitempty" parquet:"name=person_id, type=UTF8 type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY size=26"`
	UserId                      *string           `json:"user_id,omitempty" parquet:"name=user_id, type=UTF8 type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY size=26"`
	DeviceId                    *string           `json:"device_id,omitempty" parquet:"name=device_id, type=UTF8 type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY size=26"`
	Message                     string            `json:"message" parquet:"name=message, type=UTF8 type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY size=2000"`
	BusinessCapability          string            `json:"business_capability" parquet:"name=business_capability, type=UTF8 type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY size=26"`
	BusinessDomain              string            `json:"business_domain" parquet:"name=business_domain, type=UTF8 type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY size=26"`
	BusinessService             string            `json:"business_service" parquet:"name=business_service, type=UTF8 type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY size=26"`
	ApplicationService          string            `json:"application_service" parquet:"name=application_service, type=UTF8 type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY size=100"`
	Audit                       *bool             `json:"audit,omitempty" parquet:"name=audit, type=BOOLEAN"`
	ResourceType                *string           `json:"resource_type" parquet:"name=resource_type, type=UTF8 type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY size=14"`
	CloudProvider               *string           `json:"cloud_provider" parquet:"name=cloud_provider, type=UTF8 type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY size=5"`
	SourceId                    *string           `json:"source_id,omitempty" parquet:"name=source_id, type=UTF8 type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY size=60"`
	HTTPResponse                *int              `json:"http_response,omitempty" parquet:"name=http_response, type=INT32"`
	ErrorCode                   *string           `json:"error_code,omitempty" parquet:"name=error_code, type=UTF8 type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY size=40"`
	StackTrace                  *string           `json:"stack_trace,omitempty" parquet:"name=stack_trace, type=UTF8 type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY size=2000"`
	Duration                    *float64          `json:"duration,omitempty" parquet:"name=duration, type=DOUBLE"`
	TraceIP                     []string          `json:"trace_ip,omitempty" parquet:"name=trace_ip, type=LIST, convertedtype=LIST, elementtype=UTF8, repetitiontype=REPEATED"`
	Region                      *string           `json:"region,omitempty" parquet:"name=region, type=UTF8 type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY size=15"`
	AZ                          *string           `json:"az,omitempty" parquet:"name=az, type=UTF8 type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY size=20"`
	Tags                        []string          `json:"tags,omitempty" parquet:"name=tags, type=MAP, convertedtype=LIST, valuetype=BYTE_ARRAY, valueconvertedtype=UTF8"`
	Args                        map[string]string `json:"args,omitempty" parquet:"name=args, type=MAP, convertedtype=MAP, keytype=BYTE_ARRAY, keyconvertedtype=UTF8, valuetype=UTF8, valueconvertedtype=UTF8"`
	TransactionMessageReference *string           `json:"transaction_message_reference,omitempty" parquet:"name=transaction_message_reference, type=UTF8 type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY size=512"`
	Ttl                         *int              `json:"ttl,omitempty" parquet:"name=ttl, type=INT32"`
	AutoIndex                   *bool             `json:"auto_index,omitempty" parquet:"name=auto_index, type=BOOLEAN"`
	LoggerName                  *string           `json:"logger_name,omitempty" parquet:"name=logger_name, type=UTF8 type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY size=256"`
	ThreadName                  *string           `json:"thread_name,omitempty" parquet:"name=thread_name, type=UTF8 type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY size=256"`
}

func NewLog() Record {
	return &Log{}
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

func (l *Log) SetSchema(schema string) error {
	return nil
}

func (l *Log) ToString() string {
	return fmt.Sprintf("%+v", l)
}

func (l *Log) Decode(data map[interface{}]interface{}) error {
	return nil
}

func (l *Log) Headers() []string {
	return nil
}

func (l *Log) Key() string {
	return ""
}
