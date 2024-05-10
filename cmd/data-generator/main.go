package main

import (
	"data2parquet/pkg/domain"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"runtime/debug"
	"time"

	"github.com/goccy/go-json"
	"github.com/oklog/ulid"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	count := 1000000
	lines := make([]domain.Log, count)

	resType := "ec2"
	cloudProvider := "aws"
	httpResult := int64(200)
	stack := string(debug.Stack())
	start := time.Now()
	region := "us-east-1"
	az := "us-east-1a"
	boolValue := true
	loggerName := "data2parquet"
	threadName := "data2parquet.main"

	for i := 0; i < count; i++ {
		duration := time.Since(start).Milliseconds()
		lines[i] = domain.Log{
			Level:                       "INFO",
			Message:                     "My random log message to text to parquet conversion, index " + fmt.Sprintf("%d", i),
			Time:                        time.Now().Format(time.RFC3339Nano),
			CorrelationId:               GetID(),
			SessionId:                   GetID(),
			MessageId:                   GetID(),
			PersonId:                    GetID(),
			UserId:                      GetID(),
			DeviceId:                    GetID(),
			BusinessCapability:          "business_capability" + fmt.Sprintf("%d", i%10),
			BusinessDomain:              "business_domain" + fmt.Sprintf("%d", i%10),
			BusinessService:             "business_service" + fmt.Sprintf("%d", i%20),
			ApplicationService:          "application_service" + fmt.Sprintf("%d", i%30),
			ResourceType:                &resType,
			CloudProvider:               &cloudProvider,
			SourceId:                    GetID(),
			HTTPResponse:                &httpResult,
			ErrorCode:                   GetID(),
			StackTrace:                  &stack,
			Duration:                    &duration,
			Region:                      &region,
			AZ:                          &az,
			Tags:                        []string{"tag1", "tag2", "tag3", "tag4", "tag5", "tag6", "tag7", "tag8", "tag9", "tag10"},
			Args:                        map[string]string{"arg1": "val1", "arg2": "val2", "arg3": "val3", "arg4": "val4", "arg5": "val5", "arg6": "val6", "arg7": "val7", "arg8": "val8", "arg9": "val9", "arg10": "val10"},
			TransactionMessageReference: GetID(),
			AutoIndex:                   &boolValue,
			Audit:                       &boolValue,
			LoggerName:                  &loggerName,
			ThreadName:                  &threadName,
			TraceIP:                     []string{"192.168.0.1", "0.0.0.1"},
		}
	}

	filePath := "data/logs.json"
	file, err := os.Create(filePath)
	if err != nil {
		slog.Error("Error creating output file", "error", err)
		os.Exit(1)
	}

	defer file.Close()

	data := map[string][]domain.Log{}
	data["logs"] = lines

	jsonData, err := json.MarshalIndent(data, "", " ")

	if err != nil {
		slog.Error("Error marshalling data", "error", err)
		os.Exit(1)
	}

	_, err = file.Write(jsonData)

	if err != nil {
		slog.Error("Error writing data to file", "error", err)
		os.Exit(1)
	}

	slog.Info("Data written", "filePath", filePath)

}

func GetID() *string {
	entropy := rand.New(rand.NewSource(time.Now().UnixNano()))
	ret := ulid.MustNew(ulid.Timestamp(time.Now()), entropy).String()
	return &ret
}
