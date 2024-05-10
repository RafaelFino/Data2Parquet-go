package main

import (
	"data2parquet/pkg/domain"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"runtime/debug"
	"sync"
	"time"

	"github.com/goccy/go-json"
	"github.com/lmittmann/tint"
	"github.com/mattn/go-isatty"
	"github.com/oklog/ulid"
)

func main() {
	logHandler := tint.NewHandler(os.Stdout, &tint.Options{
		NoColor:    !isatty.IsTerminal(os.Stdout.Fd()),
		Level:      slog.LevelDebug.Level(),
		TimeFormat: time.RFC3339Nano,
	})

	logger := slog.New(logHandler)
	slog.SetDefault(logger)

	count := 500000
	parallel := 4

	start := time.Now()
	result := make(chan *domain.Record, parallel)
	wg := &sync.WaitGroup{}
	wg.Add(parallel)

	for i := 0; i < parallel; i++ {
		go GenerateLog(i, count/parallel, result, wg)
	}

	data := map[string][]*domain.Record{}
	data["logs"] = make([]*domain.Record, 0, count)

	signal := make(chan bool)

	go func(s chan bool, wg *sync.WaitGroup) {
		wg.Wait()
		slog.Info("All logs generated", "duration", time.Since(start))
		signal <- true

	}(signal, wg)

	received := 0
	for {
		select {
		case l := <-result:
			{
				data["logs"] = append(data["logs"], l)
				received++
			}
		case <-signal:
			{
				slog.Info("Received signal to close", "count", len(data["logs"]))
				break
			}
		}

		if received == count {
			break
		} else {
			if received%(count/10) == 0 {
				slog.Info("Received logs", "received", received, "total", count)
			}
		}
	}

	slog.Info("Data generated", "duration", time.Since(start), "count", len(data["logs"]))

	filePath := "data/logs.json"
	file, err := os.Create(filePath)
	if err != nil {
		slog.Error("Error creating output file", "error", err)
		os.Exit(1)
	}

	defer file.Close()

	slog.Info("Marshalling data", "filePath", filePath, "duration", time.Since(start))

	jsonData, err := json.MarshalIndent(data, "", "\t")

	if err != nil {
		slog.Error("Error marshalling data", "error", err)
		os.Exit(1)
	}

	_, err = file.Write(jsonData)

	if err != nil {
		slog.Error("Error writing data to file", "error", err)
		os.Exit(1)
	}

	slog.Info("Data written", "filePath", filePath, "duration", time.Since(start), "fileSize", len(jsonData))

}

func GenerateLog(pid int, count int, result chan *domain.Record, wg *sync.WaitGroup) {
	defer wg.Done()

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

	slog.Info("Starting to generate logs", "pid", pid, "count", count)

	for i := 0; i < count; i++ {
		duration := time.Since(start).Milliseconds()
		line := &domain.Record{
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

		if i%(count/4) == 0 {
			slog.Info("Running", "pid", pid, "count", i)
		}

		result <- line
	}

	slog.Info("Finished generating logs", "pid", pid, "count", count)
}

func GetID() *string {
	entropy := rand.New(rand.NewSource(time.Now().UnixNano()))
	ret := ulid.MustNew(ulid.Timestamp(time.Now()), entropy).String()
	return &ret
}
