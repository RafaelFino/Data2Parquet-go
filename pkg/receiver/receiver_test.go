package receiver_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime/debug"
	"testing"
	"time"

	"github.com/oklog/ulid"
	"gopkg.in/loremipsum.v1"

	"data2parquet/pkg/config"
	"data2parquet/pkg/domain"
	"data2parquet/pkg/receiver"
)

func PrepareConfig() *config.Config {
	return &config.Config{
		RecordType:     config.RecordTypeLog,
		BufferType:     config.BufferTypeMem,
		WriterType:     config.WriterTypeFile,
		WriterFilePath: "/tmp/data2parquet",
		BufferSize:     100,
		FlushInterval:  1,
	}
}

func TestReceiver(t *testing.T) {
	cfg := PrepareConfig()
	rec := receiver.NewReceiver(context.Background(), cfg)

	if rec == nil {
		t.Error("Receiver is nil")
	}
}

func TestReceiverPush(t *testing.T) {
	cfg := PrepareConfig()
	rec := receiver.NewReceiver(context.Background(), cfg)

	if rec == nil {
		t.Error("Receiver is nil")
	}

	data := generateData(1000)

	for _, d := range data {
		err := rec.Write(d)

		if err != nil {
			t.Error("Error writing data")
		}
	}

	outputdir := cfg.WriterFilePath + "/" + filepath.Dir(data[0].GetInfo().Target(domain.MakeID(), domain.GetMD5Sum([]byte("blablabla"))))

	err := rec.Flush()

	if err != nil {
		t.Error("Error flushing data")
	}

	err = rec.Close()

	time.Sleep(10 * time.Second)

	if err != nil {
		t.Error("Error closing receiver")
	}

	if _, err := os.Stat(outputdir); errors.Is(err, os.ErrNotExist) {
		t.Error("Output directory does not exist")
	}

	dirs, err := os.ReadDir(outputdir)

	if err != nil {
		t.Errorf("Error reading output directory: %s", outputdir)
	}

	if len(dirs) == 0 {
		t.Error("No files written")
	}

	t.Logf("Output directory: %s, removing test data", outputdir)
	err = os.RemoveAll(outputdir)

	if err != nil {
		t.Errorf("Error removing output directory: %s", outputdir)
	}
}

func generateData(qty int) []domain.Record {
	ret := make([]domain.Record, qty)
	resType := "ec2"
	cloudProvider := "aws"
	httpResult := "200"
	stack := string(debug.Stack())
	start := time.Now()
	region := "us-east-1"
	az := "us-east-1a"
	boolValue := true
	loggerName := "data2parquet"
	threadName := "data2parquet.main"
	words := loremipsum.NewWithSeed(int64(qty))

	args := make(map[string]string)

	for i := 0; i < 25; i++ {
		args[fmt.Sprintf("arg%02d", i)] = fmt.Sprintf("value%02d-%s", i, *getID())
	}

	for i := 0; i < qty; i++ {
		duration := fmt.Sprint(time.Since(start).Milliseconds())
		line := &domain.Log{
			Level:                       "INFO",
			Message:                     words.Sentences(5),
			Time:                        time.Now().Format(time.RFC3339Nano),
			CorrelationId:               getID(),
			SessionId:                   getID(),
			MessageId:                   getID(),
			PersonId:                    getID(),
			UserId:                      getID(),
			DeviceId:                    getID(),
			BusinessCapability:          "business_capability",
			BusinessDomain:              "business_domain",
			BusinessService:             "business_service",
			ApplicationService:          "application_service",
			ResourceType:                &resType,
			CloudProvider:               &cloudProvider,
			SourceId:                    getID(),
			HTTPResponse:                &httpResult,
			ErrorCode:                   getID(),
			StackTrace:                  &stack,
			Duration:                    &duration,
			Region:                      &region,
			AZ:                          &az,
			Tags:                        []string{"tag1", "tag2", "tag3", "tag4", "tag5", "tag6", "tag7", "tag8", "tag9", "tag10"},
			Args:                        args,
			TransactionMessageReference: getID(),
			AutoIndex:                   &boolValue,
			Audit:                       &boolValue,
			LoggerName:                  &loggerName,
			ThreadName:                  &threadName,
			TraceIP:                     []string{"192.168.0.1", "0.0.0.1"},
		}
		ret[i] = line
	}

	return ret
}

func getID() *string {
	entropy := rand.New(rand.NewSource(time.Now().UnixNano()))
	ret := ulid.MustNew(ulid.Timestamp(time.Now()), entropy).String()
	return &ret
}
