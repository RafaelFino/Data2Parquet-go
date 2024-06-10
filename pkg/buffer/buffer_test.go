package buffer_test

import (
	"bytes"
	"context"
	"data2parquet/pkg/buffer"
	"data2parquet/pkg/config"
	"data2parquet/pkg/domain"
	"fmt"
	"log"
	"math/rand"
	"runtime/debug"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/oklog/ulid"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"gopkg.in/loremipsum.v1"
)

func PrepareConfigMem() *config.Config {
	return &config.Config{
		RecordType: config.RecordTypeLog,
		BufferType: config.BufferTypeMem,
	}
}

func PrepareConfigRedis() *config.Config {
	ret := &config.Config{}

	err := ret.Set(map[string]string{
		"RecordType": config.RecordTypeLog,
		"BufferType": config.BufferTypeRedis,
		"RedisHost":  "localhost",
		"RedisPort":  "6379",
	})

	if err != nil {
		log.Fatalf("Error setting config: %s", err)
	}

	return ret
}

func TestMem(t *testing.T) {
	cfg := PrepareConfigMem()
	buf := buffer.New(context.Background(), cfg)

	if buf == nil {
		t.Error("Buffer is nil")
	}

	testBuffer(buf, t)
}

func TestRedis(t *testing.T) {
	cfg := PrepareConfigRedis()

	ctx := context.Background()
	req := testcontainers.ContainerRequest{
		Image:        "redis:latest",
		ExposedPorts: []string{"6379/tcp"},
		WaitingFor:   wait.ForLog("Ready to accept connections"),
	}
	redisC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		log.Fatalf("Could not start redis: %s", err)
	}
	defer func() {
		if err := redisC.Terminate(ctx); err != nil {
			t.Errorf("Could not stop redis: %s", err)
		}
	}()

	endpoint, err := redisC.Endpoint(ctx, "")
	if err != nil {
		t.Error(err)
	}

	client := redis.NewClient(&redis.Options{
		Addr: endpoint,
	})

	buf := buffer.NewRedis(context.Background(), cfg, client)

	testBuffer(buf, t)
}

func testBuffer(buf buffer.Buffer, t *testing.T) {
	if buf == nil {
		t.Error("Buffer is nil")
	}

	if !buf.IsReady() {
		t.Error("Buffer is not ready")
	}

	if buf.HasRecovery() {
		t.Error("Buffer has recovery")
	}

	if buf.Keys() == nil {
		t.Error("Buffer keys is nil")
	}

	var key string = "test"
	data := generateData(100)

	for _, record := range data {
		_, err := buf.Push(key, record)

		if err != nil {
			t.Error(err)
		}
	}

	if buf.Len(key) != 100 {
		t.Error("Buffer length is not 100")
	}

	result := buf.Get(key)

	if len(result) != 100 {
		t.Error("Buffer length is not 100")
	}

	for i, record := range result {
		recData := record.GetData()

		if recData == nil {
			t.Error("Record data is nil")
		}

		if recData["level"] != "INFO" {
			t.Error("Record level is not INFO")
		}

		if recData["message"] == "" {
			t.Error("Record message is empty")
		}

		if recData["time"] == "" {
			t.Error("Record time is empty")
		}

		if recData["correlation_id"] == "" {
			t.Error("Record correlation_id is empty")
		}

		if recData["session_id"] == "" {
			t.Error("Record session_id is empty")
		}

		if recData["message_id"] == "" {
			t.Error("Record message_id is empty")
		}

		if recData["person_id"] == "" {
			t.Error("Record person_id is empty")
		}

		if recData["user_id"] == "" {
			t.Error("Record user_id is empty")
		}

		if recData["device_id"] == "" {
			t.Error("Record device_id is empty")
		}

		if recData["business_capability"] == "" {
			t.Error("Record business_capability is empty")
		}

		if recData["business_domain"] == "" {
			t.Error("Record business_domain is empty")
		}

		if recData["business_service"] == "" {
			t.Error("Record business_service is empty")
		}

		if recData["application_service"] == "" {
			t.Error("Record application_service is empty")
		}

		if recData["resource_type"] == "" {
			t.Error("Record resource_type is empty")
		}

		if recData["cloud_provider"] == "" {
			t.Error("Record cloud_provider is empty")
		}

		if recData["source_id"] == "" {
			t.Error("Record source_id is empty")
		}

		if recData["http_response"] == "" {
			t.Error("Record http_response is empty")
		}

		if recData["error_code"] == "" {
			t.Error("Record error_code is empty")
		}

		if recData["stack_trace"] == "" {
			t.Error("Record stack_trace is empty")
		}

		if recData["duration"] == "" {
			t.Error("Record duration is empty")
		}

		if recData["region"] == "" {
			t.Error("Record region is empty")
		}

		if recData["az"] == "" {
			t.Error("Record az is empty")
		}

		if recData["tags"] == nil {
			t.Error("Record tags is nil")
		}

		if recData["args"] == nil {
			t.Error("Record args is nil")
		}

		if recData["transaction_message_reference"] == "" {
			t.Error("Record transaction_message_reference is empty")
		}

		if recData["auto_index"] == "" {
			t.Error("Record auto_index is empty")
		}

		if recData["audit"] == "" {
			t.Error("Record audit is empty")
		}

		if recData["logger_name"] == "" {
			t.Error("Record logger_name is empty")
		}

		if recData["thread_name"] == "" {
			t.Error("Record thread_name is empty")
		}

		if recData["trace_ip"] == nil {
			t.Error("Record trace_ip is nil")
		}

		source := data[i]

		if source.ToJson() != record.ToJson() {
			t.Error("Record to json is not equal")
		}
	}

	for _, record := range data {
		err := buf.PushDLQ(key, record)

		if err != nil {
			t.Error(err)
		}
	}

	recData := generateData(100)

	bts := bytes.Buffer{}

	for _, record := range recData {
		item := record.ToMsgPack()
		n, err := bts.Write(item)

		if err != nil {
			t.Error(err)
		}

		if n != len(item) {
			t.Error("Buffer write size is not equal")
		}

		err = buf.PushRecovery(key, &bts)

		if err != nil {
			t.Error(err)
		}
	}

	if !buf.HasRecovery() {
		t.Error("Buffer has recovery")
	}

	recs, err := buf.GetRecovery()

	if err != nil {
		t.Error(err)
	}

	if len(recs) != 100 {
		t.Error("Recovery length is not 100")
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
			BusinessCapability:          "business_capability" + fmt.Sprintf("%02d", i%10),
			BusinessDomain:              "business_domain" + fmt.Sprintf("%02d", i%10),
			BusinessService:             "business_service" + fmt.Sprintf("%02d", i%20),
			ApplicationService:          "application_service" + fmt.Sprintf("%02d", i%30),
			CorrelationId:               getID(),
			SessionId:                   getID(),
			MessageId:                   getID(),
			PersonId:                    getID(),
			UserId:                      getID(),
			DeviceId:                    getID(),
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
