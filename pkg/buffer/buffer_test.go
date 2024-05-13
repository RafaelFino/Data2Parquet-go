package buffer_test

import (
	"data2parquet/pkg/buffer"
	"data2parquet/pkg/config"
	"data2parquet/pkg/domain"
	"fmt"
	"testing"
	"time"
)

func TestMem(t *testing.T) {
	t.Log("Testing Mem buffer")
	cfg := config.NewConfig()

	if cfg == nil {
		t.Error("Config is nil")
		return
	}

	cfg.BufferType = "mem"
	cfg.BufferSize = 5000

	runTest(t, cfg)
}

func TestRedis(t *testing.T) {
	t.Log("Testing Redis buffer")
	cfg := config.NewConfig()

	if cfg == nil {
		t.Error("Config is nil")
		return
	}

	cfg.BufferType = "ledis"
	cfg.RedisHost = "localhost:6379"
	cfg.RedisPassword = ""
	cfg.RedisDB = 0
	cfg.RedisDataPrefix = "test"
	cfg.RedisKeys = "keys"
	cfg.LedisPath = "/tmp"

	runTest(t, cfg)
}

func runTest(t *testing.T, cfg *config.Config) {
	buf := buffer.NewMem(cfg)

	if buf == nil {
		t.Log("Buffer is nil and should not be nil after creation")
		t.Error("Buffer is nil")
	}

	if !buf.IsReady() {
		t.Log("Buffer is not ready and should be ready")
		t.Error("Buffer is not ready")
	}

	data := make([]*domain.Record, 5000)

	for i := 0; i < 5000; i++ {
		data[i] = domain.NewRecord(map[interface{}]interface{}{
			"level":               "info",
			"message":             fmt.Sprintf("test message %d", i),
			"time":                "2021-01-01T00:00:00Z",
			"correlation_id":      "test",
			"cloud_provider":      "aws",
			"region":              "us-east-1",
			"person_id":           "test",
			"business_capability": "test",
			"business_domain":     "test",
			"business_service":    "test",
			"application_service": "test",
			"audit":               true,
		})
	}

	var err error

	for i := 0; i < 5000; i++ {
		err = buf.Push("test", data[i])

		if err != nil {
			t.Log("Error pushing data to buffer", "error", err)
			t.Error("Error pushing data to buffer")
		}
	}

	for i := 0; i < 60; i++ {
		l := buf.Len("test")
		if l != 5000 {
			time.Sleep(1 * time.Second)
			t.Log("Buffer length is not 5000 yet, waiting", "length", l)
		}
	}

	ret := buf.Get("test")

	if ret == nil {
		t.Log("Data is nil")
		t.Error("Data is nil")
	}

	if len(ret) != 5000 {
		t.Log("Data length is not 5000", "length", len(ret), "data", ret)
		t.Error("Data length is not 5000")
	}

	for i, item := range ret {
		if item == nil {
			t.Log("Data is nil", "index", i)
			t.Error("Data is nil")
		} else {
			if item.Level != "info" {
				t.Log("Level is not info", "level", item.Level)
				t.Error("Level is not info")
			}

			if item.Message != fmt.Sprintf("test message %d", i) {
				t.Log("Message is not correct", "message", item.Message)
				t.Error("Message is not correct")
			}
		}

		if item.Time != "2021-01-01T00:00:00Z" {
			t.Log("Time is not correct", "time", item.Time)
			t.Error("Time is not correct")
		}

		if item.BusinessCapability != "test" {
			t.Log("BusinessCapability is not correct", "business_capability", item.BusinessCapability)
			t.Error("BusinessCapability is not correct")
		}

		if item.BusinessDomain != "test" {
			t.Log("BusinessDomain is not correct", "business_domain", item.BusinessDomain)
			t.Error("BusinessDomain is not correct")
		}

		if item.BusinessService != "test" {
			t.Log("BusinessService is not correct", "business_service", item.BusinessService)
			t.Error("BusinessService is not correct")
		}

		if item.ApplicationService != "test" {
			t.Log("ApplicationService is not correct", "application_service", item.ApplicationService)
			t.Error("ApplicationService is not correct")
		}

		if !(*item.Audit) {
			t.Log("Audit is not correct", "audit", item.Audit)
			t.Error("Audit is not correct")
		}

		if item.ToJson() != data[i].ToJson() {
			t.Log("Data is not correct", "data", item.ToJson())
			t.Error("Data is not correct")
		}

		if item.ToString() != data[i].ToString() {
			t.Log("Data is not correct", "data", item.ToString())
			t.Error("Data is not correct")
		}

		if item.Key() != data[i].Key() {
			t.Log("Key is not correct", "key", item.Key())
			t.Error("Key is not correct")
		}

		if string(item.ToMsgPack()) != string(data[i].ToMsgPack()) {
			t.Log("MsgPack is not correct", "msgpack", item.ToMsgPack())
			t.Error("MsgPack is not correct")
		}

		if item.FromMsgPack(data[i].ToMsgPack()) != nil {
			t.Log("Error decoding msgpack", "error", item.FromMsgPack(data[i].ToMsgPack()))
			t.Error("Error decoding msgpack")
		}

		if item.FromJson(data[i].ToJson()) != nil {
			t.Log("Error decoding json", "error", item.FromJson(data[i].ToJson()))
			t.Error("Error decoding json")
		}
	}
}
