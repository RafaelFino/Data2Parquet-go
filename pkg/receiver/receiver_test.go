package receiver_test

import (
	"data2parquet/pkg/buffer"
	"data2parquet/pkg/config"
	"data2parquet/pkg/domain"
	"data2parquet/pkg/receiver"
	"fmt"
	"testing"
	"time"
)

func TestReceiver(t *testing.T) {
	t.Log("Testing Receiver")
	cfg := config.NewConfig()

	if cfg == nil {
		t.Error("Config is nil")
		return
	}

	cfg.BufferType = "mem"
	cfg.BufferSize = 5000
	cfg.WriterType = "none"

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

	start := time.Now()

	rcv := receiver.NewReceiver(cfg)

	if rcv == nil {
		t.Log("Receiver is nil and should not be nil after creation")
		t.Error("Receiver is nil")
	}

	for i := 0; i < 5000; i++ {
		err := rcv.Write(data[i])

		if err != nil {
			t.Log("Error writing record", "error", err)
			t.Error("Error writing record")
		}
	}

	rcv.Close()

	t.Log("Receiver test completed", "duration", time.Since(start))
}
