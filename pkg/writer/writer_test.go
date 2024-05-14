package writer_test

import (
	"context"
	"data2parquet/pkg/config"
	"data2parquet/pkg/domain"
	"data2parquet/pkg/writer"
	"testing"
	"time"
)

func TestNone(t *testing.T) {
	t.Log("Testing None writer")
	cfg := config.NewConfig()

	if cfg == nil {
		t.Error("Config is nil")
		return
	}

	cfg.WriterType = "none"

	runTest(t, cfg)
}

func TestFile(t *testing.T) {
	t.Log("Testing File writer")
	cfg := config.NewConfig()

	if cfg == nil {
		t.Error("Config is nil")
		return
	}

	cfg.WriterType = "file"
	cfg.WriterFilePath = "/tmp"

	runTest(t, cfg)
}

func runTest(t *testing.T, cfg *config.Config) {
	t.Logf("Testing %s writer", cfg.WriterType)
	w := writer.New(context.Background(), cfg)

	if w == nil {
		t.Log("Writer is nil and should not be nil after creation")
		t.Fatal("Writer is nil")

	}

	if !w.IsReady() {
		t.Log("Writer is not ready and should be ready")
		t.Fatal("Writer is not ready")
	}

	data := make([]*domain.Record, 5000)
	tm := time.Now().Format(time.RFC3339Nano)

	for i := 0; i < 5000; i++ {
		data[i] = domain.NewRecord(map[interface{}]interface{}{
			"level":               "info",
			"message":             "test message",
			"time":                tm,
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

	ret := w.Write(data)

	if writer.CheckWriterError(ret) {
		for _, r := range ret {
			t.Log("Error writing data", "error", r.Error)
		}

		t.Error("Error writing data")
	}

	err := w.Close()

	if err != nil {
		t.Log("Error closing writer", "error", err)
		t.Error("Error closing writer")
	}
}
