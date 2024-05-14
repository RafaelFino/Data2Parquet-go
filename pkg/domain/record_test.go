package domain_test

import (
	"data2parquet/pkg/domain"
	"testing"
	"time"
)

func TestNewRecord(t *testing.T) {
	t.Log("Testing NewRecord")
	tm := time.Now().Format(time.RFC3339Nano)

	r := domain.NewRecord(map[interface{}]interface{}{
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

	if r == nil {
		t.Error("Record is nil")
		return
	}

	if r.Level != "info" {
		t.Error("Level should be info")
	}

	if r.Message != "test message" {
		t.Error("Message should be test message")
	}

	if r.Time != tm {
		t.Errorf("Time should be now [%s/%s]", r.Time, tm)
	}

	if r.CorrelationId != nil && string(*r.CorrelationId) != "test" {
		t.Error("CorrelationID should be test")
	}

	if r.CloudProvider != nil && string(*r.CloudProvider) != "aws" {
		t.Error("CloudProvider should be aws")
	}

	if r.Region != nil && string(*r.Region) != "us-east-1" {
		t.Error("Region should be us-east-1")
	}

	if r.PersonId != nil && string(*r.PersonId) != "test" {
		t.Error("PersonID should be test")
	}

	if r.BusinessCapability != "test" {
		t.Error("BusinessCapability should be test")
	}

	if r.BusinessDomain != "test" {
		t.Error("BusinessDomain should be test")
	}

	if r.BusinessService != "test" {
		t.Error("BusinessService should be test")
	}

	if r.ApplicationService != "test" {
		t.Error("ApplicationService should be test")
	}

	if r.Audit != nil && !*r.Audit {
		t.Error("Audit should be true")
	}

	j := r.ToJson()

	if j == "" {
		t.Error("JSON should not be empty")
	}

	r2 := &domain.Record{}

	err := r2.FromJson(j)

	if err != nil {
		t.Error("Error parsing JSON")
	}

	if r2.ToJson() != j {
		t.Error("ToJson should be the same")
	}

	r3 := &domain.Record{}

	err = r3.FromMsgPack(r.ToMsgPack())

	if err != nil {
		t.Error("Error parsing MsgPack")
	}

	if r2.ToJson() != r3.ToJson() {
		t.Error("ToJson should be the same")
	}
}
