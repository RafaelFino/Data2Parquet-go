package domain

import (
	"fmt"
	"strings"
	"time"

	"data2parquet/pkg/config"
)

type LogInfo struct {
	BusinessCapability string `msg:"business_capability" json:"business_capability,omitempty"`
	BusinessDomain     string `msg:"business_domain" json:"business_domain,omitempty"`
	BusinessService    string `msg:"business_service" json:"business_service,omitempty"`
	ApplicationService string `msg:"application_service" json:"application_service,omitempty"`
	key                string
}

func NewLogInfoFromKey(key string) RecordInfo {
	values := strings.Split(key, KeySeparator)

	for len(values) < 4 {
		values = append(values, "unkown")
	}

	ret := &LogInfo{
		BusinessCapability: values[0],
		BusinessDomain:     values[1],
		BusinessService:    values[2],
		ApplicationService: values[3],
		key:                key,
	}

	return ret
}

func (i *LogInfo) RecordType() string {
	return config.RecordTypeLog
}

func (i *LogInfo) Capability() string {
	return i.BusinessCapability
}

func (i *LogInfo) Domain() string {
	return i.BusinessDomain
}

func (i *LogInfo) Service() string {
	return i.BusinessService
}

func (i *LogInfo) Application() string {
	return i.ApplicationService
}

func (i *LogInfo) Key() string {
	return i.key
}

func (i *LogInfo) Target(id string, hash string) string {
	tm := time.Now()
	year, month, day := tm.Date()
	hour, _, _ := tm.Clock()

	return fmt.Sprintf("capability=%s/year=%04d/month=%02d/day=%02d/hour=%02d/%s-%s%s.parquet", i.Capability(), year, month, day, hour, id, i.Key(), hash)
}

func (i *LogInfo) makeKey() {
	i.key = fmt.Sprintf("%s%s%s%s%s%s%s", i.Capability(), KeySeparator, i.Domain(), KeySeparator, i.Service(), KeySeparator, i.Application())
}
