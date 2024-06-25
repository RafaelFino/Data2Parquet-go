package domain

import (
	"fmt"
	"strings"
	"time"

	"data2parquet/pkg/config"
)

type LogInfo struct {
	BusinessCapability string `msg:"business-capability" json:"business-capability,omitempty"`
	BusinessDomain     string `msg:"business-domain" json:"business-domain,omitempty"`
	BusinessService    string `msg:"business-service" json:"business-service,omitempty"`
	ApplicationService string `msg:"application-service" json:"application-service,omitempty"`
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
