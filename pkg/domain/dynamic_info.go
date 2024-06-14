package domain

import (
	"fmt"
	"strings"
	"time"

	"data2parquet/pkg/config"
)

type DynamicInfo struct {
	DynamicService     string `msg:"service" json:"service,omitempty"`
	DynamicDomain      string `msg:"domain" json:"domain,omitempty"`
	DynamicCapability  string `msg:"capability" json:"capability,omitempty"`
	DynamicApplication string `msg:"application" json:"application,omitempty"`
	key                string
}

func NewDynamicInfoFromKey(key string) RecordInfo {
	values := strings.Split(key, KeySeparator)

	for len(values) < 4 {
		values = append(values, "unkown")
	}

	ret := &DynamicInfo{
		DynamicCapability:  values[0],
		DynamicDomain:      values[1],
		DynamicService:     values[2],
		DynamicApplication: values[3],
		key:                key,
	}

	return ret
}

func (i *DynamicInfo) RecordType() string {
	return config.RecordTypeDynamic
}

func (i *DynamicInfo) Capability() string {
	return i.DynamicCapability
}

func (i *DynamicInfo) Domain() string {
	return i.DynamicDomain
}

func (i *DynamicInfo) Service() string {
	return i.DynamicService
}

func (i *DynamicInfo) Application() string {
	return i.DynamicApplication
}

func (i *DynamicInfo) Key() string {
	return fmt.Sprintf("%s%s%s%s%s%s%s", i.Capability(), KeySeparator, i.Domain(), KeySeparator, i.Service(), KeySeparator, i.Application())
}

func (i *DynamicInfo) Target(id string, hash string) string {
	tm := time.Now()
	year, month, day := tm.Date()
	hour, _, _ := tm.Clock()

	return fmt.Sprintf("%s/year=%04d/month=%02d/day=%02d/hour=%02d/%s-%s-%s.parquet", i.Capability(), year, month, day, hour, id, i.Key(), hash)
}

func (i *DynamicInfo) makeKey() {
	i.key = fmt.Sprintf("%s%s%s%s%s%s%s", i.Capability(), KeySeparator, i.Domain(), KeySeparator, i.Service(), KeySeparator, i.Application())
}
