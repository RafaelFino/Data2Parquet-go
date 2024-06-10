package domain

import (
	"data2parquet/pkg/config"
	"fmt"
	"strings"
	"time"
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

func (i *DynamicInfo) Target() string {
	tm := time.Now()
	year, month, day := tm.Date()
	hour, min, sec := tm.Clock()

	return fmt.Sprintf("%s/year=%04d/month=%02d/day=%02d/hour=%02d/%02d%02d%02d-%s.parquet", i.Capability(), year, month, day, hour, hour, min, sec, i.Key())
}

func (i *DynamicInfo) makeKey() {
	i.key = fmt.Sprintf("%s%s%s%s%s%s%s", i.Capability(), KeySeparator, i.Domain(), KeySeparator, i.Service(), KeySeparator, i.Application())
}
