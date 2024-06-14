package domain

import (
	"encoding/json"
	"fmt"

	msgp "github.com/vmihailenco/msgpack/v5"
)

type Dynamic struct {
	Data map[string]interface{} `msg:"data" json:"data"`
	Info *DynamicInfo           `msg:"info" json:"info,omitempty"`
}

func NewDynamic(data map[string]interface{}) Record {
	ret := &Dynamic{
		Data: make(map[string]interface{}),
	}

	ret.Decode(data)
	ret.UpdateInfo()

	return ret
}

func (d *Dynamic) UpdateInfo() {
	d.Info = &DynamicInfo{}

	d.Info.DynamicService = "dynamic_service"
	if v, ok := d.Data["service"]; ok {
		d.Info.DynamicService = fmt.Sprintf("%s", v)
	}

	d.Info.DynamicDomain = "dynamic_domain"
	if v, ok := d.Data["domain"]; ok {
		d.Info.DynamicDomain = fmt.Sprintf("%s", v)
	}

	d.Info.DynamicCapability = "dynamic_capability"
	if v, ok := d.Data["capability"]; ok {
		d.Info.DynamicCapability = fmt.Sprintf("%s", v)
	}

	d.Info.DynamicApplication = "dynamic_application"
	if v, ok := d.Data["application"]; ok {
		d.Info.DynamicApplication = fmt.Sprintf("%s", v)
	}

	d.Info.makeKey()
}

func (d *Dynamic) GetData() map[string]interface{} {
	return d.Data
}

func (d *Dynamic) Decode(data map[string]interface{}) {
	for k, v := range data {
		d.Data[fmt.Sprint(k)] = v
	}
}

func (d *Dynamic) GetInfo() RecordInfo {
	return d.Info
}

func (d *Dynamic) Key() string {
	return d.Info.Key()
}

func (d *Dynamic) ToString() string {
	return fmt.Sprintf("%+v", d)
}

func (d *Dynamic) ToJson() string {
	data, err := json.Marshal(d)

	if err != nil {
		slog.Error("Error marshalling JSON", "error", err)
		return ""
	}

	return string(data)
}

func (d *Dynamic) FromJson(data string) error {
	err := json.Unmarshal([]byte(data), d)

	if err != nil {
		slog.Error("Error unmarshalling JSON", "error", err)
		return err
	}

	return nil
}

func (d *Dynamic) ToMsgPack() []byte {
	data, err := msgp.Marshal(d)

	if err != nil {
		slog.Error("Error marshalling MsgPack", "error", err)
		return nil
	}

	return data
}

func (d *Dynamic) FromMsgPack(data []byte) error {
	err := msgp.Unmarshal(data, d)

	if err != nil {
		slog.Error("Error unmarshalling MsgPack", "error", err)
		return err
	}

	return nil
}
