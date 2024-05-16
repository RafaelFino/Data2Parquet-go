package domain

import (
	"encoding/json"
	"fmt"

	msgp "github.com/vmihailenco/msgpack/v5"
	"golang.org/x/exp/slog"
)

type Dynamic struct {
	Data map[string]interface{} `msg:"data" json:"data"`
}

func NewDynamic(data map[interface{}]interface{}) Record {
	ret := &Dynamic{
		Data: make(map[string]interface{}),
	}

	ret.Decode(data)

	return ret
}

func (d *Dynamic) Decode(data map[interface{}]interface{}) {
	for k, v := range data {
		d.Data[fmt.Sprint(k)] = v
	}
}

func (d *Dynamic) Key() string {
	return fmt.Sprintf("%+v", d)
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