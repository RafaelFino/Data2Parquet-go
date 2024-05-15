package domain

import (
	"data2parquet/pkg/config"
	"encoding/json"
	"fmt"
	"os"

	msgp "github.com/vmihailenco/msgpack/v5"
	"golang.org/x/exp/slog"
)

var schemas = make(map[string]string)

type Dynamic struct {
	jsonSchema string
}

func NewDynamic(config *config.Config, data map[interface{}]interface{}) Record {
	ret := &Dynamic{}

	if len(config.JsonSchema) == 0 {
		slog.Error("Dynamic schema is empty", "module", "domain.record", "function", "NewDynamic")
		return nil
	}

	if schema, ok := schemas[config.JsonSchema]; ok {
		ret.jsonSchema = schema
	} else {
		file, err := os.ReadFile(config.JsonSchema)

		if err != nil {
			slog.Error("Error reading schema file", "error", err, "module", "domain.record", "function", "NewDynamic")
			return nil
		}

		ret.jsonSchema = string(file)
		schemas[config.JsonSchema] = ret.jsonSchema
	}

	ret.Decode(data)

	return ret
}

func (d *Dynamic) Decode(data map[interface{}]interface{}) {

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
