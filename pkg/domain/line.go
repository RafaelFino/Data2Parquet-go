package domain

type Line interface {
	SetSchema(schema string) error
	Decode(data map[interface{}]interface{}) error
	Headers() []string
	ToString() string
	Key() string
}
