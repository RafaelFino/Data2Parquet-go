package domain

type Record interface {
	SetSchema(schema string) error
	Decode(data map[interface{}]interface{}) error
	Headers() []string
	ToString() string
	Key() string
}
