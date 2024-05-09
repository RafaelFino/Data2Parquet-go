package domain

type Line interface {
	Decode(data map[interface{}]interface{}) error
	Headers() []string
	ToString() string
	Key() string
}
