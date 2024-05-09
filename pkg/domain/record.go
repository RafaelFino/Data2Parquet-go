package domain

type Record interface {
	Decode(data map[interface{}]interface{})
	ToString() string
	Key() string
}
