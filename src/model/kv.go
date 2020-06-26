package model

type StrKV interface {
	Get(key string) string
	Put(key, value string)
	Append(key, value string)
}

type IntKV interface {
	Get(key int64) string
	Put(key int64, value string)
	Append(key int64, value string)
}