package model

import "encoding/binary"

type strKV struct {
	kv map[string]string
}

func (mk *strKV) Get(key string) string {
	if value, ok := mk.kv[key]; ok {
		return value
	}
	return ""
}

func (mk *strKV) Put(key, value string) {
	mk.kv[key] = value
}

func (mk *strKV) Append(key, value string) {
	mk.kv[key] += value
}

type intKV struct {
	kv StrKV
}

func (i *intKV) key(key int64) string {
	ikey := make([]byte, 8)
	binary.BigEndian.PutUint64(ikey, uint64(key))
	return string(ikey)
}

func (i *intKV) Get(key int64) string {
	i.kv.Get(i.key(key))
}

func (i *intKV) Put(key int64, value string) {
	i.kv.Put(i.key(key), value)
}

func (i *intKV) Append(key int64, value string) {
	i.kv.Append(i.key(key), value)
}




