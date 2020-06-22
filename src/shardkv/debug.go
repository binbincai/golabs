package shardkv

import "log"

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func DAssert(cond bool) {
	if !cond {
		panic("")
	}
}