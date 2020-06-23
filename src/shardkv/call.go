package shardkv

import "time"

func call(timeout time.Duration, callback func()) bool {
	done := make(chan bool, 1)
	go func() {
		callback()
		done <- true
		close(done)
	}()

	t := time.NewTimer(timeout)
	select {
	case <- t.C:
		return false
	case <- done:
		t.Stop()
	}
	return true
}
