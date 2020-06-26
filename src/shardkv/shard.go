package shardkv

import (
	"fmt"
	"github.com/binbincai/golabs/src/lablog"
	"github.com/binbincai/golabs/src/shardmaster"
)

type Shards struct {
	// own: 当前这个shard kv负责的shard.
	// migrate: 当前这个shard kv已经不负责该shard, 但是还未被多数派认可, 仍然可以写入.
	// migrated: 当前这个shard kv已经不负责该shard, 已经被多数派认可, 不可写入, 等待发送.
	own               map[int]map[string]string
	migrate           map[int]map[string]string
	migrated          map[int]map[string]string

	triggerTag map[int]int64
	sendTag    map[int]int64
	finishTag  map[int]int64

	cache map[int]map[int64]Result

	gid int
}

func newShards(numShard, gid int) *Shards {
	s := &Shards{
		own: make(map[int]map[string]string),
		migrate: make(map[int]map[string]string),
		migrated: make(map[int]map[string]string),

		triggerTag: make(map[int]int64),
		sendTag:    make(map[int]int64),
		finishTag:  make(map[int]int64),

		cache: make(map[int]map[int64]Result),

		gid: gid,
	}
	for shard:=0; shard<numShard; shard++ {
		s.cache[shard] = make(map[int64]Result)
	}
	return s
}

func (s *Shards) string() string {
	info := ""
	shards := make([]int, 0)
	for shard := range s.own {
		shards = append(shards, shard)
	}
	info += fmt.Sprintf("own: %v", shards)

	shards = shards[0:0]
	for shard := range s.migrate {
		shards = append(shards, shard)
	}
	info += fmt.Sprintf(", migrate: %v", shards)

	shards = shards[0:0]
	for shard := range s.migrated {
		shards = append(shards, shard)
	}
	info += fmt.Sprintf(", migrated: %v", shards)
	return info
}

func (s *Shards) getKeyValues(key string) map[string]string {
	shard := key2shard(key)
	if keyValue, ok := s.own[shard]; ok {
		lablog.Assert(keyValue != nil)
		return keyValue
	}
	if keyValue, ok := s.migrate[shard]; ok {
		lablog.Assert(keyValue != nil)
		return keyValue
	}
	return nil
}

func (s *Shards) init(config shardmaster.Config) {
	lablog.Assert(s.gid != 0)
	own := make([]int, 0)
	for shard, gid := range config.Shards {
		if gid != s.gid {
			continue
		}
		own = append(own, shard)
	}
	for _, shard := range own {
		s.own[shard] = make(map[string]string)
	}
}

func (s *Shards) tryMigrate(config shardmaster.Config, migrateTags map[int]int64, cb func()) {
	migrate := make([]int, 0)
	for shard := range s.own {
		if config.Shards[shard] != s.gid {
			migrate = append(migrate, shard)
		}
	}
	for _, shard := range migrate {
		s.migrate[shard] = s.own[shard]
		s.triggerTag[shard] = migrateTags[shard]
		lablog.Assert(s.triggerTag[shard] != 0)
		delete(s.own, shard)
	}
	go cb()
}

func (s *Shards) trigger(shard int, sendTag, finishTag int64, cb func()) {
	lablog.Assert(s.migrate[shard] != nil)
	s.migrated[shard] = s.migrate[shard]
	s.sendTag[shard] = sendTag
	s.finishTag[shard] = finishTag
	delete(s.migrate, shard)
	delete(s.triggerTag, shard)
	lablog.Assert(s.migrated[shard] != nil)
	lablog.Assert(s.sendTag[shard] != 0)
	lablog.Assert(s.finishTag[shard] != 0)
	go cb()
}

func (s *Shards) receive(shard int, keyValues map[string]string, cache map[int64]Result) {
	lablog.Assert(keyValues != nil)
	lablog.Assert(cache != nil)
	keyValuesIn := make(map[string]string)
	for k, v := range keyValues {
		keyValuesIn[k] = v
	}
	cacheIn := make(map[int64]Result)
	for k, v := range cache {
		cacheIn[k] = v
	}
	s.own[shard] = keyValuesIn
	s.cache[shard] = cacheIn
}

func (s *Shards) finishMigrate(shard int) {
	delete(s.migrated, shard)
	delete(s.sendTag, shard)
	delete(s.finishTag, shard)
	delete(s.cache, shard)
	s.cache[shard] = make(map[int64]Result)
}

func (s *Shards) isHandled(shard int, tag int64) (bool, Result) {
	if result, ok := s.cache[shard][tag]; ok {
		return true, result
	}
	return false, Result{Err: OK}
}

func (s *Shards) handled(shard int, tag int64, result Result) {
	s.cache[shard][tag] = result
}

func (s *Shards) canRsyncConfig() bool {
	if len(s.migrate) != 0 {
		return false
	}
	if len(s.migrated) != 0 {
		return false
	}
	lablog.Assert(len(s.triggerTag) == 0)
	lablog.Assert(len(s.sendTag) == 0)
	lablog.Assert(len(s.finishTag) == 0)
	return true
}

func (s *Shards) getNextTrigger() (has bool, shard int, tag int64) {
	if len(s.migrate) == 0 {
		return
	}
	if len(s.migrated) != 0 {
		return
	}
	has = true
	for shard = range s.migrate {
		tag = s.triggerTag[shard]
		break
	}
	return
}

func (s *Shards) getMigrateSend() (has bool, shard int, sendTag int64, finishTag int64,
	keyValues map[string]string, cache map[int64]Result) {
	if len(s.migrated) == 0 {
		return
	}
	has = true
	for shard = range s.migrated {
		sendTag = s.sendTag[shard]
		finishTag = s.finishTag[shard]
		keyValues = s.migrated[shard]
		cache = s.cache[shard]
		break
	}
	return
}