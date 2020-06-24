package shardkv

import (
	"context"
	"fmt"
	"github.com/binbincai/golabs/src/labgob"
	"github.com/binbincai/golabs/src/lablog"
	"github.com/binbincai/golabs/src/labrpc"
	"github.com/binbincai/golabs/src/raft"
	"github.com/binbincai/golabs/src/shardmaster"
	"sync"
	"time"
)



type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Op string

	// Get/Put/Append
	Key string
	Value string

	// Config
	Config shardmaster.Config

	// MigrateShard
	Shard     int
	KeyValues map[string]string
	TriggerTags map[int]int64
	SendTag   int64
	FinishTag int64
	ConfigNum int

	Tag int64
	PrevTag int64

	ResultCh chan Result
}

type Result struct {
	WrongLeader bool
	Err         Err
	Value       string
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	// own: 当前这个shard kv负责的shard.
	// migrate: 当前这个shard kv已经不负责该shard, 但是还未被多数派认可, 仍然可以写入.
	// migrated: 当前这个shard kv已经不负责该shard, 已经被多数派认可, 不可写入, 等待发送.
	own               map[int]map[string]string
	migrate           map[int]map[string]string
	migrated          map[int]map[string]string
	migrateTriggerTag map[int]int64
	migrateSendTag    map[int]int64
	migrateFinishTag  map[int]int64
	migrateTriggering bool
	migratedSending   bool
	migrateCh         chan bool
	needSync 		  bool

	prefers           map[int]int

	pending   map[int64]chan Result
	cache     map[int64]Result
	exitApplyCh    chan bool
	exitRequestCh chan bool
	exitSyncCh chan bool
	requestCh chan Op

	config shardmaster.Config
	fetchingConfig bool

	logger *lablog.Logger
}

func (kv *ShardKV) shardInfoWithLock() string {
	info := ""
	shards := make([]int, 0)
	for shard := range kv.own {
		shards = append(shards, shard)
	}
	info += fmt.Sprintf("own: %v", shards)

	shards = shards[0:0]
	for shard := range kv.migrate {
		shards = append(shards, shard)
	}
	info += fmt.Sprintf(", migrate: %v", shards)

	shards = shards[0:0]
	for shard := range kv.migrated {
		shards = append(shards, shard)
	}
	info += fmt.Sprintf(", migrated: %v", shards)
	return info
}

func (kv *ShardKV) shardInfo() string{
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return kv.shardInfoWithLock()
}

func (kv *ShardKV) requestUser(op Op) bool {
	kv.mu.Lock()
	keyValues := kv.getKeyValues(op.Key)
	kv.mu.Unlock()
	if keyValues == nil {
		lablog.Assert(op.ResultCh != nil)
		op.ResultCh <- Result{Err: ErrWrongGroup}
		close(op.ResultCh)
		kv.logger.Printf(kv.gid, kv.me, "ShardKV.requestUser wrong group, op: %s, tag: %d", op.Op, op.Tag)
		return false
	}
	return true
}

func (kv *ShardKV) requestInner(op Op) bool {
	return true
}

func (kv *ShardKV) request(op Op) {
	kv.logger.Printf(kv.gid, kv.me, "ShardKV.request start, op: %s, tag: %d", op.Op, op.Tag)
	defer kv.logger.Printf(kv.gid, kv.me, "ShardKV.request end, op: %s, tag: %d", op.Op, op.Tag)

	kv.mu.Lock()
	result, cache := kv.cache[op.Tag]
	kv.mu.Unlock()
	if cache {
		lablog.Assert(op.ResultCh != nil)
		op.ResultCh <- result
		close(op.ResultCh)
		return
	}

	switch op.Op {
	case "Config", "MigrateTrigger", "MigrateReceive", "MigrateFinish":
		if !kv.requestInner(op) {
			return
		}
	case "Get", "Put", "Append":
		if !kv.requestUser(op) {
			return
		}
	}

	if !kv.rf.IsLeader() {
		lablog.Assert(op.ResultCh != nil)
		op.ResultCh <- Result{WrongLeader: true}
		close(op.ResultCh)
		kv.logger.Printf(kv.gid, kv.me,"ShardKV.request wrong leader, op: %s, tag: %d", op.Op, op.Tag)
		return
	}

	lablog.Assert(op.ResultCh != nil)
	kv.mu.Lock()
	kv.pending[op.Tag] = op.ResultCh
	kv.mu.Unlock()

	kv.logger.Printf(kv.gid, kv.me,"ShardKV.request before start raft, op: %s, tag: %d", op.Op, op.Tag)
	call(150*time.Millisecond, func() {
		kv.rf.Start(op)
	})
	kv.logger.Printf(kv.gid, kv.me, "ShardKV.request after start raft, op: %s, tag: %d", op.Op, op.Tag)
}

func (kv *ShardKV) backgroundRequest() {
	for {
		select {
		case <- kv.exitRequestCh:
			break
		case requestMsg := <- kv.requestCh:
			kv.request(requestMsg)
		}
	}
}

func (kv *ShardKV) syncConfig() {
	// 同一个时刻, 只允许一个同步请求存在.
	kv.mu.Lock()
	fetchingConfig := kv.fetchingConfig
	defer func() {
		kv.fetchingConfig = fetchingConfig
		kv.mu.Unlock()
	}()

	configNum := 2
	if kv.config.Num != 0 {
		configNum = -1
	}
	// 只有leader节点需要同步配置.
	kv.mu.Unlock()
	isLeader := kv.rf.IsLeader()
	kv.mu.Lock()
	if !isLeader {
		return
	}
	if fetchingConfig {
		return
	}
	kv.fetchingConfig = true

	if len(kv.migrate) != 0 {
		return
	}
	if len(kv.migrated) != 0 {
		return
	}
	lablog.Assert(len(kv.migrateTriggerTag) == 0)
	lablog.Assert(len(kv.migrateSendTag) == 0)
	lablog.Assert(len(kv.migrateFinishTag) == 0)

	kv.mu.Unlock()
	ck := shardmaster.MakeClerk(kv.masters)
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	config := ck.Query2(ctx, configNum)
 	kv.mu.Lock()
	if kv.config.Num >= config.Num {
		if !kv.needSync {
			return
		}
	}

	tag := nrand()
	kv.logger.Printf(kv.gid, kv.me, "ShardKV.syncConfig, cfg num: %d, req cfg num: %d, ret cfg num: %d, tag: %d",
		kv.config.Num, configNum, config.Num, tag)

	op := Op{
		Op: "Config",
		Config: config,
		Tag: tag,
		TriggerTags: make(map[int]int64),
		ResultCh: make(chan Result, 1),
	}
	for shard:=0; shard<shardmaster.NShards; shard++ {
		op.TriggerTags[shard] = nrand()
	}

	call(200*time.Millisecond, func() {
		kv.mu.Unlock()
		defer kv.mu.Lock()
		lablog.Assert(op.ResultCh != nil)
		kv.requestCh <- op
		<-op.ResultCh // 等待请求被处理
	})
}

func (kv *ShardKV) migrateTrigger() {
	if !kv.rf.IsLeader() {
		return
	}

	kv.mu.Lock()
	migrateTriggering := kv.migrateTriggering
	defer func () {
		kv.migrateTriggering = migrateTriggering
		kv.mu.Unlock()
	}()
	if migrateTriggering {
		return
	}
	kv.migrateTriggering = true

	// 触发迁移操作, 将migrate内的kv
	// 迁移到migrated.
	if len(kv.migrate) == 0 {
		return
	}
	// 同一个时刻只有一个在迁移.
	if len(kv.migrated) != 0 {
		return
	}

	for shard := range kv.migrate {
		func (shard int) {
			tag := kv.migrateTriggerTag[shard]
			lablog.Assert(tag != 0)
			shardTag := nrand()
			finishTag := nrand()
			kv.logger.Printf(kv.gid, kv.me, "ShardKV.migrateTrigger start, shard: %d, tag: %d", shard, tag)
			kv.mu.Unlock()
			defer func() {
				kv.mu.Lock()
				kv.logger.Printf(kv.gid,kv.me, "ShardKV.migrateTrigger end, shard: %d, tag: %d", shard, tag)
			}()
			op := Op{
				Op:        "MigrateTrigger",
				Shard:     shard,
				Tag:       tag,
				SendTag:   shardTag,
				FinishTag: finishTag,
				ResultCh:  make(chan Result, 1),
			}
			call(200*time.Millisecond, func() {
				lablog.Assert(op.ResultCh != nil)
				kv.requestCh <- op
				<- op.ResultCh
			})
		}(shard)
		break // 一次迁移一个shard的keyValue
	}
}

func (kv *ShardKV) sendToPeer(config shardmaster.Config, tag int64, shard int, keyValues map[string]string) bool {
	kv.mu.Lock()
	preferIndex := kv.prefers[config.Shards[shard]]
	kv.mu.Unlock()

	args := &MigrateArgs{
		Tag: tag,
		Shard: shard,
		KeyValues: keyValues,
		ConfigNum: config.Num,
	}
	gid := config.Shards[shard]
	servers := config.Groups[gid]

	kv.logger.Printf(kv.gid, kv.me, "ShardKV.sendToPeer start, shard: %d, tag: %d", shard, tag)
	defer kv.logger.Printf(kv.gid, kv.me, "ShardKV.sendToPeer start, shard: %d, tag: %d", shard, tag)

	for i:=0; i<len(servers); i++ {
		serverIndex := (preferIndex+i)%len(servers)
		server := kv.make_end(servers[serverIndex])
		reply := &MigrateReply{}
		ok := server.Call("ShardKV.MigrateShard", args, reply)
		if ok && reply.Err == OK {
			kv.logger.Printf(kv.gid, kv.me, "ShardKV.sendToPeer send succ, to: (%d, %d), shard: %d, tag: %d",
				config.Shards[shard], serverIndex, shard, tag)
			kv.mu.Lock()
			kv.prefers[config.Shards[shard]] = serverIndex
			kv.mu.Unlock()
			return true
		}
		kv.logger.Printf(kv.gid, kv.me, "ShardKV.sendToPeer send fail, to: (%d, %d), reply: %v, shard: %d, tag: %d",
			config.Shards[shard], serverIndex, *reply, shard, tag)
		time.Sleep(100*time.Millisecond)
	}

	return false
}

func (kv *ShardKV) migrateSend() {
	if !kv.rf.IsLeader() {
		return
	}
	kv.mu.Lock()
	migratedSending := kv.migratedSending
	defer func() {
		kv.migratedSending = migratedSending
		kv.mu.Unlock()
	}()
	if migratedSending {
		return
	}
	kv.migratedSending = true

	if len(kv.migrated) == 0 {
		return
	}

	for shard, keyValues := range kv.migrated {
		func(){
			kv.logger.Printf(kv.gid, kv.me, "ShardKV.migrateSend start, shard: %d", shard)
			config := kv.config
			tag := kv.migrateSendTag[shard]
			finishTag := kv.migrateFinishTag[shard]
			lablog.Assert(tag != 0)
			lablog.Assert(finishTag != 0)
			kv.mu.Unlock()
			defer func() {
				kv.mu.Lock()
				kv.logger.Printf(kv.gid, kv.me, "ShardKV.migrateSend end, shard: %d", shard)
			}()

			lablog.Assert(keyValues != nil)
			var success bool
			call(500*time.Millisecond, func() {
				success = kv.sendToPeer(config, tag, shard, keyValues)
			})
			if !success {
				return
			}

			op := Op {
				Op:        "MigrateFinish",
				Shard:     shard,
				Tag: finishTag,
				ResultCh: make(chan Result, 1),
			}
			kv.logger.Printf(kv.gid, kv.me, "ShardKV.migrateSend send MigrateFinish, shard: %d, tag: %d", shard, op.Tag)
			call(200*time.Millisecond, func() {
				lablog.Assert(op.ResultCh != nil)
				kv.requestCh <- op
				<- op.ResultCh
			})
		}()
		break
	}
}

func (kv *ShardKV) backgroundSync() {
	var t *time.Timer
	for {
		if t != nil {
			t.Stop()
		}
		t = time.NewTimer(100*time.Millisecond)
		select {
		case <- kv.exitSyncCh:
			break
		case <- kv.migrateCh:
			kv.syncConfig()
			kv.migrateTrigger()
			kv.migrateSend()
		case <- t.C:
			kv.syncConfig()
			kv.migrateTrigger()
			kv.migrateSend()
		}
	}
}

func (kv *ShardKV) getKeyValues(key string) map[string]string {
	shard := key2shard(key)
	if keyValue, ok := kv.own[shard]; ok {
		lablog.Assert(keyValue != nil)
		return keyValue
	}
	if keyValue, ok := kv.migrate[shard]; ok {
		lablog.Assert(keyValue != nil)
		return keyValue
	}
	return nil
}

func (kv *ShardKV) applyGet(op Op) Result {
	resultMsg := Result{
		Err: OK,
	}
	keyValue := kv.getKeyValues(op.Key)
	if keyValue == nil {
		resultMsg.Err = ErrWrongGroup
		return resultMsg
	}
	if v, ok := keyValue[op.Key]; ok {
		resultMsg.Value = v
	}
	shard := key2shard(op.Key)
	kv.logger.Printf(kv.gid, kv.me, "ShardKV.applyGet, key: %s, ret value: %s, shard: %d, tag: %d, %s",
		op.Key, resultMsg.Value, shard, op.Tag, kv.shardInfoWithLock())
	return resultMsg
}

func (kv *ShardKV) applyPut(op Op) Result {
	resultMsg := Result{
		Err: OK,
	}
	keyValue := kv.getKeyValues(op.Key)
	if keyValue == nil {
		resultMsg.Err = ErrWrongGroup
		return resultMsg
	}
	keyValue[op.Key] = op.Value
	shard := key2shard(op.Key)
	kv.logger.Printf(kv.gid, kv.me, "ShardKV.applyPut, key: %s, value: %s, shard: %d, tag: %d, %s",
		op.Key, op.Value, shard, op.Tag, kv.shardInfoWithLock())
	return resultMsg
}

func (kv *ShardKV) applyAppend(op Op) Result {
	resultMsg := Result{
		Err: OK,
	}
	keyValue := kv.getKeyValues(op.Key)
	if keyValue == nil {
		resultMsg.Err = ErrWrongGroup
		return resultMsg
	}
	keyValue[op.Key] += op.Value
	shard := key2shard(op.Key)
	kv.logger.Printf(kv.gid, kv.me, "ShardKV.applyAppend, key: %s, append value: %s, current value: %s, shard: %d, tag: %d, %s",
		op.Key, op.Value, keyValue[op.Key], shard, op.Tag, kv.shardInfoWithLock())
	return resultMsg
}

func (kv *ShardKV) applyConfig(op Op) Result {
	kv.needSync = false
	resultMsg := Result{
		Err: OK,
	}
	// TODO: 这里如何搞?
	//if op.Config.Num <= kv.config.Num {
	//	return resultMsg
	//}

	kv.config = op.Config
	if kv.config.Num == 2 {
		for shard, gid := range kv.config.Shards {
			if gid != kv.gid {
				continue
			}
			kv.own[shard] = make(map[string]string)
		}
		kv.logger.Printf(kv.gid, kv.me, "ShardKV.applyConfig, cfg num: %d, %s, tag: %d",
			kv.config.Num, kv.shardInfoWithLock(), op.Tag)
	}

	migrateShards := make([]int, 0)
	for shard := range kv.own {
		if kv.config.Shards[shard] != kv.gid {
			kv.migrate[shard] = kv.own[shard]
			kv.migrateTriggerTag[shard] = op.TriggerTags[shard]
			migrateShards = append(migrateShards, shard)
		}
	}
	for _, shard := range migrateShards {
		delete(kv.own, shard)
	}
	if len(migrateShards) > 0 {
		go func() {
			kv.migrateCh <- false
		}()
	}
	kv.logger.Printf(kv.gid, kv.me, "ShardKV.applyConfig, cfg num: %d, %s, tag: %d",
		kv.config.Num, kv.shardInfoWithLock(), op.Tag)
	return resultMsg
}

func (kv *ShardKV) applyMigrateTrigger(op Op) Result {
	resultMsg := Result{
		Err: OK,
	}

	lablog.Assert2(kv.migrate[op.Shard] != nil, fmt.Sprintf("gid: %d, rid: %d", kv.gid, kv.me))

	kv.migrated[op.Shard] = kv.migrate[op.Shard]
	kv.migrateSendTag[op.Shard] = op.SendTag
	kv.migrateFinishTag[op.Shard] = op.FinishTag
	delete(kv.migrate, op.Shard)
	delete(kv.migrateTriggerTag, op.Shard)
	lablog.Assert(kv.migrated[op.Shard] != nil)
	if len(kv.migrated) > 0 {
		go func() {
			kv.migrateCh <- false
		}()
	}

	kv.logger.Printf(kv.gid, kv.me, "ShardKV.applyMigrateTrigger, cfg num: %d, shard: %d, tag: %d, " +
		"shard tag: %d, finish tag: %d, %s", kv.config.Num, op.Shard, op.Tag, op.SendTag, op.FinishTag, kv.shardInfoWithLock())
	return resultMsg
}

func (kv *ShardKV) applyMigrateReceive(op Op) Result {
	lablog.Assert(op.ConfigNum != 0)
	if kv.config.Num > op.ConfigNum {
		kv.needSync = true
	}
	resultMsg := Result{
		Err: OK,
	}

	lablog.Assert(op.KeyValues != nil)
	keyValues := make(map[string]string)
	for k, v := range op.KeyValues {
		keyValues[k] = v
	}
	kv.own[op.Shard] = keyValues
	if len(kv.migrate) > 0 {
		go func() {
			kv.migrateCh <- false
		}()
	}

	kv.logger.Printf(kv.gid, kv.me, "ShardKV.applyMigrateReceive, cfg num: %d, shard: %d, tag: %d, %s",
		kv.config.Num, op.Shard, op.Tag, kv.shardInfoWithLock())
	return resultMsg
}

func (kv *ShardKV) applyMigrateFinish(op Op) Result {
	resultMsg := Result{
		Err: OK,
	}
	delete(kv.migrated, op.Shard)
	delete(kv.migrateSendTag, op.Shard)
	delete(kv.migrateFinishTag, op.Shard)
	kv.logger.Printf(kv.gid, kv.me, "ShardKV.applyMigrateFinish, cfg num: %d, shard: %d, tag: %d, %s",
		kv.config.Num, op.Shard, op.Tag, kv.shardInfoWithLock())
	return resultMsg
}

func (kv *ShardKV) apply(applyMsg raft.ApplyMsg) {
	if !applyMsg.CommandValid {
		return
	}
	op, ok := applyMsg.Command.(Op)
	if !ok {
		return
	}

	kv.logger.Printf(kv.gid, kv.me, "ShardKV.apply start, op: %s, tag: %d", op.Op, op.Tag)
	defer kv.logger.Printf(kv.gid,kv.me, "ShardKV.apply end, op: %s, tag: %d", op.Op, op.Tag)

	// 检查是否已经处理过该请求.
	kv.mu.Lock()
	_, handled := kv.cache[op.Tag]
	kv.mu.Unlock()
	if handled {
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	resultMsg := Result{}
	switch op.Op {
	case "Get":
		resultMsg = kv.applyGet(op)
	case "Put":
		resultMsg = kv.applyPut(op)
	case "Append":
		resultMsg = kv.applyAppend(op)
	case "Config":
		resultMsg = kv.applyConfig(op)
	case "MigrateTrigger":
		resultMsg = kv.applyMigrateTrigger(op)
	case "MigrateReceive":
		resultMsg = kv.applyMigrateReceive(op)
	case "MigrateFinish":
		resultMsg = kv.applyMigrateFinish(op)
	default:
		panic(fmt.Sprintf("Unknown op: %s, me: %d", op.Op, kv.me))
	}

	if resultCh, ok := kv.pending[op.Tag]; ok {
		lablog.Assert(resultCh != nil)
		resultCh <- resultMsg
		close(resultCh)
		delete(kv.pending, op.Tag)
		kv.logger.Printf(kv.gid, kv.me, "ShardKV.apply, result: %v, op: %s, tag: %d", resultMsg, op.Op, op.Tag)
	}
	// 只对成功的请求进行cache.
	if resultMsg.Err == OK {
		kv.cache[op.Tag] = resultMsg
	}
}

func (kv *ShardKV) backgroundApply() {
	for {
		select {
		case <- kv.exitApplyCh:
			break
		case applyMsg := <- kv.applyCh:
			kv.apply(applyMsg)
		}
	}
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	op := Op{
		Key: args.Key,
		Op: "Get",
		Tag: args.Tag,
		PrevTag: args.PrevTag,
		ResultCh: make(chan Result, 1),
	}
	kv.requestCh <- op

	t := time.NewTimer(150*time.Millisecond)
	lablog.Assert(op.ResultCh != nil)
	select {
	case <- t.C:
		reply.Err = ErrTimeout
		kv.logger.Printf(kv.gid, kv.me, "ShardKV.Get timeout, key: %s, tag: %d, %s",
			args.Key, args.Tag, kv.shardInfo())
	case result :=<- op.ResultCh:
		reply.WrongLeader = result.WrongLeader
		reply.Value = result.Value
		reply.Err = result.Err
		kv.logger.Printf(kv.gid, kv.me, "ShardKV.Get, key: %s, result: %v, tag: %d, %s",
			args.Key, result, args.Tag, kv.shardInfo())
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	op := Op{
		Key: args.Key,
		Value: args.Value,
		Op: args.Op,
		Tag: args.Tag,
		PrevTag: args.PrevTag,
		ResultCh: make(chan Result, 1),
	}
	kv.requestCh <- op

	shard := key2shard(args.Key)
	lablog.Assert(op.ResultCh != nil)
	t := time.NewTimer(150*time.Millisecond)
	select {
	case <- t.C:
		reply.Err = ErrTimeout
		kv.logger.Printf(kv.gid, kv.me, "ShardKV.PutAppend timeout, key: %s, val: %s, shard: %d, tag: %d, %s",
			args.Key, args.Value, shard, args.Tag, kv.shardInfo())
	case result :=<- op.ResultCh:
		reply.WrongLeader = result.WrongLeader
		reply.Err = result.Err
		kv.logger.Printf(kv.gid, kv.me, "ShardKV.PutAppend, key: %s, val: %s, result: %v, shard: %d, tag: %d, %s",
			args.Key, args.Value, result, shard, op.Tag, kv.shardInfo())
	}
}

func (kv *ShardKV) MigrateShard(args *MigrateArgs, reply *MigrateReply) {
	op := Op{
		Op: "MigrateReceive",
		Shard: args.Shard,
		KeyValues: args.KeyValues,
		ConfigNum: args.ConfigNum,
		Tag: args.Tag,
		PrevTag: args.PrevTag,
		ResultCh: make(chan Result, 1),
	}

	kv.requestCh <- op

	lablog.Assert(op.KeyValues != nil)
	lablog.Assert(op.ResultCh != nil)
	t := time.NewTimer(150*time.Millisecond)
	select {
	case <- t.C:
		reply.Err = ErrTimeout
		kv.logger.Printf(kv.gid, kv.me,"ShardKV.MigrateShard timeout, shard: %d, tag: %d, %s",
			args.Shard, args.Tag, kv.shardInfo())
	case result :=<- op.ResultCh:
		reply.WrongLeader = result.WrongLeader
		reply.Err = result.Err
		kv.logger.Printf(kv.gid,kv.me, "ShardKV.MigrateShard, result: %v, shard: %d, tag: %d, %s",
			result, args.Shard, args.Tag, kv.shardInfo())
	}
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	// Your code here, if desired.
	kv.rf.Kill()
	kv.exitApplyCh <- false
	kv.exitRequestCh <- false
	kv.exitSyncCh <- false
	kv.logger.Printf(kv.gid, kv.me, "ShardKV.Kill")
}


//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.

	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.own = make(map[int]map[string]string)
	kv.migrate = make(map[int]map[string]string)
	kv.migrated = make(map[int]map[string]string)
	kv.migrateTriggerTag = make(map[int]int64)
	kv.migrateSendTag = make(map[int]int64)
	kv.migrateFinishTag = make(map[int]int64)
	kv.migrateCh = make(chan bool)
	kv.needSync = true

	kv.prefers = make(map[int]int)

	kv.pending = make(map[int64]chan Result)
	kv.cache = make(map[int64]Result)
	kv.exitApplyCh = make(chan bool)
	kv.exitRequestCh = make(chan bool)
	kv.exitSyncCh = make(chan bool)
	kv.requestCh = make(chan Op)
	kv.logger = lablog.New(true, "shardkv_server")
	go kv.syncConfig()
	go kv.backgroundApply()
	go kv.backgroundRequest()
	go kv.backgroundSync()

	kv.logger.Printf(kv.gid, kv.me,"StartServer")
	return kv
}
