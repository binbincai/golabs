package shardkv

import (
	"fmt"
	"github.com/binbincai/golabs/src/labgob"
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

	Key string
	Value string

	Config shardmaster.Config

	Shard int
	KeyValues map[string]string
	ShardTag int64

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
	own map[int]map[string]string
	migrate map[int]map[string]string
	migrated map[int]map[string]string
	migratedTag map[int]int64
	migrateTriggering bool
	migratedSending bool
	migrateCh chan bool
	prefers map[int]int

	pending   map[int64]chan Result
	cache     map[int64]Result
	exitApplyCh    chan bool
	exitRequestCh chan bool
	exitSyncCh chan bool
	requestCh chan Op

	config shardmaster.Config
	fetchingConfig bool
}

func (kv *ShardKV) requestUser(op Op) bool {
	shard := key2shard(op.Key)
	kv.mu.Lock()
	cGID := kv.config.Shards[shard]
	kv.mu.Unlock()
	if cGID != kv.gid {
		op.ResultCh <- Result{Err: ErrWrongGroup}
		DPrintf("ShardKV.request wrong group, me: %d, gid: %d", kv.me, kv.gid)
		return false
	}
	return true
}

func (kv *ShardKV) requestInner(op Op) bool {
	return true
}

func (kv *ShardKV) request(op Op) {
	DPrintf("ShardKV.request start, me: %d, gid: %d, op: %s, tag: %d",
		kv.me, kv.gid, op.Op, op.Tag)
	defer DPrintf("ShardKV.request end, me: %d, gid: %d, op: %s, tag: %d",
		kv.me, kv.gid, op.Op, op.Tag)
	kv.mu.Lock()
	result, cache := kv.cache[op.Tag]
	kv.mu.Unlock()
	DPrintf("ShardKV.request lock, me: %d, gid: %d, op: %s, tag: %d",
		kv.me, kv.gid, op.Op, op.Tag)
	if cache {
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

	isLeader := kv.rf.IsLeader()
	if !isLeader {
		op.ResultCh <- Result{WrongLeader: true}
		DPrintf("ShardKV.request wrong leader, me: %d, gid: %d, op: %s, tag: %d",
			kv.me, kv.gid, op.Op, op.Tag)
		return
	}

	kv.mu.Lock()
	kv.pending[op.Tag] = op.ResultCh
	kv.mu.Unlock()

	DPrintf("ShardKV.request before start raft, me: %d, gid: %d, op: %s, tag: %d",
		kv.me, kv.gid, op.Op, op.Tag)
	kv.rf.Start(op)
	DPrintf("ShardKV.request after start raft, me: %d, gid: %d, op: %s, tag: %d",
		kv.me, kv.gid, op.Op, op.Tag)
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
	configNum := 2
	if kv.config.Num != 0 {
		configNum = -1
	}
	// 只有leader节点需要同步配置.
	if !kv.rf.IsLeader() {
		return
	}

	// 同一个时刻, 只允许一个同步请求存在.
	kv.mu.Lock()
	fetchingConfig := kv.fetchingConfig
	defer func() {
		kv.fetchingConfig = fetchingConfig
		kv.mu.Unlock()
	}()

	if fetchingConfig {
		return
	}
	kv.fetchingConfig = true

	ck := shardmaster.MakeClerk(kv.masters)
	kv.mu.Unlock()
	config := ck.Query(configNum)
	kv.mu.Lock()
	if kv.config.Num == config.Num {
		return
	}

	DPrintf("ShardKV.syncConfig, me: %d, gid: %d, cfg num: %d, req cfg num: %d, ret cfg num: %d",
		kv.me, kv.gid, kv.config.Num, configNum, config.Num)

	op := Op{
		Op: "Config",
		Config: config,
		Tag: nrand(),
		ResultCh: make(chan Result),
	}
	func() {
		kv.mu.Unlock()
		defer kv.mu.Lock()
		kv.requestCh <- op

		t := time.NewTimer(time.Second)
		select {
		case <- t.C:
		case <- op.ResultCh:
			t.Stop()
		}
	}()
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
	for shard, keyValues := range kv.migrate {
		func () {
			DPrintf("ShardKV.migrateTrigger start, me: %d, gid: %d, shard: %d",
				kv.me, kv.gid, shard)
			kv.mu.Unlock()
			defer func() {
				defer kv.mu.Lock()
				DPrintf("ShardKV.migrateTrigger end, me: %d, gid: %d, shard: %d",
					kv.me, kv.gid, shard)
			}()

			op := Op{
				Op:        "MigrateTrigger",
				Shard:     shard,
				KeyValues: keyValues,
				Tag: nrand(),
				ShardTag: nrand(),
				ResultCh: make(chan Result),
			}
			kv.requestCh <- op
			t := time.NewTimer(100*time.Second)
			select {
			case <- t.C:
			case <- op.ResultCh:
				t.Stop()
			}
		}()
		break // 一次迁移一个shard的keyValue
	}
}

func (kv *ShardKV) sendToPeer(config shardmaster.Config, shard int, keyValues map[string]string) {
	kv.mu.Lock()
	preferIndex := kv.prefers[config.Shards[shard]]
	serverIndex := preferIndex
	tag := kv.migratedTag[shard]
	kv.mu.Unlock()
	args := &MigrateArgs{
		Shard: shard,
		KeyValues: keyValues,
		Tag: tag,
	}
	servers := config.Groups[config.Shards[shard]]
	OUT:
	for {
		for i:=0; i<len(servers); i++ {
			serverIndex = (preferIndex+i)%len(servers)
			server := kv.make_end(servers[serverIndex])
			reply := &MigrateReply{}
			ok := server.Call("ShardKV.MigrateShard", args, reply)
			if ok && reply.Err == OK {
				break OUT
			}
			DPrintf("ShardKV.sendToPeer send fail, to gid: %d, repl: %v, tag: %d",
				config.Shards[shard], *reply, tag)
		}
		time.Sleep(100*time.Millisecond)
	}
	kv.mu.Lock()
	kv.prefers[config.Shards[shard]] = serverIndex
	kv.mu.Unlock()
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
			DPrintf("ShardKV.migrateSend start, me: %d, gid: %d, shard: %d",
				kv.me, kv.gid, shard)
			config := kv.config
			kv.mu.Unlock()
			defer func() {
				kv.mu.Lock()
				DPrintf("ShardKV.migrateSend end, me: %d, gid: %d, shard: %d",
					kv.me, kv.gid, shard)
			}()

			DPrintf("ShardKV.migrateSend before send to peer, me: %d, gid: %d, shard: %d",
				kv.me, kv.gid, shard)
			kv.sendToPeer(config, shard, keyValues)
			DPrintf("ShardKV.migrateSend after send to peer, me: %d, gid: %d, shard: %d",
				kv.me, kv.gid, shard)

			op := Op {
				Op:        "MigrateFinish",
				Shard:     shard,
				Tag: nrand(),
				ResultCh: make(chan Result),
			}
			kv.requestCh <- op
			t := time.NewTimer(time.Second)
			select {
			case <- t.C:
			case <- op.ResultCh:
				t.Stop()
			}
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
		return keyValue
	}
	if keyValue, ok := kv.migrate[shard]; ok {
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
	return resultMsg
}

func (kv *ShardKV) applyConfig(op Op) Result {
	resultMsg := Result{
		Err: OK,
	}
	if op.Config.Num == kv.config.Num {
		return resultMsg
	}

	kv.config = op.Config
	if kv.config.Num == 2 {
		shards := make([]int, 0)
		for shard, gid := range kv.config.Shards {
			if gid != kv.gid {
				continue
			}
			kv.own[shard] = make(map[string]string)
			shards = append(shards, shard)
		}
		DPrintf("ShardKV.applyConfig, me: %d, gid: %d, cfg num: %d, initial own shards: %v",
			kv.me, kv.gid, kv.config.Num, shards)
	}

	migrateShards := make([]int, 0)
	for shard := range kv.own {
		if kv.config.Shards[shard] != kv.gid {
			kv.migrate[shard] = kv.own[shard]
			migrateShards = append(migrateShards, shard)
		}
	}
	for _, shard := range migrateShards {
		delete(kv.own, shard)
	}
	ownShards := make([]int, 0)
	for shard := range kv.own {
		ownShards = append(ownShards, shard)
	}
	DPrintf("ShardKV.applyConfig, me: %d, gid: %d, cfg num: %d, own shards: %v, migrate shards: %v",
		kv.me, kv.gid, kv.config.Num, ownShards, migrateShards)
	if len(migrateShards) > 0 {
		go func() {
			kv.migrateCh <- false
		}()
	}
	return resultMsg
}

func (kv *ShardKV) applyMigrateTrigger(op Op) Result {
	resultMsg := Result{
		Err: OK,
	}
	kv.migrated[op.Shard] = kv.migrate[op.Shard]
	kv.migratedTag[op.Shard] = op.ShardTag
	delete(kv.migrate, op.Shard)
	DPrintf("ShardKV.applyMigrateTrigger, me: %d, gid: %d, cfg num: %d, shard: %d, tag: %d",
		kv.me, kv.gid, kv.config.Num, op.Shard, op.ShardTag)
	if len(kv.migrated) > 0 {
		go func() {
			kv.migrateCh <- false
		}()
	}
	return resultMsg
}

func (kv *ShardKV) applyMigrateReceive(op Op) Result {
	resultMsg := Result{
		Err: OK,
	}
	if kv.config.Shards[op.Shard] == kv.gid {
		kv.own[op.Shard] = op.KeyValues
	} else {
		kv.migrate[op.Shard] = op.KeyValues
		go func() {
			kv.migrateCh <- false
		}()
	}

	ownShards := make([]int, 0)
	for shard := range kv.own {
		ownShards = append(ownShards, shard)
	}
	DPrintf("ShardKV.applyMigrateReceive, me: %d, cfg num: %d, shard: %d, tag: %d, own: %v",
		kv.me, kv.config.Num, op.Shard, op.Tag, ownShards)
	return resultMsg
}

func (kv *ShardKV) applyMigrateFinish(op Op) Result {
	resultMsg := Result{
		Err: OK,
	}
	delete(kv.migrated, op.Shard)
	delete(kv.migratedTag, op.Shard)
	DPrintf("ShardKV.applyMigrateFinish, me: %d, cfg num: %d, shard: %d",
		kv.me, kv.config.Num, op.Shard)
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

	DPrintf("ShardKV.apply start, me: %d, gid: %d, op: %s", kv.me, kv.gid, op.Op)
	defer DPrintf("ShardKV.apply end, me: %d, gid: %d, op: %s", kv.me, kv.gid, op.Op)

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
		resultCh <- resultMsg
		close(resultCh)
		delete(kv.pending, op.Tag)
		DPrintf("ShardKV.apply result: %v, op: %s, me: %d, gid: %d", resultMsg, op.Op, kv.me, kv.gid)
	}
	kv.cache[op.Tag] = resultMsg
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

	t := time.NewTimer(100*time.Millisecond)
	select {
	case <- t.C:
		reply.Err = ErrTimeout
	case result :=<- op.ResultCh:
		reply.WrongLeader = result.WrongLeader
		reply.Value = result.Value
		reply.Err = result.Err
		DPrintf("ShardKV.Get: %v, me: %d, gid: %d", result, kv.me, kv.gid)
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

	t := time.NewTimer(100*time.Millisecond)
	select {
	case <- t.C:
		reply.Err = ErrTimeout
	case result :=<- op.ResultCh:
		reply.WrongLeader = result.WrongLeader
		reply.Err = result.Err
		DPrintf("ShardKV.PutAppend: %v, me: %d, gid: %d", result, kv.me, kv.gid)
	}
}

func (kv *ShardKV) MigrateShard(args *MigrateArgs, reply *MigrateReply) {
	op := Op{
		Op: "MigrateReceive",
		Shard: args.Shard,
		KeyValues: args.KeyValues,
		Tag: args.Tag,
		PrevTag: args.PrevTag,
	}

	kv.requestCh <- op
	DPrintf("ShardKV.MigrateShard send to request queue, me: %d, gid: %d, tag: %d",
		kv.me, kv.gid, args.Tag)

	t := time.NewTimer(100*time.Millisecond)
	select {
	case <- t.C:
		reply.Err = ErrTimeout
		DPrintf("ShardKV.MigrateShard timeout, me: %d, gid: %d, tag: %d",
			kv.me, kv.gid, args.Tag)
	case result :=<- op.ResultCh:
		reply.WrongLeader = result.WrongLeader
		reply.Err = result.Err
		DPrintf("ShardKV.MigrateShard: %v, me: %d, gid: %d, tag: %d",
			result, kv.me, kv.gid, args.Tag)
	}
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	kv.exitApplyCh <- false
	kv.exitRequestCh <- false
	kv.exitSyncCh <- false
	DPrintf("ShardKV.Kill me: %d", kv.me)
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
	kv.migratedTag = make(map[int]int64)
	kv.migrateCh = make(chan bool)
	kv.prefers = make(map[int]int)

	kv.pending = make(map[int64]chan Result)
	kv.cache = make(map[int64]Result)
	kv.exitApplyCh = make(chan bool)
	kv.exitRequestCh = make(chan bool)
	kv.exitSyncCh = make(chan bool)
	kv.requestCh = make(chan Op)
	go kv.syncConfig()
	go kv.backgroundApply()
	go kv.backgroundRequest()
	go kv.backgroundSync()

	DPrintf("StartServer me: %d, gid: %d", kv.me, kv.gid)
	return kv
}
