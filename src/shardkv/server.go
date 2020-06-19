package shardkv


import (
	"github.com/binbincai/golabs/src/labrpc"
	"github.com/binbincai/golabs/src/raft"
	"github.com/binbincai/golabs/src/labgob"
	"github.com/binbincai/golabs/src/shardmaster"

	"time"
	"sync"
)



type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key string
	Value string
	Op string
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
	keyValue map[string]string
	pending map[int64]chan Result
	cache map[int64]Result
	exitCh chan bool
	requestCh chan Op

	config shardmaster.Config
}

func (kv *ShardKV) apply(applyMsg raft.ApplyMsg) {
	DPrintf("ShardKV.apply start, me: %d", kv.me)
	defer DPrintf("ShardKV.apply end, me: %d", kv.me)
	if !applyMsg.CommandValid {
		return
	}

	op, ok := applyMsg.Command.(Op)
	if !ok {
		return
	}

	shard := key2shard(op.Key)
	if kv.config.Shards[shard] != kv.gid {
		return
	}

	handled := func() bool {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		_, ok := kv.cache[op.Tag]
		return ok
	}()
	if handled {
		return
	}

	resultMsg := Result{
		Err: OK,
	}

	if op.Op == "" {
		if v, ok := kv.keyValue[op.Key]; ok {
			resultMsg.Value = v
		}
	}
	if op.Op == "Put" {
		kv.keyValue[op.Key] = op.Value
	}
	if op.Op == "Append" {
		if _, ok := kv.keyValue[op.Key]; !ok {
			kv.keyValue[op.Key] = ""
		}
		kv.keyValue[op.Key] += op.Value
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	if resultCh, ok := kv.pending[op.Tag]; ok {
		resultCh <- resultMsg
		close(resultCh)
		delete(kv.pending, op.Tag)
		DPrintf("ShardKV.apply result: %v, me: %d", resultMsg, kv.me)
	}
	kv.cache[op.Tag] = resultMsg
}

func (kv *ShardKV) request(op Op) {
	DPrintf("ShardKV.request start, me: %d", kv.me)
	defer DPrintf("ShardKV.request end, me: %d", kv.me)
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		op.ResultCh <- Result{WrongLeader: true}
		DPrintf("ShardKV.request wrong leader, me: %d", kv.me)
		return
	}

	shard := key2shard(op.Key)
	kv.mu.Lock()
	cGID := kv.config.Shards[shard]
	kv.mu.Unlock()
	if cGID != kv.gid {
		op.ResultCh <- Result{Err: ErrWrongGroup}
		DPrintf("ShardKV.request wrong group, me: %d, gid: %d", kv.me, kv.gid)
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	if result, ok := kv.cache[op.Tag]; ok {
		op.ResultCh <- result
		close(op.ResultCh)
		return
	}
	kv.pending[op.Tag] = op.ResultCh
	// TODO: 清理cache
}

func (kv *ShardKV) fetchConfig() {
	ck := shardmaster.MakeClerk(kv.masters)
	config := ck.Query(-1)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.config.Num == config.Num {
		return
	}
	kv.config = config
}

func (kv *ShardKV) background() {
	go kv.fetchConfig()
	var t *time.Timer
	for {
		if t != nil {
			t.Stop()
		}
		t = time.NewTimer(100*time.Millisecond)
		select {
		case <- kv.exitCh:
			break
		case applyMsg := <- kv.applyCh:
			kv.apply(applyMsg)
		case requestMsg := <- kv.requestCh:
			go kv.request(requestMsg)
		case <- t.C:
			go kv.fetchConfig()
		}
	}
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	op := Op{
		Key: args.Key,
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
		DPrintf("ShardKV.Get: %v, me: %d", result, kv.me)
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
		DPrintf("ShardKV.PutAppend: %v, me: %d", result, kv.me)
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
	kv.exitCh <- false
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

	kv.keyValue = make(map[string]string)
	kv.pending = make(map[int64]chan Result)
	kv.cache = make(map[int64]Result)
	kv.exitCh = make(chan bool)
	kv.requestCh = make(chan Op)
	go kv.background()

	DPrintf("StartServer me: %d", kv.me)
	return kv
}
