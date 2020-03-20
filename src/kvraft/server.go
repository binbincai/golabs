package raftkv

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	Key           string
	Value         string
	OperationType string
	Tag           int64
}

func NewOp(key, value, operationType string, tag int64) Op {
	return Op{key, value, operationType, tag}
}

type ResultMsg struct {
	value string
}

type PendingMsg struct {
	ch    chan ResultMsg
	index int
	term  int
	op    Op
}

type AppliedMsg struct {
	index int
	op    Op
	value string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	keyValue    map[string]string // need persist
	pendingCh   chan PendingMsg
	appliedCh   chan AppliedMsg
	tagToResult map[int64]ResultMsg // need persit
	timeout     time.Duration
	tagApplied  map[int64]bool // need persist
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	if rmsg, ok := kv.tagToResult[args.Tag]; ok {
		reply.Value = rmsg.value
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	op := NewOp(args.Key, "", "Get", args.Tag)
	index, term, isleader := kv.rf.Start(op)
	if !isleader {
		reply.WrongLeader = true
		return
	}
	pmsg := PendingMsg{}
	pmsg.ch = make(chan ResultMsg, 1)
	pmsg.index = index
	pmsg.term = term
	pmsg.op = op
	kv.pendingCh <- pmsg

	t := time.NewTimer(kv.timeout)
	defer t.Stop()

	select {
	case rmsg := <-pmsg.ch:
		reply.Value = rmsg.value
	case <-t.C:
		reply.Err = "Get timeout"
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	if _, ok := kv.tagToResult[args.Tag]; ok {
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	op := NewOp(args.Key, args.Value, args.Op, args.Tag)
	index, term, isleader := kv.rf.Start(op)
	if !isleader {
		reply.WrongLeader = true
		return
	}
	pmsg := PendingMsg{}
	pmsg.ch = make(chan ResultMsg, 1)
	pmsg.index = index
	pmsg.term = term
	pmsg.op = op
	kv.pendingCh <- pmsg

	t := time.NewTimer(kv.timeout)
	defer t.Stop()

	select {
	case <-pmsg.ch:

	case <-t.C:
		reply.Err = "PutAppend timeout"
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	DPrintf("Kill me: %d, kv: %v", kv.me, kv.keyValue)
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.keyValue = make(map[string]string)
	kv.pendingCh = make(chan PendingMsg)
	kv.appliedCh = make(chan AppliedMsg)
	kv.tagApplied = make(map[int64]bool)
	kv.tagToResult = make(map[int64]ResultMsg)

	kv.timeout = time.Second

	go kv.applyBackground()
	go kv.pendingBackground()

	DPrintf("StartKVServer me: %d, keyvalue:%v", me, kv.keyValue)

	return kv
}

func (kv *KVServer) apply(msg raft.ApplyMsg) AppliedMsg {
	amsg := AppliedMsg{
		index: msg.CommandIndex,
	}
	if !msg.CommandValid {
		return amsg
	}
	op := msg.Command.(Op)
	if kv.tagApplied[op.Tag] {
		return amsg
	}
	kv.tagApplied[op.Tag] = true

	amsg.op = op

	if op.OperationType == "Get" {
		if v, ok := kv.keyValue[op.Key]; ok {
			amsg.value = v
		}
	}

	if op.OperationType == "Put" {
		kv.keyValue[op.Key] = op.Value
	}

	if op.OperationType == "Append" {
		if v, ok := kv.keyValue[op.Key]; ok {
			kv.keyValue[op.Key] = string(append([]byte(v), op.Value...))
		} else {
			kv.keyValue[op.Key] = op.Value
		}
	}
	return amsg
}

func (kv *KVServer) applyBackground() {
	for {
		select {
		case msg := <-kv.applyCh:
			kv.appliedCh <- kv.apply(msg)
		}
	}
}

func (kv *KVServer) pendingBackground() {
	tagToPendingMsgs := make(map[int64][]PendingMsg)
	for {
		select {
		case pmsg := <-kv.pendingCh:
			tag := pmsg.op.Tag
			if _, ok := tagToPendingMsgs[tag]; !ok {
				tagToPendingMsgs[tag] = make([]PendingMsg, 0)
			}
			tagToPendingMsgs[tag] = append(tagToPendingMsgs[tag], pmsg)
		case amsg := <-kv.appliedCh:
			tag := amsg.op.Tag
			value := amsg.value
			if _, ok := tagToPendingMsgs[tag]; !ok {
				continue
			}

			result := ResultMsg{value: value}
			for _, pmsg := range tagToPendingMsgs[tag] {
				pmsg.ch <- result
			}
			tagToPendingMsgs[tag] = tagToPendingMsgs[tag][0:0]

			kv.mu.Lock()
			kv.tagToResult[tag] = result
			kv.mu.Unlock()
		}
	}
}
