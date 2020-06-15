package raftkv

import (
	"bytes"
	"github.com/binbincai/golabs/src/labgob"
	"github.com/binbincai/golabs/src/labrpc"
	"log"
	"github.com/binbincai/golabs/src/raft"
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
	PrevTag       int64
}

func NewOp(key, value, operationType string, tag, prevTag int64) Op {
	return Op{key, value, operationType, tag, prevTag}
}

type ResultMsg struct {
	Value string
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
	keyValue   map[string]string // need persist
	tagApplied map[int64]bool    //
	pendingCh  chan PendingMsg   //
	appliedCh  chan AppliedMsg   //
	timeout    time.Duration     //
	lastIndex  int               // need persist
	lastTerm   int               // nned persist

	persister *raft.Persister
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	op := NewOp(args.Key, "", "Get", args.Tag, 0)
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
		reply.Value = rmsg.Value
	case <-t.C:
		reply.Err = "Get timeout"
	}

	if !kv.rf.IsLeader() {
		reply.WrongLeader = true
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	op := NewOp(args.Key, args.Value, args.Op, args.Tag, args.PrevTag)
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

	if !kv.rf.IsLeader() {
		reply.WrongLeader = true
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
	//DPrintf("Kill me: %d, kv: %v", kv.me, kv.keyValue)
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
	//kv.maxraftstate = maxraftstate
	kv.maxraftstate = 1

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.keyValue = make(map[string]string)
	kv.tagApplied = make(map[int64]bool)
	kv.pendingCh = make(chan PendingMsg)
	kv.appliedCh = make(chan AppliedMsg)
	kv.timeout = time.Second / 2
	kv.lastIndex = 0
	kv.lastTerm = 0

	kv.persister = persister
	kv.readPersist(persister.ReadSnapshot())

	go kv.backgroundApply()
	go kv.backgroundPending()
	go kv.backgroundSnapshot()

	DPrintf("StartKVServer me: %d", me)

	return kv
}

func (kv *KVServer) apply(msg raft.ApplyMsg) AppliedMsg {
	amsg := AppliedMsg{
		index: msg.CommandIndex,
	}

	if msg.CommandIndex != kv.lastIndex+1 {
		return amsg
	}

	op := msg.Command.(Op)
	amsg.op = op

	if op.OperationType == "Get" {
		if v, ok := kv.keyValue[op.Key]; ok {
			amsg.value = v
		}
	} else if !kv.tagApplied[op.Tag] {
		kv.tagApplied[op.Tag] = true

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
	}

	if op.PrevTag > 0 {
		delete(kv.tagApplied, op.PrevTag)
	}

	kv.lastIndex = msg.CommandIndex
	kv.lastTerm = msg.CommandTerm
	DPrintf("me: %d, key: %s, value: %s", kv.me, op.Key, kv.keyValue[op.Key])
	return amsg
}

func (kv *KVServer) readPersist(data []byte) {
	if len(data) == 0 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	d.Decode(&kv.keyValue)
	d.Decode(&kv.tagApplied)
	d.Decode(&kv.lastIndex)
	d.Decode(&kv.lastTerm)
}

func (kv *KVServer) applyInner(msg raft.ApplyMsg) {
	if len(msg.SnapshotData) != 0 {
		kv.readPersist(msg.SnapshotData)
	}
}

func (kv *KVServer) backgroundApply() {
	for {
		select {
		case msg := <-kv.applyCh:
			if msg.CommandValid {
				kv.mu.Lock()
				amsg := kv.apply(msg)
				kv.mu.Unlock()
				kv.appliedCh <- amsg
				DPrintf("Apply, me: %d, %v", kv.me, msg)
			} else {
				kv.mu.Lock()
				kv.applyInner(msg)
				kv.mu.Unlock()
				DPrintf("Apply inner, me: %d, %v", kv.me, msg)
			}
		}
	}
}

func (kv *KVServer) backgroundPending() {
	tagToPendingMsgs := make(map[int64][]PendingMsg)

	replyPendingMsgs := func(tag int64, rmsg ResultMsg) {
		for _, pmsg := range tagToPendingMsgs[tag] {
			pmsg.ch <- rmsg
		}
		tagToPendingMsgs[tag] = tagToPendingMsgs[tag][0:0]
	}

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
			if _, ok := tagToPendingMsgs[tag]; !ok {
				continue
			}
			replyPendingMsgs(tag, ResultMsg{amsg.value})
		}
	}
}

func (kv *KVServer) snapShotIfNeeded() {
	if kv.maxraftstate <= 0 {
		return
	}
	if kv.maxraftstate > kv.persister.RaftStateSize() {
		return
	}

	snapshot := func() (data []byte, index, term int) {
		kv.mu.Lock()
		lastIndex, lastTerm := kv.lastIndex, kv.lastTerm
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		e.Encode(kv.keyValue)
		e.Encode(kv.tagApplied)
		e.Encode(kv.lastIndex)
		e.Encode(kv.lastTerm)
		kv.mu.Unlock()
		return w.Bytes(), lastIndex, lastTerm
	}

	kv.rf.PersitWithSnapshot(snapshot)
	DPrintf("Snapshot, me: %d, maxsize: %d, currentsize: %d, last index: %d", kv.me, kv.maxraftstate, kv.persister.RaftStateSize(), kv.lastIndex)
}

func (kv *KVServer) backgroundSnapshot() {
	for {
		timeout := 10 * time.Second / 1000
		timer := time.NewTimer(timeout)
		select {
		case <-timer.C:
			kv.snapShotIfNeeded()
		}
	}
}
