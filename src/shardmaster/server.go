package shardmaster

import (
	"github.com/binbincai/golabs/src/labgob"
	"github.com/binbincai/golabs/src/labrpc"
	"github.com/binbincai/golabs/src/raft"
	"sync"
	"time"
)

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	configs []Config
	raft *raft.Raft
	exitCh chan bool
	requestCh chan *Op
	pending map[int64]chan ResultMsg
	cache map[int64]ResultMsg
}

type Op struct {
	// Your data here.
	JoinArgs  *JoinArgs
	LeaveArgs *LeaveArgs
	MoveArgs  *MoveArgs
	QueryArgs *QueryArgs
	Tag int64
	PrevTag int64

	ResultMsgCh chan ResultMsg
}

type ResultMsg struct {
	Err         Err
	Config      Config

	Index int
	Term int
}

func (sm *ShardMaster) moveShards(config *Config) {
	// TODO: 目前使用简单的取余方法分shard.
	groups := make([]int, 0)
	for group, _ := range config.Groups {
		groups = append(groups, group)
	}
	groupCnt := len(groups)
	if groupCnt == 0 {
		for shard := range config.Shards {
			config.Shards[shard] = 0
		}
		return
	}
	for shard := range config.Shards {
		config.Shards[shard] = groups[shard%groupCnt]
	}
}

func (sm *ShardMaster) apply(applyMsg raft.ApplyMsg)  {
	DPrintf("ShardMaster.apply start")
	defer DPrintf("ShardMaster.apply end")
	if !applyMsg.CommandValid {
		return
	}

	op, ok := applyMsg.Command.(*Op)
	if !ok {
		return
	}
	if _, ok := sm.cache[op.Tag]; ok {
		return
	}

	repl := ResultMsg{
		Index: applyMsg.CommandIndex,
		Term: applyMsg.CommandTerm,
	}
	nc := copyConfig(sm.configs[len(sm.configs)-1])

	if op.JoinArgs != nil {
		args := op.JoinArgs
		for gid, servers := range args.Servers {
			nc.Groups[gid] = servers
		}
		sm.moveShards(&nc)
		nc.Num = len(sm.configs)+1
		sm.configs = append(sm.configs, nc)
	}

	if op.LeaveArgs != nil {
		args := op.LeaveArgs
		for _, gid := range args.GIDs {
			delete(nc.Groups, gid)
		}
		sm.moveShards(&nc)
		nc.Num = len(sm.configs)+1
		sm.configs = append(sm.configs, nc)
	}

	if op.MoveArgs != nil {
		args := op.MoveArgs
		nc.Shards[args.Shard] = args.GID
		nc.Num = len(sm.configs)+1
		sm.configs = append(sm.configs, nc)
	}

	if op.QueryArgs != nil {
		args := op.QueryArgs
		if args.Num == -1 || args.Num > len(sm.configs) {
			repl.Config = copyConfig(sm.configs[len(sm.configs)-1])
		} else if args.Num == 0 {
			repl.Config = Config{}
		} else {
			repl.Config = copyConfig(sm.configs[args.Num-1])
		}
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()
	if resultMsgCh, ok := sm.pending[op.Tag]; ok {
		resultMsgCh <- repl
		close(resultMsgCh)
		delete(sm.pending, op.Tag)
	}
}

func (sm *ShardMaster) request(op *Op) {
	DPrintf("ShardMaster.request start")
	defer DPrintf("ShardMaster.request end")
	sm.mu.Lock()
	if repl, ok := sm.cache[op.Tag]; ok {
		op.ResultMsgCh <- repl
		return
	}
	delete(sm.cache, op.PrevTag)
	sm.mu.Unlock()

	_, _, isLeader := sm.rf.Start(op)
	if !isLeader {
		op.ResultMsgCh <- ResultMsg{
			Err: "Wrong leader",
		}
		return
	}
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.pending[op.Tag] = op.ResultMsgCh
}

func (sm *ShardMaster) background() {
	for {
		select {
		case <- sm.exitCh:
			break
		case applyMsg := <- sm.applyCh:
			sm.apply(applyMsg)
		case requestOp := <- sm.requestCh:
			go sm.request(requestOp)
		}
	}
}

func (sm *ShardMaster) waitExec(op *Op) (resultMsg ResultMsg) {
	op.ResultMsgCh = make(chan ResultMsg, 1)
	sm.requestCh <- op

	t := time.NewTimer(150*time.Millisecond)
	select {
	case <-t.C:
		resultMsg.Err = "Command execute timeout"
	case resultMsg = <- op.ResultMsgCh:
		t.Stop()
	}

	return
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	op := &Op{}
	op.JoinArgs = args
	op.Tag = args.Tag
	op.PrevTag = args.PrevTag
	resultMsg := sm.waitExec(op)
	reply.Err = resultMsg.Err
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	op := &Op{}
	op.LeaveArgs = args
	op.Tag = args.Tag
	op.PrevTag = args.PrevTag
	resultMsg := sm.waitExec(op)
	reply.Err = resultMsg.Err
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	op := &Op{}
	op.MoveArgs = args
	op.Tag = args.Tag
	op.PrevTag = args.PrevTag
	resultMsg := sm.waitExec(op)
	reply.Err = resultMsg.Err
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	op := &Op{}
	op.QueryArgs = args
	op.Tag = args.Tag
	op.PrevTag = args.PrevTag
	resultMsg := sm.waitExec(op)
	reply.Err = resultMsg.Err
	reply.Config = resultMsg.Config
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	// Your code here, if desired.
	sm.exitCh <- false
	sm.rf.Kill()
	DPrintf("ShardMaster.Kill")
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(&Op{})

	sm.applyCh = make(chan raft.ApplyMsg)
	sm.exitCh = make(chan bool)
	sm.requestCh = make(chan *Op)
	sm.pending = make(map[int64]chan ResultMsg)
	sm.cache = make(map[int64]ResultMsg)
	go sm.background()

	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	DPrintf("StartServer")
	return sm
}
