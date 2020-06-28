package shardmaster

import (
	"github.com/binbincai/golabs/src/labgob"
	"github.com/binbincai/golabs/src/lablog"
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

	logger *lablog.Logger
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
	if !applyMsg.CommandValid {
		return
	}

	op, ok := applyMsg.Command.(*Op)
	if !ok {
		return
	}
	sm.mu.Lock()
	_, handled := sm.cache[op.Tag]
	sm.mu.Unlock()
	if handled {
		return
	}

	sm.logger.Printf(0, sm.me, "ShardMaster.apply start, join: %v, leave: %v, query: %v",
		op.JoinArgs!=nil, op.LeaveArgs!=nil, op.QueryArgs!=nil)
	defer func() {
		sm.logger.Printf(0, sm.me, "ShardMaster.apply end, cfg cnt: %d", len(sm.configs))
		for i, config := range sm.configs {
			sm.logger.Printf(0, sm.me,"\tconfig %d: %v", i, config)
		}
	}()

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

	if repl.Err == "" {
		sm.cache[op.Tag] = repl
	}
}

func (sm *ShardMaster) request(op *Op) {
	sm.logger.Printf(0, sm.me, "ShardMaster.request start")
	defer sm.logger.Printf(0, sm.me, "ShardMaster.request end")

	sm.mu.Lock()
	result, handled := sm.cache[op.Tag]
	sm.mu.Unlock()
	if handled {
		op.ResultMsgCh <- result
		close(op.ResultMsgCh)
		return
	}

	if !sm.rf.IsLeader() {
		op.ResultMsgCh <- ResultMsg{
			Err: "Wrong leader",
		}
		return
	}
	sm.rf.Start(op)

	sm.mu.Lock()
	sm.pending[op.Tag] = op.ResultMsgCh
	sm.mu.Unlock()
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
	sm.logger.Printf(0, sm.me, "ShardMaster.Join start, tag: %d", args.Tag)
	defer sm.logger.Printf(0, sm.me, "ShardMaster.Join end, tag: %d", args.Tag)
	// Your code here.
	op := &Op{}
	op.JoinArgs = args
	op.Tag = args.Tag
	op.PrevTag = args.PrevTag
	resultMsg := sm.waitExec(op)
	reply.Err = resultMsg.Err
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	sm.logger.Printf(0, sm.me, "ShardMaster.Leave start, tag: %d", args.Tag)
	defer sm.logger.Printf(0, sm.me, "ShardMaster.Leave end, tag: %d", args.Tag)
	// Your code here.
	op := &Op{}
	op.LeaveArgs = args
	op.Tag = args.Tag
	op.PrevTag = args.PrevTag
	resultMsg := sm.waitExec(op)
	reply.Err = resultMsg.Err
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	sm.logger.Printf(0, sm.me, "ShardMaster.Move start, tag: %d", args.Tag)
	defer sm.logger.Printf(0, sm.me, "ShardMaster.Move end, tag: %d", args.Tag)
	// Your code here.
	op := &Op{}
	op.MoveArgs = args
	op.Tag = args.Tag
	op.PrevTag = args.PrevTag
	resultMsg := sm.waitExec(op)
	reply.Err = resultMsg.Err
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	sm.logger.Printf(0, sm.me, "ShardMaster.Query start, tag: %d", args.Tag)
	defer sm.logger.Printf(0, sm.me, "ShardMaster.Query end, tag: %d", args.Tag)
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
	sm.logger.Printf(0, 0, "ShardMaster.Kill")
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

	sm.logger = lablog.New(true, "shardmaster_server")
	go sm.background()

	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	sm.logger.Printf(0, sm.me, "StartServer")
	return sm
}
