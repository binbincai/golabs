package shardmaster

import (
	"github.com/binbincai/golabs/src/labgob"
	"github.com/binbincai/golabs/src/labrpc"
	"github.com/binbincai/golabs/src/raft"
	"log"
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
}

type Op struct {
	// Your data here.
	JoinArgs  *JoinArgs
	LeaveArgs *LeaveArgs
	MoveArgs  *MoveArgs
	QueryArgs *QueryArgs

	ResultMsgCh chan ResultMsg
}

type ResultMsg struct {
	WrongLeader bool
	Err         Err
	Config      Config

	Index int
	Term int
}


const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func (sm *ShardMaster) moveShards(config *Config) {
	GroupCnt := len(config.Groups)
	for shard := range config.Shards {
		config.Shards[shard] = shard%GroupCnt+1
	}
	configNum := len(sm.configs)
	DPrintf("Shards after movement: %v, num: %d, groups: %d", config.Shards, configNum, len(config.Groups))
}

func (sm *ShardMaster) apply(applyMsg raft.ApplyMsg)  {
	DPrintf("ShardMaster.apply start")
	defer DPrintf("ShardMaster.apply end")
	if !applyMsg.CommandValid {
		return
	}

	repl := ResultMsg{
		Index: applyMsg.CommandIndex,
		Term: applyMsg.CommandTerm,
	}

	nc := copyConfig(sm.configs[len(sm.configs)-1])
	op, ok := applyMsg.Command.(*Op)
	if !ok {
		return
	}

	if op.JoinArgs != nil {
		args := op.JoinArgs
		for gid, servers := range args.Servers {
			nc.Groups[gid] = servers
		}
		sm.moveShards(&nc)
	}

	if op.LeaveArgs != nil {
		DPrintf("Leave before: %v", nc.Groups)
		args := op.LeaveArgs
		for _, gid := range args.GIDs {
			delete(nc.Groups, gid)
		}
		sm.moveShards(&nc)
		DPrintf("Leave args: %v, %v", args, nc.Groups)
	}

	if op.MoveArgs != nil {
		args := op.MoveArgs
		nc.Shards[args.Shard] = args.GID
	}

	if op.QueryArgs != nil {
		args := op.QueryArgs
		if args.Num == -1 || args.Num > len(sm.configs) {
			configNum := len(sm.configs)
			repl.Config = copyConfig(sm.configs[configNum-1])
			DPrintf("Shards 2: %v, req num: %d, config num: %d", repl.Config.Shards, args.Num, configNum)
		} else {
			repl.Config = copyConfig(sm.configs[args.Num-1])
			DPrintf("Shards 1: %v, req num: %d, config num: %d", repl.Config.Shards, args.Num, len(sm.configs))
		}
	}

	sm.configs = append(sm.configs, nc)
	op.ResultMsgCh <- repl
	close(op.ResultMsgCh)
}

func (sm *ShardMaster) backgroundExec() {
	for {
		select {
		case <- sm.exitCh:
			break
		case applyMsg := <- sm.applyCh:
			sm.apply(applyMsg)
		}
	}
}

func (sm *ShardMaster) waitExec(op *Op) ResultMsg {
	resultMsg := ResultMsg{}
	op.ResultMsgCh = make(chan ResultMsg, 1)
	index, term, isLeader := sm.rf.Start(op)
	if !isLeader {
		resultMsg.Err = "Wrong leader"
		return resultMsg
	}
	resultMsg.Err = ""

	t := time.NewTimer(150*time.Millisecond)
	select {
	case <- t.C:
		resultMsg.Err = "Command execute timeout"
	case resultMsg = <- op.ResultMsgCh:
		t.Stop()
		if !(index == resultMsg.Index && term == resultMsg.Term) {
			resultMsg.Err = "Command collision happened"
		}
	}

	return resultMsg
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	DPrintf("ShardMaster.Join start")
	// Your code here.
	op := &Op{}
	op.JoinArgs = args
	resultMsg := sm.waitExec(op)
	reply.Err = resultMsg.Err
	DPrintf("ShardMaster.Join end, err: %v, me: %d, tag: %d", reply.Err, sm.me, args.Tag)
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	DPrintf("ShardMaster.Leave start")
	// Your code here.
	op := &Op{}
	op.LeaveArgs = args
	resultMsg := sm.waitExec(op)
	reply.Err = resultMsg.Err
	DPrintf("ShardMaster.Leave end, err: %v, me: %d, tag: %d", reply.Err, sm.me, args.Tag)
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	DPrintf("ShardMaster.Move start")
	// Your code here.
	op := &Op{}
	op.MoveArgs = args
	resultMsg := sm.waitExec(op)
	reply.Err = resultMsg.Err
	DPrintf("ShardMaster.Move end, err: %v, me: %d, tag: %d", reply.Err, sm.me, args.Tag)
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	DPrintf("ShardMaster.Query start")
	// Your code here.
	op := &Op{}
	op.QueryArgs = args
	resultMsg := sm.waitExec(op)
	reply.Err = resultMsg.Err
	reply.Config = resultMsg.Config
	DPrintf("ShardMaster.Query end, err: %v, me: %d, tag: %d", reply.Err, sm.me, args.Tag)
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

	labgob.Register(Op{})

	sm.applyCh = make(chan raft.ApplyMsg)
	sm.exitCh = make(chan bool)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	go sm.backgroundExec()

	return sm
}
