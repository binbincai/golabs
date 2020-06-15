package shardmaster

//
// Shardmaster clerk.
//

import (
	"github.com/binbincai/golabs/src/labrpc"
	"sync"
)
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	mu sync.Mutex
	preferIndex int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	return ck
}

func (ck *Clerk) replicaCall(rpcCallback func(*labrpc.ClientEnd) bool) {
	DPrintf("Clerk.replicaCall start")
	defer DPrintf("Clerk.replicaCall end")
	ck.mu.Lock()
	preferIndex := ck.preferIndex
	serverIndex := preferIndex
	ck.mu.Unlock()
	defer func() {
		ck.mu.Lock()
		defer ck.mu.Unlock()
		ck.preferIndex = serverIndex
	}()

	for {
		for offset:=0; offset<len(ck.servers); offset++ {
			serverIndex = (preferIndex+offset)%len(ck.servers)
			DPrintf("Clerk.replicaCall server index: %d ", serverIndex)
			if rpcCallback(ck.servers[serverIndex]) {
				return
			}
		}
		time.Sleep(100*time.Millisecond)
	}
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{
		Num: num,
		Tag: nrand(),
	}
	repl := &QueryReply{}
	ck.replicaCall(func(server *labrpc.ClientEnd) bool {
		repl = &QueryReply{}
		ok := server.Call("ShardMaster.Query", args, repl)
		DPrintf("Clerk.Query, ok: %v, err: %v, tag: %d", ok, repl.Err, args.Tag)
		return ok && repl.Err == ""
	})
	return repl.Config
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{
		Servers: servers,
		Tag: nrand(),
	}
	repl := &JoinReply{}
	// Your code here.
	ck.replicaCall(func(server *labrpc.ClientEnd) bool {
		repl = &JoinReply{}
		ok := server.Call("ShardMaster.Join", args, repl)
		DPrintf("Clerk.Join, err: %v, tag: %d", repl.Err, args.Tag)
		//return ok && repl.Err == false
		return ok && repl.Err == ""
	})
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{
		GIDs: gids,
		Tag: nrand(),
	}
	repl := &LeaveReply{}
	ck.replicaCall(func(server *labrpc.ClientEnd) bool {
		repl = &LeaveReply{}
		ok := server.Call("ShardMaster.Leave", args, &repl)
		DPrintf("Clerk.Join, err: %v, tag: %d", repl.Err, args.Tag)
		//return ok && repl.WrongLeader == false
		return ok && repl.Err == ""
	})
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{
		Shard: shard,
		GID: gid,
		Tag: nrand(),
	}
	repl := &MoveReply{}
	ck.replicaCall(func(server *labrpc.ClientEnd) bool {
		repl = &MoveReply{}
		ok := server.Call("ShardMaster.Move", args, &repl)
		DPrintf("Clerk.Move, ok: %v, err: %v, tag: %d", ok, repl.Err, args.Tag)
		//return ok && repl.WrongLeader == false
		return ok && repl.Err == ""
	})
}
