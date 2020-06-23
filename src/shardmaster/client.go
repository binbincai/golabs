package shardmaster

//
// Shardmaster clerk.
//

import (
	"context"
	"github.com/binbincai/golabs/src/lablog"
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
	prevTag int64
	logger *lablog.Logger
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
	ck.logger = lablog.New(true, "shardmaster_client")
	return ck
}

func (ck *Clerk) replicaCall(rpcCallback func(*labrpc.ClientEnd) bool) {
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
			if rpcCallback(ck.servers[serverIndex]) {
				return
			}
		}
		time.Sleep(100*time.Millisecond)
	}
}

func (ck *Clerk) getPrevTag() int64 {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	return ck.prevTag
}

func (ck *Clerk) setPrevTag(tag int64) {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	ck.prevTag = tag
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{
		Num: num,
		Tag: nrand(),
		PrevTag: ck.getPrevTag(),
	}
	repl := &QueryReply{}
	ck.logger.Printf(0, 0, "Clerk.Query, num: %d, tag: %d", num, args.Tag)
	defer ck.logger.Printf(0, 0, "Clerk.Query end, num: %d, tag: %d", num, args.Tag)
	ck.replicaCall(func(server *labrpc.ClientEnd) bool {
		repl = &QueryReply{}
		ok := server.Call("ShardMaster.Query", args, repl)
		if ok && repl.Err == "" {
			ck.logger.Printf(0, 0, "Clerk.Query succ, num: %d, tag: %d, repl: %v",
				num, args.Tag, *repl)
			return true
		}
		ck.logger.Printf(0, 0, "Clerk.Query fail, num: %d, tag: %d, repl: %v",
			num, args.Tag, *repl)
		return false
	})
	ck.setPrevTag(args.Tag)
	return repl.Config
}

func (ck *Clerk) Query2(ctx context.Context, num int) Config {
	args := &QueryArgs{
		Num: num,
		Tag: nrand(),
		PrevTag: ck.getPrevTag(),
	}

	ck.logger.Printf(0, 0, "Clerk.Query, num: %d, tag: %d", num, args.Tag)
	defer ck.logger.Printf(0, 0, "Clerk.Query end, num: %d, tag: %d", num, args.Tag)

	wait := make(chan QueryReply)
	go ck.replicaCall(func(server *labrpc.ClientEnd) bool {
		repl := &QueryReply{}
		done := make(chan bool)
		go func() {
			ok := server.Call("ShardMaster.Query", args, repl)
			if ok && repl.Err == "" {
				ck.logger.Printf(0, 0, "Clerk.Query succ, num: %d, tag: %d, repl: %v", num, args.Tag, *repl)
				done <- true
				return
			}
			ck.logger.Printf(0, 0, "Clerk.Query fail, num: %d, tag: %d, repl: %v", num, args.Tag, *repl)
			done <- false
		}()

		var succ bool
		select {
		case <- ctx.Done():
			wait <- QueryReply{}
			succ = true
		case succ = <- done:
			if succ {
				wait <- *repl
			}
		}
		return succ
	})

	ck.setPrevTag(args.Tag)
	repl := <- wait
	return repl.Config
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{
		Servers: servers,
		Tag: nrand(),
		PrevTag: ck.getPrevTag(),
	}
	repl := &JoinReply{}
	// Your code here.
	ck.logger.Printf(0, 0, "Clerk.Join, servers: %v, tag: %d", servers, args.Tag)
	defer ck.logger.Printf(0, 0, "Clerk.Join end, servers: %v, tag: %d", servers, args.Tag)
	ck.replicaCall(func(server *labrpc.ClientEnd) bool {
		repl = &JoinReply{}
		ok := server.Call("ShardMaster.Join", args, repl)
		if ok && repl.Err == "" {
			ck.logger.Printf(0, 0, "Clerk.Join succ, servers: %v, tag: %d, repl: %v",
				servers, args.Tag, *repl)
			return true
		}
		ck.logger.Printf(0, 0, "Clerk.Join fail, servers: %v, tag: %d, repl: %v",
			servers, args.Tag, *repl)
		return false
	})
	ck.setPrevTag(args.Tag)
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{
		GIDs: gids,
		Tag: nrand(),
		PrevTag: ck.getPrevTag(),
	}
	repl := &LeaveReply{}
	ck.logger.Printf(0, 0, "Clerk.Join, gids: %v, tag: %d", gids, args.Tag)
	defer ck.logger.Printf(0, 0, "Clerk.Join end, gids: %v, tag: %d", gids, args.Tag)
	ck.replicaCall(func(server *labrpc.ClientEnd) bool {
		repl = &LeaveReply{}
		ok := server.Call("ShardMaster.Leave", args, &repl)
		if ok && repl.Err == "" {
			ck.logger.Printf(0, 0, "Clerk.Join succ, gids: %v, tag: %d, repl: %v",
				gids, args.Tag, *repl)
			return true
		}
		ck.logger.Printf(0, 0, "Clerk.Join fail, gids: %v, tag: %d, repl: %v",
			gids, args.Tag, *repl)
		return false
	})
	ck.setPrevTag(args.Tag)
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{
		Shard: shard,
		GID: gid,
		Tag: nrand(),
		PrevTag: ck.getPrevTag(),
	}
	repl := &MoveReply{}
	ck.logger.Printf(0, 0, "Clerk.Move, shard: %d, gid: %v, tag: %d", shard, gid, args.Tag)
	defer ck.logger.Printf(0, 0, "Clerk.Move end, shard: %d, gid: %v, tag: %d", shard, gid, args.Tag)
	ck.replicaCall(func(server *labrpc.ClientEnd) bool {
		repl = &MoveReply{}
		ok := server.Call("ShardMaster.Move", args, &repl)
		if ok && repl.Err == "" {
			ck.logger.Printf(0, 0, "Clerk.Move succ, shard: %d, gid: %v, tag: %d, repl: %v",
				shard, gid, args.Tag, *repl)
			return true
		}
		ck.logger.Printf(0, 0, "Clerk.Move fail, shard: %d, gid: %v, tag: %d, repl: %v",
			shard, gid, args.Tag, *repl)
		return false
	})
	ck.setPrevTag(args.Tag)
}
