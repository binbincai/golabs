package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardmaster to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"github.com/binbincai/golabs/src/labrpc"
	"sync"
)
import "crypto/rand"
import "math/big"
import "github.com/binbincai/golabs/src/shardmaster"
import "time"

//
// which shard is a key in?
// please use this function,
// and please do not change it.
//
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardmaster.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardmaster.Clerk
	config   shardmaster.Config
	make_end func(string) *labrpc.ClientEnd

	// You will have to modify this struct.
	mu sync.Mutex
	prevTag int64
	prefers map[int]int
}

//
// the tester calls MakeClerk.
//
// masters[] is needed to call shardmaster.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
//
func MakeClerk(masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardmaster.MakeClerk(masters)
	ck.make_end = make_end
	// You'll have to add code here.
	ck.prefers = make(map[int]int)
	return ck
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

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
//
func (ck *Clerk) Get(key string) string {
	DPrintf("Clerk.Get start")
	defer DPrintf("Clerk.Get end")
	args := GetArgs{}
	args.Key = key
	args.Tag = nrand()
	args.PrevTag = ck.getPrevTag()

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			ck.mu.Lock()
			prefer := ck.prefers[gid]
			server := prefer
			ck.mu.Unlock()
			for si := 0; si < len(servers); si++ {
				server = (prefer+si)%len(servers)
				srv := ck.make_end(servers[server])
				var reply GetReply
				DPrintf("Clerk.Get call server %d of total %d", server, len(servers))
				ok := srv.Call("ShardKV.Get", &args, &reply)
				if ok && reply.WrongLeader == false && (reply.Err == OK || reply.Err == ErrNoKey) {
					return reply.Value
				}
				if ok && (reply.Err == ErrWrongGroup) {
					break
				}
			}
			ck.mu.Lock()
			ck.prefers[gid] = server
			ck.mu.Unlock()
		}
		time.Sleep(100 * time.Millisecond)
		// ask master for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}

	ck.setPrevTag(args.Tag)
	return ""
}

//
// shared by Put and Append.
// You will have to modify this function.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	DPrintf("Clerk.PutAppend start")
	defer DPrintf("Clerk.PutAppend end")
	args := PutAppendArgs{}
	args.Key = key
	args.Value = value
	args.Op = op
	args.Tag = nrand()
	args.PrevTag = ck.getPrevTag()

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			ck.mu.Lock()
			prefer := ck.prefers[gid]
			server := prefer
			ck.mu.Unlock()
			for si := 0; si < len(servers); si++ {
				server = (prefer+si)%len(servers)
				srv := ck.make_end(servers[server])
				var reply PutAppendReply
				DPrintf("Clerk.PutAppend call server %d of total %d", server, len(servers))
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				if ok && reply.WrongLeader == false && reply.Err == OK {
					return
				}
				if ok && reply.Err == ErrWrongGroup {
					break
				}
			}
			ck.mu.Lock()
			ck.prefers[gid] = server
			ck.mu.Unlock()
		}
		time.Sleep(100 * time.Millisecond)
		// ask master for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}

	ck.setPrevTag(args.Tag)
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
