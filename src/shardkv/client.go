package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardmaster to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"github.com/binbincai/golabs/src/lablog"
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

	logger *lablog.Logger
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
	ck.logger = lablog.New(true, "shardkv_client")
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
	args := GetArgs{}
	args.Key = key
	args.Tag = nrand()
	args.PrevTag = ck.getPrevTag()

	ck.logger.Printf(0,0, "Clerk.Get start, key: %s, tag: %d", args.Key, args.Tag)
	defer ck.logger.Printf(0, 0,"Clerk.Get end, key: %s, tag: %d", args.Key, args.Tag)

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
				ck.logger.Printf(0,0, "Clerk.Get call server: (%d,%d), tag: %d", gid, server, args.Tag)
				ok := srv.Call("ShardKV.Get", &args, &reply)
				if ok && reply.WrongLeader == false && (reply.Err == OK || reply.Err == ErrNoKey) {
					ck.mu.Lock()
					ck.prefers[gid] = server
					ck.mu.Unlock()
					ck.logger.Printf(0,0, "Clerk.Get call succ, server: (%d,%d), tag: %d, reply: %v",
						gid, server, args.Tag, reply)
					return reply.Value
				}
				ck.logger.Printf(0,0, "Clerk.Get call fail, server: (%d,%d), tag: %d, reply: %v",
					gid, server, args.Tag, reply)
				if ok && (reply.Err == ErrWrongGroup) {
					break
				}
			}
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
	args := PutAppendArgs{}
	args.Key = key
	args.Value = value
	args.Op = op
	args.Tag = nrand()
	args.PrevTag = ck.getPrevTag()

	ck.logger.Printf(0, 0, "Clerk.PutAppend start, op: %s, key: %s, value: %s, tag: %d",
		args.Op, args.Key, args.Value, args.Tag)
	defer ck.logger.Printf(0, 0, "Clerk.PutAppend end, op: %s, key: %s, value: %s, tag: %d",
		args.Op, args.Key, args.Value, args.Tag)

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
				ck.logger.Printf(0, 0, "Clerk.PutAppend call server: (%d, %d), shard: %d, tag: %d",
					gid, server, shard, args.Tag)
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				if ok && reply.WrongLeader == false && reply.Err == OK {
					ck.mu.Lock()
					ck.prefers[gid] = server
					ck.mu.Unlock()
					ck.logger.Printf(0, 0, "Clerk.PutAppend call succ, server: (%d, %d), reply: %v, shard: %d, tag: %d",
						gid, server, reply, shard, args.Tag)
					return
				}
				ck.logger.Printf(0, 0, "Clerk.PutAppend call fail, server: (%d, %d), reply: %v, shard: %d, tag: %d",
					gid, server, reply, shard, gid)
				if ok && reply.Err == ErrWrongGroup {
					break
				}
			}
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
