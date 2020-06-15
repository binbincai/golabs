package raftkv

import (
	"crypto/rand"
	"github.com/binbincai/golabs/src/labrpc"
	"math/big"
	"time"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	prefer int
	//mu      sync.Mutex
	timeout time.Duration

	tag int64
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
	// You'll have to add code here.
	ck.timeout = time.Second
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	args := NewGetArgs(key)

	//ck.mu.Lock()
	prefer := ck.prefer
	//ck.mu.Unlock()

	for {
		for i := range ck.servers {
			reply := &GetReply{}

			t := time.NewTimer(ck.timeout)
			d := make(chan bool, 1)
			go func() {
				index := (prefer + i) % len(ck.servers)
				d <- ck.servers[index].Call("KVServer.Get", args, reply)
			}()

			select {
			case ok := <-d:
				t.Stop()
				if !ok {
					continue
				}
			case <-t.C:
				continue
			}

			if reply.WrongLeader {
				continue
			}
			if reply.Err != "" {
				continue
			}

			//ck.mu.Lock()
			ck.prefer = (prefer + i) % len(ck.servers)
			//ck.mu.Unlock()

			return reply.Value
		}
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := NewPutAppendArgs(key, value, op)
	args.PrevTag = ck.tag

	//ck.mu.Lock()
	prefer := ck.prefer
	//ck.mu.Unlock()

	for {
		for i := range ck.servers {
			reply := &PutAppendReply{}

			t := time.NewTimer(ck.timeout)
			d := make(chan bool, 1)
			go func() {
				index := (prefer + i) % len(ck.servers)
				d <- ck.servers[index].Call("KVServer.PutAppend", args, reply)
			}()

			select {
			case ok := <-d:
				t.Stop()
				if !ok {
					continue
				}
			case <-t.C:
				continue
			}

			if reply.WrongLeader {
				continue
			}
			if reply.Err != "" {
				continue
			}

			//ck.mu.Lock()
			ck.prefer = (prefer + i) % len(ck.servers)
			ck.tag = args.Tag
			//ck.mu.Unlock()
			return
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
