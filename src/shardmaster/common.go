package shardmaster

//
// Master shard server: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

func copyConfig(config Config) Config {
	c := Config{
		Num: config.Num,
		Shards: config.Shards,
		Groups: make(map[int][]string),
	}
	for k, v := range config.Groups {
		c.Groups[k] = v
	}
	return c
}

const (
	OK = "OK"
)

type Err string

type JoinArgs struct {
	Servers map[int][]string // new GID -> servers mappings
	Tag int64
	PrevTag int64
}

type JoinReply struct {
	Err         Err
}

type LeaveArgs struct {
	GIDs []int
	Tag int64
	PrevTag int64
}

type LeaveReply struct {
	Err         Err
}

type MoveArgs struct {
	Shard int
	GID   int
	Tag int64
	PrevTag int64
}

type MoveReply struct {
	Err         Err
}

type QueryArgs struct {
	Num int // desired config number
	Tag int64
	PrevTag int64
}

type QueryReply struct {
	Err         Err
	Config      Config
}
