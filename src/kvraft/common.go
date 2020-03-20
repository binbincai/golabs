package raftkv

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Tag int64
}

func NewPutAppendArgs(key, value, op string) *PutAppendArgs {
	return &PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
		Tag:   nrand(),
	}
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Tag int64
}

func NewGetArgs(key string) *GetArgs {
	return &GetArgs{
		Key: key,
		Tag: nrand(),
	}
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}
