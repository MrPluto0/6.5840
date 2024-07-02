package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrTimeout     = "ErrTimeout"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

type ErrStruct struct {
	Err Err
}

// Put or Append
type PutAppendArgs struct {
	Id    int64
	Seq   int64
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	ErrStruct
}

type GetArgs struct {
	Id  int64
	Seq int64
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	ErrStruct
	Value string
}
