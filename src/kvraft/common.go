package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrInternal    = "ErrInternal"
	ErrTimeout     = "ErrTimeout"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	OpType
	ReqId    int64
	Key      string
	Value    string
	ClientId int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	ReqId    int64
	Key      string
	ClientId int
}

type GetReply struct {
	Err   Err
	Value string
}
