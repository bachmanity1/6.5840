package shardkv

import "6.5840/labgob"

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

func init() {
	labgob.Register(GetArgs{})
	labgob.Register(PutAppendArgs{})
	labgob.Register(RequestShardArgs{})
	labgob.Register(AddShard{})
	labgob.Register(RemoveShard{})
	labgob.Register(UpdateConfig{})
}

const (
	OK             Err = "OK"
	ErrNoKey       Err = "ErrNoKey"
	ErrWrongGroup  Err = "ErrWrongGroup"
	ErrWrongLeader Err = "ErrWrongLeader"
	ErrTransient   Err = "ErrTransient"
)

type Err string

type Op string

const (
	PUT    Op = "PUT"
	APPEND Op = "APPEND"
)

// Put or Append
type PutAppendArgs struct {
	ReqId    int64
	ClientId int64
	Key      string
	Value    string
	Op       Op
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	ReqId    int64
	ClientId int64
	Key      string
}

type GetReply struct {
	Err   Err
	Value string
}

type RequestShardArgs struct {
	ReqId     int64
	ClientId  int64
	Gid       int
	Shard     int
	ConfigNum int
}

type RequestShardReply struct {
	Err  Err
	Logs map[string]string
}

type RemoveShard struct {
	Shard     int
	ConfigNum int
}

type AddShard struct {
	Shard int
	Logs  map[string]string
}

type UpdateConfig struct {
	ConfigNum int
}

func max(x, y int) int {
	if x > y {
		return x
	}
	return y
}
