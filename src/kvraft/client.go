package kvraft

import (
	"6.5840/labrpc"
	"6.5840/raft"
	"crypto/rand"
	"log"
	"math/big"
	"sync"
	"time"
)

var (
	nextCliId int
	cliIdMu   sync.Mutex
)

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	servers  []*labrpc.ClientEnd
	leaderId int
	cliId    int
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	cliIdMu.Lock()
	defer cliIdMu.Unlock()

	ck := new(Clerk)
	ck.servers = servers
	ck.cliId = nextCliId
	nextCliId++
	return ck
}

func (ck *Clerk) Get(key string) string {
	args := new(GetArgs)
	args.ReqId = nrand()
	args.Key = key
	args.ClientId = ck.cliId

	value := ""
	for {
		reply := new(GetReply)
		ck.call(ck.leaderId, "KVServer.Get", args, reply)

		if reply.Err == OK {
			value = reply.Value
			break
		} else if reply.Err == ErrTimeout || reply.Err == ErrWrongLeader {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		} else {
			log.Fatalf("unexpected err code: %v, reqId: %d, clientId: %d, leaderId: %v, key: %v",
				reply.Err, args.ReqId, ck.cliId, ck.leaderId, key)
		}
	}
	return value
}

func (ck *Clerk) PutAppend(key string, value string, op OpType) {
	args := new(PutAppendArgs)
	args.ReqId = nrand()
	args.Key = key
	args.Value = value
	args.OpType = op
	args.ClientId = ck.cliId

	for {
		reply := new(PutAppendReply)
		ck.call(ck.leaderId, "KVServer.PutAppend", args, reply)

		if reply.Err == OK {
			break
		} else if reply.Err == ErrTimeout || reply.Err == ErrWrongLeader {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		} else {
			log.Fatalf("unexpected err code: %v, reqId: %d, clientId: %d, leaderId: %v, op: %v, key: %v, value: %v",
				reply.Err, args.ReqId, ck.cliId, ck.leaderId, op, key, value)
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, PUT)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, APPEND)
}

func (ck *Clerk) call(leaderId int, svcMethod string, args interface{}, reply interface{}) {
	done := make(chan bool)
	noResponse := false
	go func() {
		done <- ck.servers[leaderId].Call(svcMethod, args, reply)
	}()

	select {
	case ok := <-done:
		noResponse = !ok
	case <-time.After(raft.ElectionTimeoutBase * time.Millisecond):
		noResponse = true
	}

	if noResponse {
		var reqId int64
		switch r := reply.(type) {
		case *GetReply:
			r.Err = ErrTimeout
			reqId = args.(*GetArgs).ReqId
		case *PutAppendReply:
			r.Err = ErrTimeout
			reqId = args.(*PutAppendArgs).ReqId
		}
		DPrintf("request timeout reqId: %d, clientId: %d, leaderId: %d", reqId, ck.cliId, leaderId)
	}
}
