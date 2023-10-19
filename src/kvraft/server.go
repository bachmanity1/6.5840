package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"bytes"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
)

const Debug = false

func DPrintf(format string, a ...interface{}) {
	blue := "\033[34m"
	logger := log.New(os.Stderr, "", log.Ltime|log.Lmicroseconds)
	if Debug {
		x := fmt.Sprintf(format, a...)
		logger.Println(blue, x)
	}
}

type OpType string

const (
	GET    OpType = "GET"
	PUT    OpType = "PUT"
	APPEND OpType = "APPEND"
)

type Op struct {
	OpType
	Key      string
	Value    string
	ReqId    int64
	ClientId int
}

func (op Op) String() string {
	return fmt.Sprintf("reqId: %d, type: %v, key: %v, value: %v", op.ReqId, op.OpType, op.Key, op.Value)
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32

	maxraftstate int
	persister    *raft.Persister

	store   map[string]string
	applied map[int64]bool
	prevReq map[int]int64
	waiting map[int64]chan string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	index, term, ok := kv.rf.Start(Op{GET, args.Key, "", args.ReqId, args.ClientId})
	if !ok {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	DPrintf("handle %v reqId: %d, serverId: %d, clientId: %v, key: %v, index: %v, term: %v", GET, args.ReqId, kv.me, args.ClientId, args.Key, index, term)
	done := make(chan string)
	kv.waiting[args.ReqId] = done
	kv.mu.Unlock()

	value := <-done
	reply.Err = OK
	reply.Value = value
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	index, term, ok := kv.rf.Start(Op{args.OpType, args.Key, args.Value, args.ReqId, args.ClientId})
	if !ok {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	DPrintf("handle %v reqId: %d, serverId: %d, clientId: %v, key: %v, index: %v, term: %v", args.OpType, args.ReqId, kv.me, args.ClientId, args.Key, index, term)
	done := make(chan string)
	kv.waiting[args.ReqId] = done
	kv.mu.Unlock()

	<-done
	reply.Err = OK
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	DPrintf("server %d killed", kv.me)
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.persister = persister

	kv.store = make(map[string]string)
	kv.applied = make(map[int64]bool)
	kv.prevReq = make(map[int]int64)
	kv.waiting = make(map[int64]chan string)
	kv.applyCh = make(chan raft.ApplyMsg)

	go kv.commitLogs()
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	return kv
}

func (kv *KVServer) commitLogs() {
	for msg := range kv.applyCh {
		kv.mu.Lock()
		if msg.CommandValid {
			DPrintf("commit log, serverId: %d, index: %v, %v", kv.me, msg.CommandIndex, msg.Command)
			if msg.Command == nil {
				DPrintf("got empty op, serverId: %d, apply message: %v", kv.me, msg)
				continue
			}
			op := msg.Command.(Op)
			value := kv.store[op.Key]
			if !kv.applied[op.ReqId] {
				if op.OpType == APPEND {
					value += op.Value
				} else if op.OpType == PUT {
					value = op.Value
				}
				kv.store[op.Key] = value
				kv.applied[op.ReqId] = true
				delete(kv.applied, kv.prevReq[op.ClientId])
				kv.prevReq[op.ClientId] = op.ReqId
			}
			done, ok := kv.waiting[op.ReqId]
			if ok {
				done <- value
			}
			delete(kv.waiting, op.ReqId)

			if kv.maxraftstate > 0 && kv.persister.RaftStateSize() >= 8*kv.maxraftstate-500 {
				w := new(bytes.Buffer)
				e := labgob.NewEncoder(w)
				e.Encode(kv.store)
				e.Encode(kv.applied)
				e.Encode(kv.prevReq)
				kv.rf.Snapshot(msg.CommandIndex, w.Bytes())
				DPrintf("take snapshot, serverId: %d, raftStateSize: %d, maxRaftStateSize: %d, snapshotSize: %d",
					kv.me, kv.persister.RaftStateSize(), kv.maxraftstate, kv.persister.SnapshotSize())
			}
		} else if msg.SnapshotValid {
			DPrintf("install snapshot, serverId: %d", kv.me)
			r := bytes.NewBuffer(msg.Snapshot)
			d := labgob.NewDecoder(r)
			var newStore map[string]string
			var newApplied map[int64]bool
			var newPrevReq map[int]int64
			if d.Decode(&newStore) != nil || d.Decode(&newApplied) != nil || d.Decode(&newPrevReq) != nil {
				log.Fatalf("snapshot decode error, serverId: %d", kv.me)
			}
			kv.store = newStore
			kv.applied = newApplied
			kv.prevReq = newPrevReq
		}
		kv.mu.Unlock()
	}
}
