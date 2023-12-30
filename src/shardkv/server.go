package shardkv

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/shardctrler"
	"bytes"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync/atomic"
	"time"
)
import "6.5840/raft"
import "sync"

const Debug = false

func DPrintf(format string, a ...interface{}) {
	blue := "\033[34m"
	logger := log.New(os.Stderr, "", log.Ltime|log.Lmicroseconds)
	if Debug {
		x := fmt.Sprintf(format, a...)
		logger.Println(blue, x)
	}
}

type ShardKV struct {
	dead         int32
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	maxraftstate int // snapshot if log grows this big
	persister    *raft.Persister
	ctrl         *shardctrler.Clerk
	shards       map[int]int
	configNum    int
	blockClients bool

	store   map[string]string
	applied map[int64]map[string]string
	prevReq map[int64]int64
	waiting map[int64]chan string
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	DPrintf("GET reqId: %d, sid: %d, gid: %d, cid: %v, key: %v", args.ReqId, kv.me, kv.gid, args.ClientId, args.Key)
	shard := key2shard(args.Key)
	if kv.shards[shard] != -1 {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		DPrintf("WRONG_GROUP - GET reqId: %d, sid: %d, gid: %d, cid: %v, key: %v", args.ReqId, kv.me, kv.gid, args.ClientId, args.Key)
		return
	}
	if kv.blockClients {
		reply.Err = ErrTransient
		kv.mu.Unlock()
		DPrintf("BLOCK_CLIENTS - GET reqId: %d, sid: %d, gid: %d, cid: %v, key: %v", args.ReqId, kv.me, kv.gid, args.ClientId, args.Key)
		return
	}
	_, _, ok := kv.rf.Start(*args)
	if !ok {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		DPrintf("WRONG_LEADER - GET reqId: %d, sid: %d, gid: %d, cid: %v, key: %v", args.ReqId, kv.me, kv.gid, args.ClientId, args.Key)
		return
	}
	done := make(chan string)
	kv.waiting[args.ReqId] = done
	kv.mu.Unlock()

	value := <-done
	reply.Err = OK
	reply.Value = value
	DPrintf("return GET reqId: %d, sid: %d, gid: %d, cid: %v, key: %v, value: %v", args.ReqId, kv.me, kv.gid, args.ClientId, args.Key, value)
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	DPrintf("%v reqId: %d, sid: %d, gid: %d, cid: %v, key: %v, value: %v", args.Op, args.ReqId, kv.me, kv.gid, args.ClientId, args.Key, args.Value)
	if _, ok := kv.applied[args.ReqId]; ok {
		reply.Err = OK
		DPrintf("ALREADY_APPLIED - %v reqId: %d, sid: %d, gid: %d, cid: %v, key: %v, value: %v", args.Op, args.ReqId, kv.me, kv.gid, args.ClientId, args.Key, args.Value)
		kv.mu.Unlock()
		return
	}

	shard := key2shard(args.Key)
	if kv.shards[shard] != -1 {
		reply.Err = ErrWrongGroup
		DPrintf("WRONG_GROUP - %v reqId: %d, sid: %d, gid: %d, cid: %v, key: %v, value: %v", args.Op, args.ReqId, kv.me, kv.gid, args.ClientId, args.Key, args.Value)
		kv.mu.Unlock()
		return
	}
	if kv.blockClients {
		reply.Err = ErrTransient
		kv.mu.Unlock()
		DPrintf("BLOCK_CLIENTS - %v reqId: %d, sid: %d, gid: %d, cid: %v, key: %v, value: %v", args.Op, args.ReqId, kv.me, kv.gid, args.ClientId, args.Key, args.Value)
		return
	}
	_, _, ok := kv.rf.Start(*args)
	if !ok {
		reply.Err = ErrWrongLeader
		DPrintf("WRONG_LEADER - %v reqId: %d, sid: %d, gid: %d, cid: %v, key: %v, value: %v", args.Op, args.ReqId, kv.me, kv.gid, args.ClientId, args.Key, args.Value)
		kv.mu.Unlock()
		return
	}
	done := make(chan string)
	kv.waiting[args.ReqId] = done
	kv.mu.Unlock()

	<-done
	reply.Err = OK
	DPrintf("return %v reqId: %d, sid: %d, gid: %d, cid: %v, key: %v, value: %v", args.Op, args.ReqId, kv.me, kv.gid, args.ClientId, args.Key, args.Value)
}

func (kv *ShardKV) RequestShard(args *RequestShardArgs, reply *RequestShardReply) {
	kv.mu.Lock()
	DPrintf("REQUEST_SHARD reqId: %d, sid: %d, shard: %d, sourceGid: %d, sourceConfig: %d, destGid: %d, destConfig: %d", args.ReqId, kv.me, args.Shard, kv.gid, kv.configNum, args.Gid, args.ConfigNum)
	if logs, ok := kv.applied[args.ReqId]; ok {
		reply.Err = OK
		reply.Logs = logs
		DPrintf("ALREADY_APPLIED - REQUEST_SHARD reqId: %d, sid: %d, shard: %d, sourceGid: %d, sourceConfig: %d, destGid: %d, destConfig: %d", args.ReqId, kv.me, args.Shard, kv.gid, kv.configNum, args.Gid, args.ConfigNum)
		kv.mu.Unlock()
		return
	}
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		DPrintf("WRONG_LEADER - REQUEST_SHARD reqId: %d, sid: %d, shard: %d, sourceGid: %d, sourceConfig: %d, destGid: %d, destConfig: %d", args.ReqId, kv.me, args.Shard, kv.gid, kv.configNum, args.Gid, args.ConfigNum)
		kv.mu.Unlock()
		return
	}
	if kv.shards[args.Shard] != args.ConfigNum {
		reply.Err = ErrTransient
		DPrintf("NOT_REMOVED_YET - REQUEST_SHARD reqId: %d, sid: %d, shard: %d, sourceGid: %d, sourceConfig: %d, destGid: %d, destConfig: %d, shardConfig: %d", args.ReqId, kv.me, args.Shard, kv.gid, kv.configNum, args.Gid, args.ConfigNum, kv.shards[args.Shard])
		kv.mu.Unlock()
		return
	}

	_, _, ok := kv.rf.Start(*args)
	if !ok {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		DPrintf("WRONG_LEADER - REQUEST_SHARD reqId: %d, sid: %d, shard: %d, sourceGid: %d, sourceConfig: %d, destGid: %d, destConfig: %d", args.ReqId, kv.me, args.Shard, kv.gid, kv.configNum, args.Gid, args.ConfigNum)
		return
	}
	done := make(chan string)
	kv.waiting[args.ReqId] = done
	DPrintf("return - REQUEST_SHARD reqId: %d, sid: %d, shard: %d, sourceGid: %d, sourceConfig: %d, destGid: %d, destConfig: %d, logs: %v", args.ReqId, kv.me, args.Shard, kv.gid, kv.configNum, args.Gid, args.ConfigNum, reply.Logs)
	kv.mu.Unlock()

	<-done
	kv.mu.Lock()
	reply.Err = OK
	reply.Logs = kv.applied[args.ReqId]
	kv.mu.Unlock()
}

func (kv *ShardKV) updateShards() {
	initialized := false
	for {
		_, isLeader := kv.rf.GetState()
		if isLeader {
			kv.mu.Lock()
			kv.blockClients = true

			if !initialized {
				kv.rf.Start(UpdateConfig{kv.configNum})
				for !kv.rf.IsAllCommitted() {
					DPrintf("server init not committed all logs, server %d, group %d, configNum %d", kv.me, kv.gid, kv.configNum)
					kv.mu.Unlock()
					time.Sleep(10 * time.Millisecond)
					kv.mu.Lock()
				}
				DPrintf("server init committed all logs, server %d, group %d, configNum %d", kv.me, kv.gid, kv.configNum)
				initialized = true
			}

			config := kv.ctrl.Query(-1)
			DPrintf("server %d, group %d, seenConfigNum %d, realConfigNum %d, seenShards: %v, realShards: %v", kv.me, kv.gid, kv.configNum, config.Num, kv.shards, config.Shards)
			for kv.configNum < config.Num && !kv.killed() {
				if !kv.rf.IsAllCommitted() {
					DPrintf("not committed all logs, server %d, group %d, seenConfigNum %d, realConfigNum %d, seenShards: %v, realShards: %v", kv.me, kv.gid, kv.configNum, config.Num, kv.shards, config.Shards)
					kv.mu.Unlock()
					time.Sleep(10 * time.Millisecond)
					kv.mu.Lock()
					continue
				}
				DPrintf("committed all logs, server %d, group %d, seenConfigNum %d, realConfigNum %d, seenShards: %v, realShards: %v", kv.me, kv.gid, kv.configNum, config.Num, kv.shards, config.Shards)

				currConfig := kv.ctrl.Query(kv.configNum)
				nextConfig := kv.ctrl.Query(kv.configNum + 1)
				for shard := 0; shard < shardctrler.NShards; shard++ {
					if currConfig.Shards[shard] != kv.gid && nextConfig.Shards[shard] == kv.gid {
						addShard := AddShard{}
						addShard.Shard = shard
						shardGroup := currConfig.Shards[shard]
						if servers, ok := currConfig.Groups[shardGroup]; ok {
							args := new(RequestShardArgs)
							args.ClientId = int64(kv.me)
							args.Shard = shard
							args.Gid = kv.gid
							args.ConfigNum = nextConfig.Num
							reqId, _ := strconv.Atoi(fmt.Sprintf("%d%d%d%d", nextConfig.Num, shard, shardGroup, kv.gid))
							args.ReqId = int64(reqId)
							reply := new(RequestShardReply)
							reply.Logs = make(map[string]string)
						Out:
							for kv.shards[shard] != -1 {
								for si := 0; si < len(servers); si++ {
									srv := kv.make_end(servers[si])

									kv.mu.Unlock()
									ok := srv.Call("ShardKV.RequestShard", args, reply)
									kv.mu.Lock()

									if ok && reply.Err == OK {
										break Out
									}
								}
								if kv.killed() {
									DPrintf("group %d server %d quit updateShards", kv.gid, kv.me)
									return
								}
							}
							addShard.Logs = reply.Logs
							DPrintf("server %d group %d got shard %d from group %d with logs %v", kv.me, kv.gid, shard, shardGroup, addShard.Logs)
						}
						kv.rf.Start(addShard)
					} else if currConfig.Shards[shard] == kv.gid && nextConfig.Shards[shard] != kv.gid {
						removeShard := RemoveShard{}
						removeShard.Shard = shard
						removeShard.ConfigNum = nextConfig.Num
						kv.rf.Start(removeShard)
					}
				}
				kv.rf.Start(UpdateConfig{nextConfig.Num})
				config = kv.ctrl.Query(-1)
			}
			kv.blockClients = false

			kv.mu.Unlock()
		}

		if kv.killed() {
			DPrintf("group %d server %d quit updateShards", kv.gid, kv.me)
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	DPrintf("shardkv killed id %d, gid: %d", kv.me, kv.gid)
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	DPrintf("starting shardkv id: %d, gid: %d", me, gid)
	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.persister = persister
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrl = shardctrler.MakeClerk(ctrlers)

	kv.store = make(map[string]string)
	kv.applied = make(map[int64]map[string]string)
	kv.prevReq = make(map[int64]int64)
	kv.waiting = make(map[int64]chan string)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.shards = make(map[int]int)
	kv.blockClients = true

	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.updateShards()
	go kv.commitLogs()

	return kv
}

func (kv *ShardKV) commitLogs() {
	for msg := range kv.applyCh {
		if kv.killed() {
			DPrintf("group %d server %d quit commitLogs", kv.gid, kv.me)
			return
		}

		kv.mu.Lock()
		if msg.CommandValid {
			DPrintf("commit log, sid: %d, gid: %d, index: %v, %v", kv.me, kv.gid, msg.CommandIndex, msg.Command)
			if msg.Command == nil {
				DPrintf("got empty op, sid: %d, gid: %d, apply message: %v", kv.me, kv.gid, msg)
				continue
			}

			var reqId, clientId int64
			var value string
			var cached map[string]string
			switch args := msg.Command.(type) {
			case GetArgs:
				reqId = args.ReqId
				clientId = args.ClientId
				value = kv.store[args.Key]
			case PutAppendArgs:
				reqId = args.ReqId
				clientId = args.ClientId
				value = kv.store[args.Key]
				if _, ok := kv.applied[reqId]; !ok {
					if args.Op == APPEND {
						value += args.Value
					} else if args.Op == PUT {
						value = args.Value
					}
				}
				kv.store[args.Key] = value
			case RequestShardArgs:
				DPrintf("server %d gid %d remove logs of shard %d", kv.me, kv.gid, args.Shard)
				reqId = args.ReqId
				clientId = args.ClientId
				if _, ok := kv.applied[reqId]; !ok {
					cached = make(map[string]string)
					for key, value := range kv.store {
						if key2shard(key) == args.Shard {
							cached[key] = value
							delete(kv.store, key)
						}
					}
					delete(kv.shards, args.Shard)
				}
			case RemoveShard:
				DPrintf("server %d gid %d remove shard %d", kv.me, kv.gid, args.Shard)
				if kv.shards[args.Shard] == -1 {
					kv.shards[args.Shard] = args.ConfigNum
				}
			case AddShard:
				DPrintf("server %d gid %d add shard %d with %d logs", kv.me, kv.gid, args.Shard, len(args.Logs))
				if _, ok := kv.shards[args.Shard]; !ok {
					kv.shards[args.Shard] = -1
					for key, value := range args.Logs {
						kv.store[key] = value
					}
				}
			case UpdateConfig:
				DPrintf("server %d gid %d update config to %d", kv.me, kv.gid, args.ConfigNum)
				hasNotRemovedShards := false
				for shard, configNum := range kv.shards {
					if configNum == args.ConfigNum {
						DPrintf("server %d gid %d has not removed shard %d, configNum %d", kv.me, kv.gid, shard, configNum)
						hasNotRemovedShards = true
					}
				}
				if hasNotRemovedShards {
					kv.rf.Start(UpdateConfig{args.ConfigNum})
				} else {
					kv.configNum = max(kv.configNum, args.ConfigNum)
				}
			}

			//kv.applied[kv.prevReq[clientId]] = nil
			kv.prevReq[clientId] = reqId
			kv.applied[reqId] = cached
			done, ok := kv.waiting[reqId]
			if ok {
				done <- value
			}
			delete(kv.waiting, reqId)

			if kv.maxraftstate > 0 && kv.persister.RaftStateSize() >= 8*kv.maxraftstate-500 {
				w := new(bytes.Buffer)
				e := labgob.NewEncoder(w)
				e.Encode(kv.store)
				e.Encode(kv.applied)
				e.Encode(kv.prevReq)
				e.Encode(kv.shards)
				e.Encode(kv.configNum)
				kv.rf.Snapshot(msg.CommandIndex, w.Bytes())
				DPrintf("take snapshot, sid: %d, gid: %d, raftStateSize: %d, maxRaftStateSize: %d, snapshotSize: %d, configNum: %d, shards: %v",
					kv.me, kv.gid, kv.persister.RaftStateSize(), kv.maxraftstate, kv.persister.SnapshotSize(), kv.configNum, kv.shards)
			}
		} else if msg.SnapshotValid {
			r := bytes.NewBuffer(msg.Snapshot)
			d := labgob.NewDecoder(r)
			var newStore map[string]string
			var newApplied map[int64]map[string]string
			var newPrevReq map[int64]int64
			var newShards map[int]int
			var newConfigNum int
			if d.Decode(&newStore) != nil || d.Decode(&newApplied) != nil || d.Decode(&newPrevReq) != nil || d.Decode(&newShards) != nil || d.Decode(&newConfigNum) != nil {
				log.Fatalf("snapshot decode error, sid: %d, gid: %d", kv.me, kv.gid)
			}
			kv.store = newStore
			kv.applied = newApplied
			kv.prevReq = newPrevReq
			kv.shards = newShards
			kv.configNum = newConfigNum
			DPrintf("install snapshot, sid: %d, gid: %d, configNum: %d, shards: %v, commandIndex: %d", kv.me, kv.gid, kv.configNum, kv.shards, msg.CommandIndex)
		}
		kv.mu.Unlock()
	}
}
