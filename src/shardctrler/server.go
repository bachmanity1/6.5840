package shardctrler

import (
	"6.5840/labrpc"
	"6.5840/raft"
	"fmt"
	"log"
	"os"
	"sort"
	"sync"
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

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	configs []Config // indexed by config num
	applied map[int64]bool
	prevReq map[int]int64
	waiting map[int64]chan bool
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	sc.mu.Lock()
	index, term, ok := sc.rf.Start(*args)
	if !ok {
		reply.Err = WrongLeader
		sc.mu.Unlock()
		return
	}
	DPrintf("handle JOIN reqId: %d, serverId: %d, clientId: %v, index: %v, term: %v", args.ReqId, sc.me, args.ClientId, index, term)
	done := make(chan bool)
	sc.waiting[args.ReqId] = done
	sc.mu.Unlock()

	<-done
	reply.Err = OK
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	sc.mu.Lock()
	index, term, ok := sc.rf.Start(*args)
	if !ok {
		reply.Err = WrongLeader
		sc.mu.Unlock()
		return
	}
	DPrintf("handle LEAVE reqId: %d, serverId: %d, clientId: %v, index: %v, term: %v", args.ReqId, sc.me, args.ClientId, index, term)
	done := make(chan bool)
	sc.waiting[args.ReqId] = done
	sc.mu.Unlock()

	<-done
	reply.Err = OK
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	sc.mu.Lock()
	index, term, ok := sc.rf.Start(*args)
	if !ok {
		reply.Err = WrongLeader
		sc.mu.Unlock()
		return
	}
	DPrintf("handle MOVE reqId: %d, serverId: %d, clientId: %v, index: %v, term: %v", args.ReqId, sc.me, args.ClientId, index, term)
	done := make(chan bool)
	sc.waiting[args.ReqId] = done
	sc.mu.Unlock()

	<-done
	reply.Err = OK
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	sc.mu.Lock()
	index, term, ok := sc.rf.Start(*args)
	if !ok {
		reply.Err = WrongLeader
		sc.mu.Unlock()
		return
	}
	DPrintf("handle QUERY reqId: %d, serverId: %d, clientId: %v, index: %v, term: %v", args.ReqId, sc.me, args.ClientId, index, term)
	done := make(chan bool)
	sc.waiting[args.ReqId] = done
	sc.mu.Unlock()

	<-done
	reply.Err = OK
	sc.mu.Lock()
	if args.Num >= 0 && args.Num < len(sc.configs) {
		reply.Config = sc.configs[args.Num]
	} else if args.Num == -1 || args.Num >= len(sc.configs) {
		reply.Config = sc.configs[len(sc.configs)-1]
	}
	sc.mu.Unlock()
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	DPrintf("kill server %d", sc.me)
	sc.rf.Kill()
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}
	sc.applied = make(map[int64]bool)
	sc.prevReq = make(map[int]int64)
	sc.waiting = make(map[int64]chan bool)
	sc.applyCh = make(chan raft.ApplyMsg)

	go sc.commitLogs()
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	return sc
}

func (sc *ShardCtrler) commitLogs() {
	for msg := range sc.applyCh {
		sc.mu.Lock()
		if msg.CommandValid {
			if msg.Command == nil {
				DPrintf("got empty cmd, serverId: %d, apply message: %v", sc.me, msg)
				continue
			}

			var reqId int64
			var clientId int
			switch args := msg.Command.(type) {
			case JoinArgs:
				DPrintf("committing JOIN request, serverId: %d, index: %d, cmd: %v", sc.me, msg.CommandIndex, msg.Command)
				if !sc.applied[args.ReqId] {
					nextConfig := from(sc.configs[len(sc.configs)-1])
					nextConfig.Num++
					for gid, servers := range args.Servers {
						nextConfig.Groups[gid] = append(nextConfig.Groups[gid], servers...)
					}
					sc.rebalance(&nextConfig)
					sc.configs = append(sc.configs, nextConfig)

					reqId = args.ReqId
					clientId = args.ClientId
				}
			case LeaveArgs:
				DPrintf("committing LEAVE request, serverId: %d, index: %d, cmd: %v", sc.me, msg.CommandIndex, msg.Command)
				if !sc.applied[args.ReqId] {
					nextConfig := from(sc.configs[len(sc.configs)-1])
					nextConfig.Num++
					for _, gid := range args.GIDs {
						delete(nextConfig.Groups, gid)
					}
					sc.rebalance(&nextConfig)
					sc.configs = append(sc.configs, nextConfig)

					reqId = args.ReqId
					clientId = args.ClientId
				}
			case MoveArgs:
				DPrintf("committing MOVE request, serverId: %d, index: %d, cmd: %v", sc.me, msg.CommandIndex, msg.Command)
				if !sc.applied[args.ReqId] {
					nextConfig := from(sc.configs[len(sc.configs)-1])
					nextConfig.Num++
					nextConfig.Shards[args.Shard] = args.GID
					sc.configs = append(sc.configs, nextConfig)

					reqId = args.ReqId
					clientId = args.ClientId
				}
			case QueryArgs:
				DPrintf("committing QUERY request, serverId: %d, index: %d, cmd: %v", sc.me, msg.CommandIndex, msg.Command)

				reqId = args.ReqId
				clientId = args.ClientId
			default:
				DPrintf("unexpected command type, serverId: %d, index: %d, cmd: %v", sc.me, msg.CommandIndex, msg.Command)
			}

			sc.applied[reqId] = true
			delete(sc.applied, sc.prevReq[clientId])
			sc.prevReq[clientId] = reqId
			done, ok := sc.waiting[reqId]
			if ok {
				done <- true
			}
			delete(sc.waiting, reqId)
		} else if msg.SnapshotValid {
			log.Fatalf("received snapshot message, serverId: %d", sc.me)
		}
		sc.mu.Unlock()
	}
}

func (sc *ShardCtrler) rebalance(cfg *Config) {
	DPrintf("start rebalance shards, serverId: %d, configId: %d, shards: %v", sc.me, cfg.Num, cfg.Shards)
	groups := make([]int, 0)
	for gid := range cfg.Groups {
		groups = append(groups, gid)
	}
	sort.Ints(groups)
	gid := func(i int) int {
		if len(groups) > 0 {
			return groups[i%len(groups)]
		}
		return 0
	}
	for i := 0; i < len(cfg.Shards); i++ {
		cfg.Shards[i] = gid(i)
	}
	DPrintf("end rebalance shards, serverId: %d, configId: %d, shards: %v", sc.me, cfg.Num, cfg.Shards)
}
