package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(Command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

const (
	ElectionTimeoutBase = 1000
	heartbeatPeriod     = 100
	appendPeriod        = 20
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	commitLock sync.Mutex
	mu         sync.RWMutex        // Lock to protect shared access to this peer's state
	peers      []*labrpc.ClientEnd // RPC end points of all peers
	persister  *Persister          // Object to hold this peer's persisted state
	me         int                 // this peer's index into peers[]
	dead       int32               // set by Kill()

	applyCh chan ApplyMsg
	// persistent
	currentTerm  int
	votedFor     int
	log          []*LogEntry
	offset       int
	snapshot     []byte
	snapshotTerm int
	// volatile
	commitIndex    int
	lastApplied    int
	leaderId       int
	lastSuccessRpc time.Time
	// volatile & exclusive to leader
	nextIndex  []int
	matchIndex []int
}

type LogEntry struct {
	Term    int
	Command interface{}
}

func (l *LogEntry) String() string {
	return fmt.Sprintf("{Term: %d, Cmd: %v}", l.Term, l.Command)
}

func (rf *Raft) lastLogIndex() int {
	return rf.offset + len(rf.log) - 1
}

func (rf *Raft) lastLogTerm() int {
	return rf.logTerm(rf.lastLogIndex())
}

func (rf *Raft) logTerm(logIndex int) int {
	if logIndex < rf.offset {
		return rf.snapshotTerm
	} else if logIndex > rf.lastLogIndex() {
		return -1
	} else {
		return rf.log[logIndex-rf.offset].Term
	}
}

func (rf *Raft) postfix(index int) []*LogEntry {
	if index >= rf.offset && index <= rf.lastLogIndex() {
		return rf.log[index-rf.offset:]
	}
	return make([]*LogEntry, 0)
}

func (rf *Raft) prefix(index int) []*LogEntry {
	if index >= rf.offset && index <= rf.lastLogIndex()+1 {
		return rf.log[:index-rf.offset]
	}
	return make([]*LogEntry, 0)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.RLock()
	defer rf.mu.RUnlock()

	return rf.currentTerm, rf.me == rf.leaderId
}

func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if err := e.Encode(rf.currentTerm); err != nil {
		DPrintf("failed to encode currentTerm, id: %d, term: %d", rf.me, rf.currentTerm)
		return
	}
	if err := e.Encode(rf.votedFor); err != nil {
		DPrintf("failed to encode votedFor, id: %d, term: %d", rf.me, rf.currentTerm)
		return
	}
	if err := e.Encode(rf.log); err != nil {
		DPrintf("failed to encode log, id: %d, term: %d", rf.me, rf.currentTerm)
		return
	}
	if err := e.Encode(rf.offset); err != nil {
		DPrintf("failed to encode offset, id: %d, offset: %d", rf.me, rf.offset)
		return
	}
	if err := e.Encode(rf.snapshotTerm); err != nil {
		DPrintf("failed to encode snapshotTerm, id: %d, snapshotTerm: %d", rf.me, rf.snapshotTerm)
		return
	}

	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.snapshot)
	DPrintf("persist state, id: %d, stateSize: %d, currentTerm: %d, votedFor: %d, logLen %d, log: %v",
		rf.me, len(raftstate), rf.currentTerm, rf.votedFor, rf.lastLogIndex(), rf.log)
}

func (rf *Raft) readPersist(data []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, votedFor, offset, snapshotTerm int
	var log []*LogEntry
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil || d.Decode(&offset) != nil || d.Decode(&snapshotTerm) != nil {
		DPrintf("failed to decode persistent state, id: %d, currentTerm: %d", rf.me, rf.currentTerm)
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.offset = offset
		rf.snapshotTerm = snapshotTerm
	}
	DPrintf("finished to read persisted state, id: %d, currentTerm: %d, votedFor: %d, logLen: %d, offset: %d, snapshotTerm: %d",
		rf.me, rf.currentTerm, rf.votedFor, rf.lastLogIndex(), rf.offset, rf.snapshotTerm)
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index < rf.offset || index > rf.lastLogIndex() {
		DPrintf("abort snapshot, index out of bounds, id: %d, index: %d, offset: %d, lastLogIndex: %d", rf.me, index, rf.offset, rf.lastLogIndex())
		return
	}
	rf.log = rf.postfix(index + 1)
	rf.offset = index + 1
	rf.snapshot = snapshot
	rf.snapshotTerm = rf.currentTerm
	rf.persist()
	DPrintf("took snapshot, id: %d, index: %d, currentTerm: %d, stateSize: %d", rf.me, index, rf.currentTerm, rf.persister.RaftStateSize())
}

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("received RequestVote, id: %d, candidateId: %d, term: %d, currentTerm: %d, votedFor: %d", rf.me, args.CandidateId, args.Term, rf.currentTerm, rf.votedFor)
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		return
	}
	defer rf.persist()
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.leaderId = -1
	}
	reply.Term = rf.currentTerm
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		(rf.lastLogIndex() == 0 || args.LastLogTerm > rf.lastLogTerm() || (args.LastLogTerm == rf.lastLogTerm() && args.LastLogIndex >= rf.lastLogIndex())) {
		reply.VoteGranted = true
		rf.lastSuccessRpc = time.Now()
		rf.votedFor = args.CandidateId
		DPrintf("granted vote, id: %d, candidateId: %d, term: %d, currentTerm: %d", rf.me, args.CandidateId, args.Term, rf.currentTerm)
	}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []*LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	XTerm   int
	XIndex  int
	XLen    int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()

	DPrintf("received AppendEntries, id: %d, leaderId: %d, term: %d, currentTerm: %d, prevLogIndex: %d, prevLogTerm: %d, leaderCommitIndex: %d, logLen: %d", rf.me, args.LeaderId, args.Term, rf.currentTerm, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, rf.lastLogIndex())
	reply.Term = rf.currentTerm
	reply.Success = false
	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		return
	}
	rf.lastSuccessRpc = time.Now()
	rf.leaderId = args.LeaderId
	rf.currentTerm = args.Term
	reply.Term = rf.currentTerm
	prevLogTerm := rf.logTerm(args.PrevLogIndex)
	if args.PrevLogIndex != 0 && (args.PrevLogIndex > rf.lastLogIndex() || args.PrevLogTerm != prevLogTerm) {
		reply.XLen = rf.lastLogIndex()
		reply.XTerm = prevLogTerm
		for i := args.PrevLogIndex; rf.logTerm(i) == reply.XTerm; i-- {
			reply.XIndex = i
		}
		rf.persist()
		rf.mu.Unlock()
		return
	}
	for i, entry := range args.Entries {
		index := args.PrevLogIndex + i + 1
		if index > rf.lastLogIndex() || entry.Term != rf.logTerm(index) {
			rf.log = rf.prefix(index)
			rf.log = append(rf.log, args.Entries[i:]...)
			break
		}
	}
	newCommitIndex := rf.commitIndex
	if args.LeaderCommit > rf.commitIndex {
		newCommitIndex = min(args.LeaderCommit, rf.lastLogIndex())
	}
	reply.Success = true
	rf.persist()
	rf.mu.Unlock()

	rf.commitLogs(newCommitIndex)
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()

	DPrintf("received InstallSnapshot, id: %d, currentTerm: %d, term: %d, leaderId: %d, lastIncludedIndex: %d, lastIncludedTerm: %d",
		rf.me, rf.currentTerm, args.Term, args.LeaderId, args.LastIncludedIndex, args.LastIncludedTerm)
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		return
	}
	rf.lastSuccessRpc = time.Now()
	rf.leaderId = args.LeaderId
	rf.currentTerm = args.Term
	rf.log = rf.postfix(args.LastIncludedIndex + 1)
	rf.offset = args.LastIncludedIndex + 1
	rf.snapshotTerm = args.LastIncludedTerm
	rf.snapshot = args.Data
	rf.persist()
	reply.Term = rf.currentTerm
	rf.mu.Unlock()

	rf.installSnapshot()
}

func (rf *Raft) startElection() {
	rf.mu.Lock()

	rf.leaderId = -1
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.lastSuccessRpc = time.Now()
	votesFor := 1
	votesAgainst := 0
	args := new(RequestVoteArgs)
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	args.LastLogIndex = rf.lastLogIndex()
	args.LastLogTerm = rf.lastLogTerm()
	rf.persist()
	DPrintf("starting election, id: %d, term: %d", rf.me, rf.currentTerm)

	rf.mu.Unlock()

	votes := make(chan bool, len(rf.peers))
	quorum := len(rf.peers)/2 + 1
	for id, peer := range rf.peers {
		if id == rf.me {
			continue
		}
		go func(id int, peer *labrpc.ClientEnd) {
			reply := new(RequestVoteReply)
			ok := peer.Call("Raft.RequestVote", args, reply)

			rf.mu.Lock()
			defer rf.mu.Unlock()
			DPrintf("sent RequestVote, ok: %v, candidateId: %d, voterId: %d, voteGranted: %v, term: %d, lastLogTerm: %d, lastLogIndex: %d", ok, args.CandidateId, id, reply.VoteGranted, reply.Term, args.LastLogTerm, args.LastLogIndex)
			voteGranted := false
			if reply.Term == rf.currentTerm && reply.VoteGranted {
				voteGranted = true
			}
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.lastSuccessRpc = time.Now()
				rf.persist()
			}
			votes <- voteGranted
		}(id, peer)
	}
	for vote := range votes {
		if vote {
			votesFor++
		} else {
			votesAgainst++
		}
		if votesFor >= quorum || votesAgainst >= quorum {
			break
		}
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term == rf.currentTerm && votesFor >= quorum {
		DPrintf("election success, leaderId: %d, Term: %d", rf.me, args.Term)
		rf.leaderId = rf.me
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex = append(rf.nextIndex, rf.lastLogIndex()+1)
			rf.matchIndex = append(rf.matchIndex, 0)
		}
		go rf.sendAppendEntries()
	} else {
		DPrintf("election fail, serverId: %d, Term: %d", rf.me, args.Term)
	}
}

func (rf *Raft) sendAppendEntries() {
	for !rf.killed() {
		for id, peer := range rf.peers {
			if id == rf.me {
				continue
			}
			go func(id int, peer *labrpc.ClientEnd) {
				rf.mu.RLock()
				if rf.me != rf.leaderId {
					DPrintf("server %d is no longer leader, abort AppendEntries rpc, term %d", rf.me, rf.currentTerm)
					rf.mu.RUnlock()
					return
				}
				if rf.nextIndex[id] < rf.offset {
					args := new(InstallSnapshotArgs)
					args.Term = rf.currentTerm
					args.LeaderId = rf.me
					args.LastIncludedTerm = rf.snapshotTerm
					args.LastIncludedIndex = rf.offset - 1
					args.Data = rf.snapshot
					reply := new(InstallSnapshotReply)
					rf.mu.RUnlock()

					ok := peer.Call("Raft.InstallSnapshot", args, reply)

					rf.mu.Lock()
					defer rf.mu.Unlock()
					if !ok {
						DPrintf("failed to send InstallSnapshot, leaderId: %d, followerId: %d, lastIncludedTerm: %d, lastIncludedIndex: %d, currentTerm: %d", args.LeaderId, id, args.LastIncludedTerm, args.LastIncludedIndex, rf.currentTerm)
						return
					}
					DPrintf("sent InstallSnapshot, leaderId: %d, followerId: %d, term: %d, lastIncludedTerm: %d, lastIncludedIndex: %d, currentTerm: %d", rf.me, id, reply.Term, args.LastIncludedTerm, args.LastIncludedIndex, rf.currentTerm)
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.lastSuccessRpc = time.Now()
						rf.leaderId = -1
						rf.votedFor = -1
						rf.persist()
						return
					}
					rf.matchIndex[id] = args.LastIncludedIndex
					rf.nextIndex[id] = rf.matchIndex[id] + 1
				} else {
					args := new(AppendEntriesArgs)
					args.Term = rf.currentTerm
					args.LeaderId = rf.me
					args.LeaderCommit = rf.commitIndex
					args.PrevLogIndex = rf.nextIndex[id] - 1
					args.PrevLogTerm = rf.logTerm(args.PrevLogIndex)
					args.Entries = make([]*LogEntry, len(rf.postfix(rf.nextIndex[id])))
					copy(args.Entries, rf.postfix(rf.nextIndex[id]))
					reply := new(AppendEntriesReply)
					rf.mu.RUnlock()

					ok := peer.Call("Raft.AppendEntries", args, reply)

					rf.mu.Lock()

					if !ok {
						DPrintf("failed to send AppendEntries, leaderId: %d, followerId: %d, prevLogIndex: %d, prevLogTerm: %d, currentTerm: %d", args.LeaderId, id, args.PrevLogIndex, args.PrevLogTerm, rf.currentTerm)
						rf.mu.Unlock()
						return
					}
					DPrintf("sent AppendEntries, leaderId: %d, followerId: %d, success: %v, term: %d, currentTerm: %d, offset: %d", args.LeaderId, id, reply.Success, reply.Term, rf.currentTerm, rf.offset)
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.lastSuccessRpc = time.Now()
						rf.leaderId = -1
						rf.votedFor = -1
						rf.persist()
						rf.mu.Unlock()
						return
					}
					if reply.Success {
						rf.matchIndex[id] = args.PrevLogIndex + len(args.Entries)
						rf.nextIndex[id] = rf.matchIndex[id] + 1
					} else {
						if args.PrevLogIndex > reply.XLen {
							rf.nextIndex[id] = reply.XLen + 1
						} else if rf.logTerm(reply.XIndex) == reply.XTerm {
							rf.nextIndex[id] = reply.XIndex + 1
						} else {
							rf.nextIndex[id] = reply.XIndex
						}
					}

					newCommitIndex := rf.commitIndex
					for n := rf.lastLogIndex(); n >= rf.offset && n > rf.commitIndex; n-- {
						if rf.logTerm(n) != rf.currentTerm {
							break
						}
						votes := 1
						for _, matchIndex := range rf.matchIndex {
							if matchIndex >= n {
								votes++
							}
						}
						if votes >= len(rf.peers)/2+1 {
							newCommitIndex = n
							break
						}
					}
					rf.mu.Unlock()

					rf.commitLogs(newCommitIndex)
				}
			}(id, peer)
		}

		rf.mu.RLock()
		if rf.me != rf.leaderId {
			DPrintf("leader stopping to send heartbeats because it received reply with higher term, leaderId: %d, currentTerm: %d", rf.me, rf.currentTerm)
			rf.mu.RUnlock()
			return
		}
		sleepTime := heartbeatPeriod * time.Millisecond
		if rf.lastLogIndex() > rf.commitIndex {
			sleepTime = appendPeriod * time.Millisecond
		}
		rf.mu.RUnlock()

		time.Sleep(sleepTime)
	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next Command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// Command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the Command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.me != rf.leaderId {
		return -1, -1, false
	}
	defer rf.persist()
	rf.log = append(rf.log, &LogEntry{rf.currentTerm, command})
	DPrintf("leader %d added new log entry with index %d and term %d", rf.me, rf.lastLogIndex(), rf.currentTerm)
	return rf.lastLogIndex(), rf.currentTerm, true
}

func (rf *Raft) installSnapshot() {
	rf.mu.RLock()
	DPrintf("install snapshot, id: %d, commitIndex: %d, offset: %d, snapshot term: %d", rf.me, rf.commitIndex, rf.offset, rf.snapshotTerm)
	if rf.offset-1 < rf.commitIndex {
		DPrintf("abort snapshot, id: %d, commitIndex: %d, offset: %d", rf.me, rf.commitIndex, rf.offset)
		rf.mu.RUnlock()
		return
	}
	applyMsg := ApplyMsg{}
	applyMsg.Snapshot = rf.snapshot
	applyMsg.SnapshotTerm = rf.snapshotTerm
	applyMsg.SnapshotIndex = rf.offset - 1
	applyMsg.SnapshotValid = true
	rf.mu.RUnlock()

	rf.commitLock.Lock()
	rf.applyCh <- applyMsg
	rf.lastApplied = applyMsg.SnapshotIndex
	rf.commitLock.Unlock()

	rf.mu.Lock()
	rf.commitIndex = max(applyMsg.SnapshotIndex, rf.commitIndex)
	rf.mu.Unlock()
}

func (rf *Raft) commitLogs(newCommitIndex int) {
	rf.mu.RLock()
	DPrintf("start commit, id: %d, term: %d, offset: %d, commitIndex: %d, newCommitIndex: %d", rf.me, rf.currentTerm, rf.offset, rf.commitIndex, newCommitIndex)
	if newCommitIndex <= rf.commitIndex {
		DPrintf("abort commit, id: %d, commitIndex: %d, newCommitIndex: %d, offset: %d", rf.me, rf.commitIndex, newCommitIndex, rf.offset)
		rf.mu.RUnlock()
		return
	}
	nextCommitIndex := rf.commitIndex + 1
	rf.mu.RUnlock()

	for nextCommitIndex <= newCommitIndex {
		applyMsg := ApplyMsg{}
		applyMsg.CommandIndex = nextCommitIndex
		applyMsg.CommandValid = true

		rf.mu.RLock()
		if nextCommitIndex < rf.offset || nextCommitIndex > rf.lastLogIndex() {
			nextCommitIndex++
			rf.mu.RUnlock()
			continue
		}
		applyMsg.Command = rf.log[nextCommitIndex-rf.offset].Command
		rf.mu.RUnlock()

		rf.commitLock.Lock()
		DPrintf("commit log, id: %d, nextCommitIndex: %d, lastApplied: %d", rf.me, nextCommitIndex, rf.lastApplied)
		applied := false
		if rf.lastApplied == nextCommitIndex-1 {
			rf.lastApplied++
			rf.applyCh <- applyMsg
			applied = true
		}
		rf.commitLock.Unlock()

		if applied {
			rf.mu.Lock()
			rf.commitIndex = max(nextCommitIndex, rf.commitIndex)
			rf.mu.Unlock()
		}
		nextCommitIndex++
	}

}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	DPrintf("server %d killed", rf.me)
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	electionTimeout := ElectionTimeoutBase + (rand.Int63() % ElectionTimeoutBase)
	DPrintf("server id: %d, initial electionTimeout: %d", rf.me, electionTimeout)
	for !rf.killed() {
		rf.mu.RLock()
		if rf.me != rf.leaderId && time.Since(rf.lastSuccessRpc).Milliseconds() > electionTimeout {
			electionTimeout = ElectionTimeoutBase + (rand.Int63() % ElectionTimeoutBase)
			DPrintf("server id: %d, updated electionTimeout: %d, currentTerm: %d", rf.me, electionTimeout, rf.currentTerm)
			go rf.startElection()
		}
		rf.mu.RUnlock()

		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// default values
	rf.votedFor = -1
	rf.currentTerm = 0
	rf.log = make([]*LogEntry, 0)
	rf.offset = 1
	rf.leaderId = -1
	rf.lastSuccessRpc = time.Now()
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	if persister.SnapshotSize() > 0 {
		rf.snapshot = persister.ReadSnapshot()
		go rf.installSnapshot()
	}
	go rf.ticker()

	return rf
}
