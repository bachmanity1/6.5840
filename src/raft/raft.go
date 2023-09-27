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
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.

const (
	electionTimeout = 1500 * time.Millisecond
	heartbeatPeriod = 100 * time.Millisecond
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
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	applyCh chan ApplyMsg
	// persistent
	currentTerm int
	votedFor    int
	log         []*LogEntry
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

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.RLock()
	defer rf.mu.RUnlock()

	return rf.currentTerm, rf.me == rf.leaderId
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

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

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.leaderId = -1
	}
	reply.Term = rf.currentTerm
	lastLogIndex := len(rf.log) - 1
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		(lastLogIndex == -1 || args.LastLogTerm > rf.log[lastLogIndex].Term || (args.LastLogTerm == rf.log[lastLogIndex].Term && args.LastLogIndex >= lastLogIndex)) {
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
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("received AppendEntries, id: %d, leaderId: %d, term: %d, currentTerm: %d, prevLogIndex: %d, prevLogTerm: %d, leaderCommitIndex: %d, lastLogIndex: %d", rf.me, args.LeaderId, args.Term, rf.currentTerm, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, len(rf.log))
	reply.Term = rf.currentTerm
	reply.Success = false
	if args.Term < rf.currentTerm {
		return
	}
	rf.lastSuccessRpc = time.Now()
	rf.leaderId = args.LeaderId
	rf.currentTerm = args.Term
	reply.Term = rf.currentTerm
	if args.PrevLogIndex != 0 && (args.PrevLogIndex < 0 || args.PrevLogIndex > len(rf.log) || args.PrevLogTerm != rf.log[args.PrevLogIndex-1].Term) {
		return
	}
	for i, entry := range args.Entries {
		index := args.PrevLogIndex + i + 1
		if index > len(rf.log) || entry.Term != rf.log[index-1].Term {
			rf.log = rf.log[:index-1]
			rf.log = append(rf.log, args.Entries[i:]...)
			break
		}
	}
	if args.LeaderCommit > rf.commitIndex {
		nextCommitIndex := rf.commitIndex + 1
		rf.commitIndex = min(args.LeaderCommit, len(rf.log))
		DPrintf("update follower's commitIndex to %d, followerId: %d, term: %d", rf.commitIndex, rf.me, rf.currentTerm)
		rf.applyMessages(nextCommitIndex)
	}
	reply.Success = true
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
	if len(rf.log) != 0 {
		args.LastLogIndex = len(rf.log)
		args.LastLogTerm = rf.log[args.LastLogIndex-1].Term
	}
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
			DPrintf("sent RequestVote, candidateId: %d, voterId: %d, ok: %v, voteGranted: %v, term: %d, currentTerm: %d", rf.me, id, ok, reply.VoteGranted, reply.Term, rf.currentTerm)
			voteGranted := false
			if reply.Term == rf.currentTerm && reply.VoteGranted {
				voteGranted = true
			}
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.lastSuccessRpc = time.Now()
			}
			select {
			case votes <- voteGranted:
				DPrintf("sent vote to chan, candidateId: %d, voterId: %d", rf.me, id)
			default:
				DPrintf("chan closed, candidateId: %d, voterId: %d", rf.me, id)
			}
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
		DPrintf("election success, leaderId: %d, Term: %d", rf.me, rf.currentTerm)
		rf.leaderId = rf.me
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex = append(rf.nextIndex, len(rf.log)+1)
			rf.matchIndex = append(rf.matchIndex, 0)
		}
		go rf.sendAppendEntries()
	} else {
		DPrintf("election fail, serverId: %d, Term: %d", rf.me, rf.currentTerm)
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
				args := new(AppendEntriesArgs)
				args.Term = rf.currentTerm
				args.LeaderId = rf.me
				args.LeaderCommit = rf.commitIndex
				args.PrevLogIndex = rf.nextIndex[id] - 1
				if args.PrevLogIndex > 0 && len(rf.log) >= args.PrevLogIndex {
					args.PrevLogTerm = rf.log[args.PrevLogIndex-1].Term
				}
				if rf.nextIndex[id] >= 1 && len(rf.log) >= rf.nextIndex[id] {
					args.Entries = rf.log[args.PrevLogIndex:]
				}
				reply := new(AppendEntriesReply)
				rf.mu.RUnlock()

				ok := peer.Call("Raft.AppendEntries", args, reply)

				rf.mu.Lock()
				defer rf.mu.Unlock()

				DPrintf("sent AppendEntries, leaderId: %d, followerId: %d, ok: %v, success: %v, Term: %d, currentTerm: %d", rf.me, id, ok, reply.Success, reply.Term, rf.currentTerm)
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.lastSuccessRpc = time.Now()
					rf.leaderId = -1
					rf.votedFor = -1
					return
				}
				if reply.Success {
					rf.nextIndex[id] = len(rf.log) + 1
					rf.matchIndex[id] = len(rf.log)
				} else if !reply.Success && rf.nextIndex[id] > 1 {
					rf.nextIndex[id]--
				}

				for n := len(rf.log); n > rf.commitIndex; n-- {
					if rf.log[n-1].Term < rf.currentTerm {
						break
					}
					votes := 1
					for _, matchIndex := range rf.matchIndex {
						if matchIndex >= n {
							votes++
						}
					}
					if votes >= len(rf.peers)/2+1 {
						DPrintf("update leader's commitIndex to %d, leaderId: %d, term: %d", n, rf.me, rf.currentTerm)
						nextCommitIndex := rf.commitIndex + 1
						rf.commitIndex = n
						rf.applyMessages(nextCommitIndex)
						break
					}
				}
			}(id, peer)
		}

		rf.mu.RLock()
		if rf.me != rf.leaderId {
			DPrintf("leader stopping to send heartbeats because it received reply with higher term, leaderId: %d, currentTerm: %d", rf.me, rf.currentTerm)
			rf.mu.RUnlock()
			return
		}
		rf.mu.RUnlock()

		time.Sleep(heartbeatPeriod)
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

	rf.log = append(rf.log, &LogEntry{rf.currentTerm, command})
	DPrintf("leader %d added new log entry with index %d and term %d", rf.me, len(rf.log), rf.currentTerm)
	return len(rf.log), rf.currentTerm, true
}

func (rf *Raft) applyMessages(nextCommitIndex int) {
	for nextCommitIndex <= rf.commitIndex { // maybe duplicates ?
		applyMsg := ApplyMsg{}
		applyMsg.Command = rf.log[nextCommitIndex-1].Command
		applyMsg.CommandIndex = nextCommitIndex
		applyMsg.CommandValid = true
		rf.applyCh <- applyMsg
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
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		rf.mu.RLock()
		if rf.me != rf.leaderId && time.Since(rf.lastSuccessRpc) > electionTimeout {
			go rf.startElection()
		}
		rf.mu.RUnlock()

		// pause for a random amount of time between 50 and 350 milliseconds.
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

	rf.votedFor = -1              // load from disk
	rf.currentTerm = 0            // load from disk
	rf.log = make([]*LogEntry, 0) // load from disk
	rf.leaderId = -1
	rf.lastSuccessRpc = time.Now()
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
