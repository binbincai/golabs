package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"fmt"
	"labgob"
	"labrpc"
	"sync"
)

//
// RoleType is for raft object's role type
//
type RoleType int32

// TODO: 新增learner用于学习？
const (
	FOLLOWER RoleType = iota
	CANDIDATE
	LEADER
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm   int         // Latest term server has seen
	votedFor      int         // CandidateId that received vote in current term
	log           map[int]Log // Log entries, first log's index is 1
	firstLogIndex int         // Index of first valid log entry
	lastLogIndex  int         // Index of last valid log entry
	snapshotIndex int         // Index of current snapshot
	snapshotTerm  int         // Term of current snapshot

	commitIndex int // Index of highest log entry known to be commited
	lastApplied int // Index of highest log entry applied to state machine

	nextIndex  []int // For each server, index of next log entry to send to that server
	matchIndex []int // For each server, index of highest log entry known replicated to that server

	role             RoleType
	resetCh          chan byte
	commitCh         chan byte
	resetHeartbeatCh chan byte
	applyCn          chan ApplyMsg

	peerIndexToSuccessIndex map[int]int
	batchCnt                int
}

func (rf *Raft) String() string {
	return fmt.Sprintf("id: %d, term: %d, snapshot: (%d,%d), commitIdx: %d, lastApplied:%d, log:(%d,%d:%d) ||| ", rf.me, rf.currentTerm, rf.snapshotIndex, rf.snapshotTerm, rf.commitIndex, rf.lastApplied, rf.firstLogIndex, rf.lastLogIndex, len(rf.log))
}

func (rf *Raft) isLeader() bool {
	return rf.role == LEADER
}

func (rf *Raft) IsLeader() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.role == LEADER
}

func (rf *Raft) beLeader() {
	rf.role = LEADER
}

func (rf *Raft) isCandidate() bool {
	return rf.role == CANDIDATE
}

func (rf *Raft) beCandidate() {
	rf.role = CANDIDATE
}

func (rf *Raft) isFollower() bool {
	return rf.role == FOLLOWER
}

func (rf *Raft) beFollower() {
	rf.role = FOLLOWER
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.isLeader()
	rf.mu.Unlock()
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.firstLogIndex)
	e.Encode(rf.lastLogIndex)
	e.Encode(rf.snapshotIndex)
	e.Encode(rf.snapshotTerm)
	rf.persister.SaveRaftState(w.Bytes())
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
	d.Decode(&rf.firstLogIndex)
	d.Decode(&rf.lastLogIndex)
	d.Decode(&rf.snapshotIndex)
	d.Decode(&rf.snapshotTerm)
	rf.lastApplied = rf.snapshotIndex
	DPrintf("me: %s, readPersist", rf.String())
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false

	rf.mu.Lock()
	term = rf.currentTerm
	isLeader = rf.isLeader()
	if isLeader {
		log := Log{Term: term, Command: command}
		rf.lastLogIndex++
		index = rf.lastLogIndex
		rf.log[index] = log

		rf.peerIndexToSuccessIndex[rf.me] = index
		rf.persist()
		go rf.heartbeat(-1)

		DPrintf("me: %s, start index: %d, command: %v", rf.String(), index, command)
	}
	rf.mu.Unlock()

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make(map[int]Log)
	rf.firstLogIndex = 1
	rf.lastLogIndex = 0
	rf.snapshotIndex = 0
	rf.commitIndex = 0
	rf.lastApplied = 0

	peerNum := len(peers)

	rf.nextIndex = make([]int, peerNum)
	rf.matchIndex = make([]int, peerNum)

	rf.beFollower()
	rf.resetCh = make(chan byte, 0)
	rf.commitCh = make(chan byte, 0)
	rf.resetHeartbeatCh = make(chan byte, 0)
	rf.applyCn = applyCh

	rf.batchCnt = 256

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.backgroundElect()
	go rf.backgroundHeartbeat()
	go rf.backgroundApply()
	go rf.backgroundTrimLog()

	return rf
}

func (rf *Raft) reset(term int) {
	rf.currentTerm = term
	rf.votedFor = -1
	rf.beFollower()
	rf.persist()
}
