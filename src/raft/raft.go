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
	"math/rand"
	"sync"
	"time"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// Log is refer to replicate log
type Log struct {
	Term    int
	Command interface{}
}

//
// RoleType is for raft object's role type
//
type RoleType int32

//
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
	return fmt.Sprintf("id: %d, term: %d, commitIdx: %d, lastApplied:%d, log:%d ||| ", rf.me, rf.currentTerm, rf.commitIndex, rf.lastApplied, len(rf.log))
}

func (rf *Raft) isLeader() bool {
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

// return currentTerm and whether this server
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
	rf.persister.SaveRaftState(w.Bytes())
}

func (rf *Raft) PersitWithSnapshot(snapshotIndex int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.snapshotIndex = snapshotIndex
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.firstLogIndex)
	e.Encode(rf.lastLogIndex)
	e.Encode(rf.snapshotIndex)
	rf.persister.SaveStateAndSnapshot(w.Bytes(), snapshot)
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
	DPrintf("me: %s, readPersist", rf.String())
}

//
// RequestVoteArgs is RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

//
// RequestVoteReply is RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	reset := false
	rf.mu.Lock()
	defer func() {
		rf.mu.Unlock()
		if reset {
			rf.resetCh <- '0'
		}
	}()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.reset(args.Term)
		reset = true
		reply.Term = rf.currentTerm
	}

	if rf.votedFor != -1 {
		if args.CandidateID != rf.votedFor {
			return
		}
	}

	if rf.lastLogIndex > 0 {
		index := rf.lastLogIndex
		log := rf.log[index]
		if args.LastLogTerm < log.Term {
			return
		}
		if args.LastLogTerm == log.Term && args.LastLogIndex < index {
			return
		}
	}

	rf.votedFor = args.CandidateID
	reply.VoteGranted = true
	reset = true

	rf.persist()
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// AppendEntriesArgs is AppendEntries's request struct
type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}

// AppendEntriesReply is AppendEntries's response struct
type AppendEntriesReply struct {
	Term    int
	Success bool
}

// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	reset := false
	commit := false
	rf.mu.Lock()
	defer func() {
		rf.mu.Unlock()
		if reset {
			rf.resetCh <- '0'
		}
		if commit {
			rf.commitCh <- '0'
		}
	}()

	reply.Term = rf.currentTerm
	reply.Success = false
	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.reset(args.Term)
		reply.Term = rf.currentTerm
		reset = true
	}

	if args.PrevLogIndex > 0 {
		if args.PrevLogIndex > rf.lastLogIndex {
			return
		}
		if args.PrevLogTerm != rf.log[args.PrevLogIndex].Term {
			return
		}
	}

	if len(args.Entries) > 0 {
		persist := false
		appendCnt := 0

		for i, log := range args.Entries {
			index := args.PrevLogIndex + i + 1
			if index <= rf.lastLogIndex && log.Term == rf.log[index].Term {
				continue
			}
			rf.log[index] = log
			rf.lastLogIndex = index
			if rf.lastLogIndex == 1 {
				rf.firstLogIndex = 1
			}
			persist = true
			appendCnt++
		}

		if persist {
			rf.persist()
		}

		DPrintf("me: %s, append logs, from: %d, cnt: %d", rf.String(), args.LeaderID, appendCnt)
	}

	index := args.PrevLogIndex + len(args.Entries)
	if args.LeaderCommit > rf.commitIndex && index > rf.commitIndex {
		rf.commitIndex = min(index, args.LeaderCommit)
		commit = true
	}

	reply.Term = rf.currentTerm
	reply.Success = true
	reset = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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
		if rf.lastLogIndex == 1 {
			rf.firstLogIndex = 1
		}
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
	rf.firstLogIndex = 0
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

func (rf *Raft) newRequestVoteArgs() *RequestVoteArgs {
	rf.mu.Lock()
	defer func() {
		rf.mu.Unlock()
	}()

	rf.currentTerm++
	rf.votedFor = rf.me
	rf.beCandidate()

	req := &RequestVoteArgs{}
	req.Term = rf.currentTerm
	req.CandidateID = rf.me
	req.LastLogIndex = 0
	req.LastLogTerm = 0
	if rf.lastLogIndex > 0 {
		index := rf.lastLogIndex
		req.LastLogIndex = index
		req.LastLogTerm = rf.log[index].Term
	}

	rf.persist()
	return req
}

func (rf *Raft) reset(term int) {
	rf.currentTerm = term
	rf.votedFor = -1
	rf.beFollower()
	rf.persist()
}

func (rf *Raft) onEelect() {
	rf.beLeader()

	rf.peerIndexToSuccessIndex = make(map[int]int)

	for peerIdx := range rf.peers {
		rf.nextIndex[peerIdx] = rf.lastLogIndex + 1
		rf.matchIndex[peerIdx] = 0
	}

	DPrintf("me: %s, become leader", rf.String())
	go rf.heartbeat(-1)
}

func (rf *Raft) elect() {
	req := rf.newRequestVoteArgs()

	votedTag := make(map[int]bool)
	voted := 1
	onReply := func(peerIdx int, reply *RequestVoteReply) {
		reset := false
		rf.mu.Lock()
		defer func() {
			rf.mu.Unlock()
			if reset {
				rf.resetCh <- '0'
			}
		}()

		if reply.Term < rf.currentTerm {
			return
		}

		if reply.Term > rf.currentTerm {
			rf.reset(reply.Term)
			reset = true
			return
		}

		if !rf.isCandidate() {
			return
		}

		if !reply.VoteGranted {
			return
		}

		if votedTag[peerIdx] {
			return
		}
		votedTag[peerIdx] = true

		DPrintf("me: %s, voted by: %d", rf.String(), peerIdx)

		voted++
		if voted > len(rf.peers)/2 {
			rf.onEelect()
			reset = true
		}
	}

	sendReq := func(peerIdx int) {
		reply := &RequestVoteReply{}
		succ := rf.sendRequestVote(peerIdx, req, reply)
		if !succ {
			return
		}
		onReply(peerIdx, reply)
	}

	rf.mu.Lock()
	for peerIdx := range rf.peers {
		if peerIdx == rf.me {
			continue
		}
		go sendReq(peerIdx)
	}
	rf.mu.Unlock()
}

func (rf *Raft) newAppendEntriesArgs(peerIdx int) *AppendEntriesArgs {
	rf.mu.Lock()
	defer func() {
		rf.mu.Unlock()
	}()

	req := &AppendEntriesArgs{}
	if !rf.isLeader() {
		return req
	}

	req.Term = rf.currentTerm
	req.LeaderID = rf.me
	req.PrevLogIndex = 0
	req.PrevLogTerm = 0
	req.Entries = make([]Log, 0)
	req.LeaderCommit = rf.commitIndex

	// TODO: index大于rf.firstLogIndex时, 需要使用snapshot
	index := rf.nextIndex[peerIdx]
	if index <= rf.lastLogIndex {
		for i := index; i <= rf.lastLogIndex && len(req.Entries) <= rf.batchCnt; i++ {
			req.Entries = append(req.Entries, rf.log[i])
		}
	}

	prevIndex := index - 1
	if prevIndex > 0 {
		req.PrevLogIndex = prevIndex
		req.PrevLogTerm = rf.log[prevIndex].Term
	}

	return req
}

func (rf *Raft) forwordCommitIndex() bool {
	commitIndex := rf.commitIndex
	nextCommitIndex := max(commitIndex, rf.lastApplied) + 1
	for nextCommitIndex <= rf.lastLogIndex {
		if rf.log[nextCommitIndex].Term != rf.currentTerm {
			nextCommitIndex++
			continue
		}
		successCnt := 0
		for peerIdx := range rf.peers {
			if rf.peerIndexToSuccessIndex[peerIdx] >= nextCommitIndex {
				successCnt++
			}
		}
		if successCnt <= len(rf.peers)/2 {
			break
		}
		rf.commitIndex = nextCommitIndex
		nextCommitIndex++
	}
	return commitIndex < rf.commitIndex
}

func (rf *Raft) heartbeat(concretePeerIdx int) {
	onReply := func(peerIdx int, req *AppendEntriesArgs, reply *AppendEntriesReply) {
		reset := false
		commit := false
		rf.mu.Lock()
		defer func() {
			rf.mu.Unlock()
			if reset {
				rf.resetCh <- '0'
			}
			if commit {
				rf.commitCh <- '0'
			}
		}()

		if reply.Term < rf.currentTerm {
			return
		}

		if reply.Term > rf.currentTerm {
			rf.reset(reply.Term)
			reset = true
			return
		}

		if !rf.isLeader() {
			return
		}

		if reply.Success {
			index := req.PrevLogIndex + len(req.Entries)
			rf.nextIndex[peerIdx] = max(rf.nextIndex[peerIdx], index+1)
			rf.matchIndex[peerIdx] = max(rf.matchIndex[peerIdx], index)
			rf.peerIndexToSuccessIndex[peerIdx] = max(rf.peerIndexToSuccessIndex[peerIdx], index)
			if len(req.Entries) > 0 {
				DPrintf("me: %s, reply success, from: %d, succ idx: %d", rf.String(), peerIdx, index)
			}
			if rf.forwordCommitIndex() {
				commit = true
				DPrintf("me: %s, commit index update, index: %d", rf.String(), rf.commitIndex)
			}

			// 加速日志的传递速度
			if rf.lastLogIndex-rf.matchIndex[peerIdx] > rf.batchCnt {
				DPrintf("me: %s, concrete heartbeat to peer: %d", rf.String(), peerIdx)
				go rf.heartbeat(peerIdx)
			}
		} else {
			rf.nextIndex[peerIdx] = max(rf.nextIndex[peerIdx]-rf.batchCnt, rf.matchIndex[peerIdx]+1)
		}
	}

	sendReq := func(peerIdx int) {
		req := rf.newAppendEntriesArgs(peerIdx)
		reply := &AppendEntriesReply{}
		succ := rf.sendAppendEntries(peerIdx, req, reply)
		if succ {
			onReply(peerIdx, req, reply)
		}
	}

	rf.mu.Lock()
	for peerIdx := range rf.peers {
		if peerIdx == rf.me {
			continue
		}
		if concretePeerIdx != -1 && concretePeerIdx != peerIdx {
			continue
		}
		go sendReq(peerIdx)
	}
	rf.mu.Unlock()
}

func (rf *Raft) apply() {
	rf.mu.Lock()
	defer func() {
		rf.mu.Unlock()
	}()
	for rf.lastApplied < rf.commitIndex {
		index := rf.lastApplied + 1
		applyMsg := ApplyMsg{
			CommandValid: true,
			Command:      rf.log[index].Command,
			CommandIndex: index,
		}

		rf.applyCn <- applyMsg

		rf.lastApplied++
		rf.persist()

		DPrintf("me: %s, apply msg: %v", rf.String(), applyMsg)
	}
}

func (rf *Raft) backgroundElect() {
	for {
		timeout := time.Duration(rand.Intn(150)+150) * time.Second / 1000
		timer := time.NewTimer(timeout)

		select {
		case <-rf.resetCh:
			timer.Stop()
			continue
		case <-timer.C:
			rf.mu.Lock()
			if !rf.isLeader() {
				go rf.elect()
			}
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) backgroundHeartbeat() {
	for {
		timeout := 50 * time.Second / 1000
		timer := time.NewTimer(timeout)

		select {
		case <-rf.resetHeartbeatCh:
			timer.Stop()
		case <-timer.C:
			rf.mu.Lock()
			if rf.isLeader() {
				go rf.heartbeat(-1)
			}
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) backgroundApply() {
	for {
		timeout := 10 * time.Second / 1000
		timer := time.NewTimer(timeout)

		select {
		case <-timer.C:
			go rf.apply()
		case <-rf.commitCh:
			timer.Stop()
			go rf.apply()

			rf.mu.Lock()
			isLeader := rf.isLeader()
			if isLeader {
				go rf.heartbeat(-1)
			}
			rf.mu.Unlock()

			if isLeader {
				rf.resetHeartbeatCh <- '0'
			}
		}
	}
}

func (rf *Raft) trimLog() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	change := false
	for i := 1; i <= rf.snapshotIndex && i < rf.commitIndex; i++ {
		delete(rf.log, i)
		rf.firstLogIndex = i + 1
		change = true
	}
	if change {
		rf.persist()
		DPrintf("Trim log, first log index: %d, snapshot index: %d", rf.firstLogIndex, rf.snapshotIndex)
	}
}

func (rf *Raft) backgroundTrimLog() {
	for {
		timeout := 10 * time.Second / 1000
		timer := time.NewTimer(timeout)
		select {
		case <-timer.C:
			rf.trimLog()
		}
	}
}
