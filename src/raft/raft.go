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
	"context"
	"github.com/binbincai/golabs/src/labgob"
	"github.com/binbincai/golabs/src/lablog"
	"github.com/binbincai/golabs/src/labrpc"
	"go.uber.org/atomic"
	"math/rand"
	"sync"
	"time"
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

	role        RoleType
	resetCh     chan struct{}
	commitCh    chan struct{}
	heartbeatCh chan struct{}
	applyCn     chan ApplyMsg

	//peerSuccessIndex map[int]int
	batchCnt int

	rnd    *rand.Rand
	logger *lablog.Logger
	done   chan struct{}
}

//func (rf *Raft) String() string {
//	rf.mu.Lock()
//	defer rf.mu.Unlock()
//	return fmt.Sprintf("id: %d, term: %d, snapshot: (%d,%d), commitIdx: %d, lastApplied:%d, log:(%d,%d:%d) ||| ", rf.me, rf.currentTerm, rf.snapshotIndex, rf.snapshotTerm, rf.commitIndex, rf.lastApplied, rf.firstLogIndex, rf.lastLogIndex, len(rf.log))
//}

func (rf *Raft) isLeader() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.role == LEADER
}

func (rf *Raft) IsLeader() bool {
	return rf.isLeader()
}

func (rf *Raft) beLeader(ctx context.Context) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ctx.Err() == nil {
		rf.role = LEADER
	}
}

func (rf *Raft) isCandidate() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.role == CANDIDATE
}

func (rf *Raft) beCandidate() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.role = CANDIDATE
}

func (rf *Raft) isFollower() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.role == FOLLOWER
}

func (rf *Raft) beFollower() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.role = FOLLOWER
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	term := rf.currentTerm
	rf.mu.Unlock()
	return term, rf.isLeader()
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
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
	rf.logger.Printf(0, rf.me, "Raft.readPersist, index: %d, term: %d", rf.lastLogIndex, rf.currentTerm)
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
	if !rf.isLeader() {
		return -1, -1, false
	}

	rf.mu.Lock()
	rf.lastLogIndex++
	index := rf.lastLogIndex
	term := rf.currentTerm
	log := Log{Term: term, Command: command}
	rf.log[index] = log
	rf.matchIndex[rf.me] = index
	rf.mu.Unlock()

	rf.persist()
	select {
	case rf.heartbeatCh <- struct{}{}:
	case <-rf.done:
	}

	rf.logger.Printf(0, rf.me, "Start log, term: %d, index: %d, command: %v", term, index, command)
	return index, term, true
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	close(rf.done)
	rf.logger.Printf(0, rf.me, "Raft.Kill")
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
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
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
	rf.resetCh = make(chan struct{}, 0)
	rf.commitCh = make(chan struct{}, 0)
	rf.heartbeatCh = make(chan struct{}, 0)
	rf.applyCn = applyCh

	rf.batchCnt = 256

	rf.logger = lablog.New(true, "raft")
	rf.rnd = rand.New(rand.NewSource(int64(0xabcd + rf.me)))
	rf.done = make(chan struct{})

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.backgroundElect()
	go rf.backgroundHeartbeat()
	go rf.backgroundApply()
	go rf.backgroundTrimLog()

	rf.logger.Printf(0, rf.me, "Raft start")
	return rf
}

func (rf *Raft) reset(term int) {
	rf.mu.Lock()
	rf.currentTerm = term
	rf.votedFor = -1
	rf.mu.Unlock()

	rf.beFollower()
	rf.persist()
}

func (rf *Raft) newRequestVoteArgs() *RequestVoteArgs {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	req := &RequestVoteArgs{}
	req.Term = rf.currentTerm
	req.CandidateID = rf.me
	req.LastLogIndex = 0
	req.LastLogTerm = 0
	if rf.lastLogIndex > 0 {
		index := rf.lastLogIndex
		req.LastLogIndex = index
		rf.mu.Unlock()
		req.LastLogTerm = rf.getLogTerm(index)
		rf.mu.Lock()
	}

	return req
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
			select {
			case <-rf.done:
			case rf.resetCh <- struct{}{}:
			}
		}
	}()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.mu.Unlock()
		rf.reset(args.Term)
		rf.mu.Lock()
		reset = true
		reply.Term = rf.currentTerm
	}

	if rf.votedFor != -1 && args.CandidateID != rf.votedFor {
		return
	}

	if rf.lastLogIndex > 0 {
		index := rf.lastLogIndex
		rf.mu.Unlock()
		term := rf.getLogTerm(index)
		rf.mu.Lock()
		if args.LastLogTerm < term {
			return
		}
		if args.LastLogTerm == term && args.LastLogIndex < index {
			return
		}
	}

	rf.votedFor = args.CandidateID
	reply.VoteGranted = true
	reset = true

	rf.mu.Unlock()
	rf.persist()
	rf.mu.Lock()
}

func (rf *Raft) onElect(ctx context.Context) {
	rf.mu.Lock()
	for peerIdx := range rf.peers {
		rf.nextIndex[peerIdx] = rf.lastLogIndex + 1
		rf.matchIndex[peerIdx] = 0
	}
	rf.mu.Unlock()

	rf.beLeader(ctx)

	select {
	case <-ctx.Done():
	case rf.heartbeatCh <- struct{}{}:
	}
}

func (rf *Raft) sendRoteReq(ctx context.Context, peerID int, peer *labrpc.ClientEnd, replCh chan *RequestVoteReply) {
	args := rf.newRequestVoteArgs()
	repl := &RequestVoteReply{}

	done := make(chan bool)
	go func() {
		ok := peer.Call("Raft.RequestVote", args, repl)
		// 1. 防止卡主done<-ok.
		// 2. 保证正常关闭done.
		select {
		case <-ctx.Done():
		case done <- ok:
		}
		close(done)
	}()

	select {
	case <-ctx.Done():
		// 当前轮投票结束.
	case ok := <-done:
		// RPC请求返回.
		if ok {
			repl.PeerID = peerID
			// 防止卡主replCh<-repl.
			select {
			case <-ctx.Done():
			case replCh <- repl:
			}
		}
	}
}

func (rf *Raft) elect(ctx context.Context) {
	rf.logger.Printf(0, rf.me, "Begin elect")
	rf.beCandidate()

OUT:
	for {
		rf.mu.Lock()
		rf.currentTerm++
		rf.votedFor = -1
		currentTerm := rf.currentTerm

		// 默认给自己投一票.
		rf.votedFor = rf.me
		counter := newElectCounter(currentTerm, len(rf.peers))
		counter.approve(currentTerm, rf.me)

		// 配置选举定时器.
		off := time.Duration(rf.rnd.Int()%600) * time.Millisecond
		t := time.NewTimer(time.Second + off)

		// 表示这一轮选举.
		ctxElect, cancelElect := context.WithCancel(context.Background())

		rf.logger.Printf(0, rf.me, "Start new elect round, term: %d", currentTerm)
		// 发起投票请求.
		replCh := make(chan *RequestVoteReply)
		for i, peer := range rf.peers {
			if i == rf.me {
				continue
			}
			go rf.sendRoteReq(ctxElect, i, peer, replCh)
		}
		rf.mu.Unlock()

		rf.persist()

	IN:
		for {
			select {
			case repl := <-replCh:
				t.Stop()
				rf.logger.Printf(0, rf.me, "Receive vote repl from %d, term: %d, repl: %v", repl.PeerID, currentTerm, *repl)
				if repl.Term < currentTerm {
					break // 中断当前select, 而不是外侧的for循环.
				}

				if repl.Term > currentTerm {
					rf.logger.Printf(0, rf.me, "Stop elect, reason: detect higher term number, term: %d", currentTerm)
					// 1. 选举失败.
					select {
					case <-ctx.Done():
					case rf.resetCh <- struct{}{}:
					}
					cancelElect()
					rf.reset(repl.Term)
					break OUT
				}

				if !repl.VoteGranted {
					rf.logger.Printf(0, rf.me, "Vote rejected by peer %d, term: %d", repl.PeerID, currentTerm)
					break
				}

				rf.logger.Printf(0, rf.me, "Vote approved by peer %d, term: %d", repl.PeerID, currentTerm)
				if !counter.approve(repl.Term, repl.PeerID) {
					break
				}

				// 2. 选举成功.
				rf.logger.Printf(0, rf.me, "Stop elect, reason: success, term: %d", currentTerm)
				cancelElect()
				rf.onElect(ctx)
				select {
				case <-ctx.Done():
				case rf.resetCh <- struct{}{}:
				}
				break OUT

			case <-t.C:
				// 选举超时.
				// 4. 无明确选举结果, 继续下一轮选举.
				rf.logger.Printf(0, rf.me, "Elect timeout, term: %d", currentTerm)
				cancelElect()
				break IN

			case <-ctx.Done():
				// 选举被迫中断.
				rf.logger.Printf(0, rf.me, "Stop elect, reason: server cancel elect process, term: %d", currentTerm)
				cancelElect()
				t.Stop()
				break OUT
			}
		}
	}

	rf.logger.Printf(0, rf.me, "Exit elect")
}

func (rf *Raft) backgroundElect() {
	// 保证同一个时刻, 仅有一个有效的选举流程在进行.
	var ctx context.Context
	var cancel context.CancelFunc
	electing := atomic.NewBool(false)
	for {
		// 心跳超时, 如果超时没有收到心跳, 则升级为候选人, 发起新的一轮选举.
		rf.mu.Lock()
		off := time.Duration(rf.rnd.Int()%600) * time.Millisecond
		t := time.NewTimer(time.Second + off)
		rf.mu.Unlock()

		select {
		case <-rf.resetCh:
			t.Stop()
			// 下列情况会触发:
			// 1. 收到主节点的心跳/投票请求.
			// 2. 收到更高节点的心跳/投票请求.
			// 3. 当前节点选主成功.
			if ctx != nil {
				cancel()
				ctx = nil
				cancel = nil
			}

		case <-t.C:
			// 超时没有收到请求.
			if rf.isLeader() {
				break
			}
			if electing.Load() {
				break
			}
			if ctx != nil {
				cancel()
				ctx = nil
				cancel = nil
			}
			// 没有进行中的选主流程, 启动新的选主流程.
			electing.Store(true)
			ctx, cancel = context.WithCancel(context.Background())
			go func() {
				rf.elect(ctx)
				electing.Store(false)
			}()

		case <-rf.done:
			t.Stop()
			// 服务退出时, 需要终止正在进行中的选主流程.
			if ctx != nil {
				cancel()
				ctx = nil
				cancel = nil
			}
			return
		}
	}
}

func (rf *Raft) apply() {
	rf.mu.Lock()
	defer func() {
		rf.mu.Unlock()
	}()
	for rf.lastApplied < rf.commitIndex {
		index := rf.lastApplied + 1
		rf.mu.Unlock()
		term := rf.getLogTerm(index)
		rf.mu.Lock()
		msg := ApplyMsg{
			CommandValid: true,
			Command:      rf.log[index].Command,
			CommandIndex: index,
			CommandTerm:  term,
		}

		if _, ok := rf.log[index]; ok {
			rf.logger.Printf(0, rf.me, "Raft.apply apply msg, term: %d, index: %d, msg: %v",
				term, index, msg)
		} else {
			rf.logger.Printf(0, rf.me, "Raft.apply apply empty msg, term: %d, index: %d, msg: %v",
				term, index, msg)
			lablog.Assert(false)
		}

		rf.applyCn <- msg

		rf.lastApplied++
		rf.mu.Unlock()
		rf.persist()
		rf.mu.Lock()
	}
}

func (rf *Raft) backgroundApply() {
	applying := atomic.NewBool(false)
	for {
		t := time.NewTimer(10 * time.Millisecond)
		select {
		case <-t.C:
		case <-rf.commitCh:
		}
		// 保证同一个时刻只有一个goroutine在执行apply,
		// 防止死锁的时候, 产生大量的无用goroutine.
		if !applying.Load() {
			applying.Store(true)
			go func() {
				rf.apply()
				applying.Store(false)
			}()
		}
	}
}

func (rf *Raft) trim() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	change := false
	for i := rf.firstLogIndex; i <= rf.snapshotIndex && i < rf.commitIndex; i++ {
		delete(rf.log, i)
		rf.firstLogIndex = i + 1
		change = true
		rf.logger.Printf(0, rf.me, "Raft.trim delete log, index: %d", i)
	}
	if change {
		rf.mu.Unlock()
		rf.persist()
		rf.mu.Lock()
		rf.logger.Printf(0, rf.me, "Raft.trim trim log, first index: %d, snapshot index: %d",
			rf.firstLogIndex, rf.snapshotIndex)
	}
}

func (rf *Raft) backgroundTrimLog() {
	for {
		timer := time.NewTimer(5 * time.Millisecond)
		select {
		case <-timer.C:
			rf.trim()
		case <-rf.done:
			return
		}
	}
}

func (rf *Raft) getLogTerm(index int) int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	lablog.Assert(index >= rf.snapshotIndex)
	if index == 0 {
		return 0
	}
	if index == rf.snapshotIndex {
		return rf.snapshotTerm
	}
	return rf.log[index].Term
}

func (rf *Raft) newAppendEntriesArgs(peerIdx int) *AppendEntriesArgs {
	rf.mu.Lock()
	defer func() {
		rf.mu.Unlock()
	}()

	nextIndex := rf.nextIndex[peerIdx]
	// 对于落后过多的节点, 不发送正常的append entries请求.
	if nextIndex <= rf.snapshotIndex {
		return &AppendEntriesArgs{}
	}

	args := &AppendEntriesArgs{}
	args.Term = rf.currentTerm
	args.LeaderID = rf.me
	args.PrevLogIndex = 0
	args.PrevLogTerm = 0
	args.Entries = make([]Log, 0)
	args.LeaderCommit = rf.commitIndex

	if nextIndex <= rf.lastLogIndex {
		for i := nextIndex; i <= rf.lastLogIndex; i++ {
			args.Entries = append(args.Entries, rf.log[i])
			if len(args.Entries) >= rf.batchCnt {
				break
			}
		}
	}

	prevIndex := nextIndex - 1
	if prevIndex > 0 {
		args.PrevLogIndex = prevIndex
		rf.mu.Unlock()
		args.PrevLogTerm = rf.getLogTerm(prevIndex)
		rf.mu.Lock()
	}

	return args
}

// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	reset := false
	commit := false
	rf.mu.Lock()
	defer func() {
		rf.mu.Unlock()
		if reset {
			rf.resetCh <- struct{}{}
		}
		if commit {
			rf.commitCh <- struct{}{}
		}
	}()

	reply.Term = rf.currentTerm
	reply.Success = false
	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.mu.Unlock()
		rf.reset(args.Term)
		rf.mu.Lock()
		reply.Term = rf.currentTerm
		reset = true
	}

	if args.PrevLogIndex > 0 {
		if args.PrevLogIndex > rf.lastLogIndex {
			return
		}
		if args.PrevLogIndex < rf.snapshotIndex {
			return
		}
		if args.PrevLogIndex == rf.snapshotIndex {
			if args.PrevLogTerm != rf.snapshotTerm {
				return
			}
		} else {
			if args.PrevLogTerm != rf.log[args.PrevLogIndex].Term {
				return
			}
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

			for i := index; i <= rf.lastLogIndex; i++ {
				delete(rf.log, i)
			}

			rf.log[index] = log
			rf.lastLogIndex = index
			persist = true
			appendCnt++
		}

		if persist {
			rf.mu.Unlock()
			rf.persist()
			rf.mu.Lock()
		}

		rf.logger.Printf(0, rf.me, "Raft.AppendEntries append logs from %d, append cnt: %d", args.LeaderID, appendCnt)
	} else {
		//rf.logger.Printf(0, rf.me, "Raft.AppendEntries receive empty log entry, last log index: %d, %v", rf.lastLogIndex, args)
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

func (rf *Raft) forwardCommitIndex() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	commitIndex := rf.commitIndex
	nextCommitIndex := max(commitIndex, rf.lastApplied) + 1
	for nextCommitIndex <= rf.lastLogIndex {
		if rf.log[nextCommitIndex].Term != rf.currentTerm {
			nextCommitIndex++
			continue
		}
		successCnt := 0
		for peerIdx := range rf.peers {
			if rf.matchIndex[peerIdx] >= nextCommitIndex {
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

func (rf *Raft) heartbeatToPeer(ctx context.Context, peerIdx int, peer *labrpc.ClientEnd) {
	//rf.logger.Printf(0, rf.me, "Raft.heartbeatToPeer start, peer idx: %d", peerIdx)
	//defer rf.logger.Printf(0, rf.me, "Raft.heartbeatToPeer end, peer idx: %d", peerIdx)

	args := rf.newAppendEntriesArgs(peerIdx)
	repl := &AppendEntriesReply{}
	done := make(chan bool)
	go func() {
		ok := peer.Call("Raft.AppendEntries", args, repl)
		select {
		case done <- ok:
		case <-ctx.Done():
		case <-rf.done:
		}
	}()

	select {
	case <-ctx.Done():
		return
	case <-rf.done:
		return
	case ok := <-done:
		if !ok {
			return
		}
	}

	if !rf.isLeader() {
		return
	}

	reset := false
	commit := false
	rf.mu.Lock()
	defer func() {
		rf.mu.Unlock()
		if reset {
			rf.resetCh <- struct{}{}
		}
		if commit {
			rf.commitCh <- struct{}{}
		}
	}()

	if repl.Term < rf.currentTerm {
		return
	}

	if repl.Term > rf.currentTerm {
		rf.mu.Unlock()
		rf.reset(repl.Term)
		rf.mu.Lock()
		reset = true
		return
	}

	if !repl.Success {
		rf.nextIndex[peerIdx] = max(rf.nextIndex[peerIdx]-rf.batchCnt, rf.matchIndex[peerIdx]+1)
		go rf.heartbeatToPeer(context.TODO(), peerIdx, peer)
		rf.logger.Printf(0, rf.me, "Raft.heartbeat, trigger heartbeat to in consistent peer, peer index: %d", peerIdx)
		return
	}

	index := args.PrevLogIndex + len(args.Entries)
	rf.nextIndex[peerIdx] = max(rf.nextIndex[peerIdx], index+1)
	rf.matchIndex[peerIdx] = max(rf.matchIndex[peerIdx], index)
	if len(args.Entries) > 0 {
		rf.logger.Printf(0, rf.me, "Raft.heartbeat, repl success from %d, success index: %d", peerIdx, index)
	}

	rf.mu.Unlock()
	if rf.forwardCommitIndex() {
		commit = true
		rf.logger.Printf(0, rf.me, "Raft.heartbeat, commit index update, commit index: %d", rf.commitIndex)
	}
	rf.mu.Lock()

	if rf.lastLogIndex-rf.matchIndex[peerIdx] > rf.batchCnt {
		go rf.heartbeatToPeer(context.TODO(), peerIdx, peer)
		rf.logger.Printf(0, rf.me, "Raft.heartbeat, trigger heartbeat to lag peer, peer index: %d", peerIdx)
	}
}

func (rf *Raft) heartbeat(ctx context.Context) {
	//rf.logger.Printf(0, rf.me, "Raft.heartbeat start")
	//defer rf.logger.Printf(0, rf.me, "Raft.heartbeat end")
	wg := sync.WaitGroup{}
	wg.Add(len(rf.peers) - 1)

	rf.mu.Lock()
	for peerIdx, peer := range rf.peers {
		if peerIdx == rf.me {
			continue
		}
		go func(peerIdx int, peer *labrpc.ClientEnd) {
			rf.heartbeatToPeer(ctx, peerIdx, peer)
			wg.Done()
		}(peerIdx, peer)
	}
	rf.mu.Unlock()

	wg.Wait()
}

func (rf *Raft) backgroundHeartbeat() {
	var ctx context.Context
	var cancel context.CancelFunc
	inHeartbeat := atomic.NewBool(false)
	for {
		t := time.NewTimer(50 * time.Millisecond)

		select {
		case <-t.C:
		case <-rf.heartbeatCh:
			t.Stop()
		case <-rf.done:
			t.Stop()
			return
		}

		if rf.IsLeader() {
			if ctx != nil {
				cancel()
				ctx = nil
				cancel = nil
			}
			if !inHeartbeat.Load() {
				inHeartbeat.Store(true)
				ctx, cancel = context.WithCancel(context.Background())
				go func(ctx context.Context) {
					rf.heartbeat(ctx)
					inHeartbeat.Store(false)
				}(ctx)
			}
		}
	}
}
