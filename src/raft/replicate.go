package raft

import "time"

// Log is refer to replicate log
type Log struct {
	Term    int
	Command interface{}
}

//
// ApplyMsg as each Raft peer becomes aware that successive log entries are
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
	CommandTerm  int

	SnapshotData []byte
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

func (rf *Raft) getLogTerm(index int) int {
	if index == 0 || index < rf.snapshotIndex {
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

	index := rf.nextIndex[peerIdx]
	if index > rf.snapshotIndex {
		if index <= rf.lastLogIndex {
			for i := index; i <= rf.lastLogIndex && len(req.Entries) <= rf.batchCnt; i++ {
				req.Entries = append(req.Entries, rf.log[i])
			}
		}

		prevIndex := index - 1
		if prevIndex > 0 {
			req.PrevLogIndex = prevIndex
			req.PrevLogTerm = rf.getLogTerm(prevIndex)
		}
	}

	return req
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

			if rf.snapshotIndex >= rf.nextIndex[peerIdx] {
				go rf.sendSnapshot(peerIdx)
			} else if rf.lastLogIndex-rf.matchIndex[peerIdx] > rf.batchCnt {
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
			CommandTerm:  rf.getLogTerm(index),
		}

		if _, ok := rf.log[index]; ok {
			DPrintf("me: %s, apply msg: %v", rf.String(), applyMsg)
		} else {
			DPrintf("me: %s, apply empty msg, index: %d", rf.String(), index)
		}

		rf.applyCn <- applyMsg

		rf.lastApplied++
		rf.persist()
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

func (rf *Raft) trimLog() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	change := false
	for i := rf.firstLogIndex; i <= rf.snapshotIndex && i < rf.commitIndex; i++ {
		delete(rf.log, i)
		rf.firstLogIndex = i + 1
		change = true
		DPrintf("me: %s, delete log, index: %d", rf.String(), i)
	}
	if change {
		rf.persist()
		DPrintf("me: %s, Trim log, first log index: %d, snapshot index: %d", rf.String(), rf.firstLogIndex, rf.snapshotIndex)
	}
}

func (rf *Raft) backgroundTrimLog() {
	for {
		timeout := 5 * time.Second / 1000
		timer := time.NewTimer(timeout)
		select {
		case <-timer.C:
			rf.trimLog()
		}
	}
}
