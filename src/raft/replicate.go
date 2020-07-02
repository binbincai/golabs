package raft

import (
	"github.com/binbincai/golabs/src/lablog"
	"time"
)

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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index == 0 || index < rf.snapshotIndex {
		return 0
	}
	if index == rf.snapshotIndex {
		return rf.snapshotTerm
	}
	return rf.log[index].Term
}

func (rf *Raft) newAppendEntriesArgs(peerIdx int) *AppendEntriesArgs {
	req := &AppendEntriesArgs{}
	if !rf.isLeader() {
		return req
	}

	rf.mu.Lock()
	defer func() {
		rf.mu.Unlock()
	}()

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
			rf.mu.Unlock()
			req.PrevLogTerm = rf.getLogTerm(prevIndex)
			rf.mu.Lock()
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

		rf.logger.Printf(0, rf.me, "Raft.AppendEntries append logs from %d, cnt: %d",
			args.LeaderID, appendCnt)
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

		if reply.Term < rf.currentTerm {
			return
		}

		if reply.Term > rf.currentTerm {
			rf.mu.Unlock()
			rf.reset(reply.Term)
			rf.mu.Lock()
			reset = true
			return
		}

		if reply.Success {
			index := req.PrevLogIndex + len(req.Entries)
			rf.nextIndex[peerIdx] = max(rf.nextIndex[peerIdx], index+1)
			rf.matchIndex[peerIdx] = max(rf.matchIndex[peerIdx], index)
			rf.peerSuccessIndex[peerIdx] = max(rf.peerSuccessIndex[peerIdx], index)
			if len(req.Entries) > 0 {
				rf.logger.Printf(0, rf.me, "Raft.heartbeat, reply success from %d, success index: %d",
					peerIdx, index)
			}
			rf.mu.Unlock()
			if rf.forwardCommitIndex() {
				commit = true
				rf.logger.Printf(0, rf.me, "Raft.heartbeat, commit index update, commit index: %d", rf.commitIndex)
			}
			rf.mu.Lock()

			if rf.snapshotIndex >= rf.nextIndex[peerIdx] {
				go rf.sendSnapshot(peerIdx)
			} else if rf.lastLogIndex-rf.matchIndex[peerIdx] > rf.batchCnt {
				rf.logger.Printf(0, rf.me, "Raft.heartbeat concrete heartbeat to peer %d", peerIdx)
				go rf.heartbeat(peerIdx)
			}
		} else {
			rf.nextIndex[peerIdx] = max(rf.nextIndex[peerIdx]-rf.batchCnt, rf.matchIndex[peerIdx]+1)
		}
	}

	sendReq := func(peerIdx int) {
		req := rf.newAppendEntriesArgs(peerIdx)
		reply := &AppendEntriesReply{}
		success := rf.sendAppendEntries(peerIdx, req, reply)
		if success {
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
	for {
		t := time.NewTimer(10 * time.Millisecond)

		select {
		case <-t.C:
			go rf.apply()
		case <-rf.commitCh:
			t.Stop()
			go rf.apply()

			if rf.isLeader() {
				go rf.heartbeat(-1)
			}

			if rf.isLeader() {
				rf.resetHeartbeatCh <- struct{}{}
			}
		}
	}
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
			if rf.peerSuccessIndex[peerIdx] >= nextCommitIndex {
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
		t := time.NewTimer(50 * time.Millisecond)

		select {
		case <-rf.resetHeartbeatCh:
			t.Stop()
			if rf.IsLeader() {
				go rf.heartbeat(-1)
			}
		case <-t.C:
			if rf.IsLeader() {
				go rf.heartbeat(-1)
			}
		case <-rf.done:
			t.Stop()
			return
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
