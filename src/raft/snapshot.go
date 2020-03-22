package raft

import (
	"bytes"
	"labgob"
)

// PersitWithSnapshot snapshot consist: 1. last applied index and term
// 2. cluster configuration
func (rf *Raft) PersitWithSnapshot(snapshot func() ([]byte, int, int)) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	data, index, term := snapshot()
	rf.snapshotIndex = index
	rf.snapshotTerm = term

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.firstLogIndex)
	e.Encode(rf.lastLogIndex)
	e.Encode(rf.snapshotIndex)
	e.Encode(rf.snapshotTerm)
	rf.persister.SaveStateAndSnapshot(w.Bytes(), data)
}

// InstallSnapshotArgs is InstallSnapshot RPC's request args
type InstallSnapshotArgs struct {
	Term              int
	LeaderID          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Offset            []int
	Data              []byte
	Done              bool
}

// InstallSnapshotReply is InstallSnapshot RPC's response args
type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) newInstallSnapshotArgs() *InstallSnapshotArgs {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	args := &InstallSnapshotArgs{}
	if !rf.isLeader() {
		return args
	}

	args.Term = rf.currentTerm
	args.LeaderID = rf.me
	args.LastIncludedIndex = rf.snapshotIndex
	args.LastIncludedTerm = rf.snapshotTerm
	args.Data = rf.persister.ReadSnapshot()
	args.Done = true
	return args
}

// InstallSnapshot RPC handler
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	reset := false
	rf.mu.Lock()
	defer func() {
		rf.mu.Unlock()
		if reset {
			rf.resetCh <- '0'
		}
	}()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.reset(args.Term)
		reset = true
		reply.Term = rf.currentTerm
	}

	snapshotIndex := args.LastIncludedIndex
	snapshotTerm := args.LastIncludedTerm
	if log, ok := rf.log[snapshotIndex]; ok {
		if log.Term == snapshotTerm {
			return
		}
	}

	if snapshotIndex <= rf.snapshotIndex {
		return
	}
	if snapshotIndex <= rf.commitIndex {
		return
	}

	for i := rf.firstLogIndex; i <= rf.lastLogIndex; i++ {
		delete(rf.log, i)
		DPrintf("me: %s, delete log(snapshot), index: %d", rf.String(), i)
	}
	rf.firstLogIndex = rf.lastLogIndex + 1
	rf.lastLogIndex = 0
	rf.persist()

	amsg := ApplyMsg{
		CommandValid: false,
		SnapshotData: args.Data,
	}
	rf.applyCn <- amsg
	rf.snapshotIndex = snapshotIndex
	rf.snapshotTerm = snapshotTerm
	rf.lastLogIndex = snapshotIndex
	rf.lastApplied = snapshotIndex
	rf.persist()
	DPrintf("me: %s, InstallSnapshot", rf.String())
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) sendSnapshot(peerIdx int) {
	args := rf.newInstallSnapshotArgs()
	reply := &InstallSnapshotReply{}
	ok := rf.sendInstallSnapshot(peerIdx, args, reply)
	if !ok {
		return
	}

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

	rf.nextIndex[peerIdx] = max(rf.nextIndex[peerIdx], args.LastIncludedIndex+1)
	rf.matchIndex[peerIdx] = max(rf.matchIndex[peerIdx], args.LastIncludedIndex)
}
