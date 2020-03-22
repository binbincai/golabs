package raft

import (
	"math/rand"
	"time"
)

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
		req.LastLogTerm = rf.getLogTerm(index)
	}

	DPrintf("me: %s, new vote, req: %v", rf.String(), req)
	rf.persist()
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
		term := rf.getLogTerm(index)
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

func (rf *Raft) backgroundElect() {
	for {
		timeout := time.Duration(rand.Intn(150)+150) * time.Second / 1000
		timer := time.NewTimer(timeout)

		select {
		case <-rf.resetCh:
			timer.Stop()
		case <-timer.C:
			rf.mu.Lock()
			if !rf.isLeader() {
				go rf.elect()
			}
			rf.mu.Unlock()
		}
	}
}
