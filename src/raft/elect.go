package raft

import (
	"context"
	"github.com/binbincai/golabs/src/labrpc"
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
	PeerID      int
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
		req.LastLogTerm = rf.getLogTerm(index)
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

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) onElect() {
	rf.beLeader()

	rf.mu.Lock()
	rf.peerSuccessIndex = make(map[int]int)
	for peerIdx := range rf.peers {
		rf.nextIndex[peerIdx] = rf.lastLogIndex + 1
		rf.matchIndex[peerIdx] = 0
	}
	rf.mu.Unlock()

	select {
	case <-rf.done:
	case rf.triggerHeartbeatCh <- struct{}{}:
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
		// 选举超时, 如果超时没有成功, 且没有其他节点变成主节点, 触发下一轮选举.
		rf.mu.Lock()
		rf.currentTerm++
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
					case <-rf.done:
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
				rf.onElect()
				select {
				case <-rf.done:
				case rf.resetCh <- struct{}{}:
				}
				break OUT

			case <-t.C:
				// 选举超时.
				// 4. 无明确选举结果, 继续下一轮选举.
				// 分别对应论文里面的三种结果.
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
			if !rf.isLeader() {
				// 如果没有进行中的选主流程, 则启动新的选主流程.
				if ctx == nil {
					ctx, cancel = context.WithCancel(context.Background())
					go rf.elect(ctx)
				}
			}

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
