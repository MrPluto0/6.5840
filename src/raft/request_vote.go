package raft

import "sync/atomic"

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int // candidate’s term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry
	LastLogTerm  int // term of candidate’s last log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.updateTerm(args.Term)
	}

	reply.Term = args.Term

	// If votedFor is null or candidateId,
	// and candidate’s log is at least as up-to-date as receiver’s log, grant vote
	if rf.votedFor == Null || rf.votedFor == args.CandidateId {
		if (args.LastLogTerm > rf.getLastLogTerm()) ||
			(args.LastLogTerm == rf.getLastLogTerm() && args.LastLogIndex >= rf.getLastIndex()) {
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
			rf.resetElectionTime()
			rf.persist()
		}
	}
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock()
	DPrintf("[S%d T%d]RequestVote %v: %v S%d T%d", rf.me, rf.currentTerm, ok, reply.VoteGranted, server, reply.Term)
	rf.mu.Unlock()

	return ok
}

func (rf *Raft) startElection() {
	// update current status
	rf.updateState(Candidate)
	rf.resetElectionTime()

	// init the RequestVoteArgs and RequestVoteReply
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastIndex(),
		LastLogTerm:  rf.getLastLogTerm(),
	}

	// send RequestVote to each server
	voteCount := int32(1)
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			// rf.resetElectionTime()
			continue
		}
		go rf.handleRequestVote(i, &args, &voteCount)
	}
}

func (rf *Raft) handleRequestVote(server int, args *RequestVoteArgs, voteCount *int32) {
	var reply RequestVoteReply
	success := rf.sendRequestVote(server, args, &reply)
	if !success {
		return
	}

	// receive the rpc's response
	func() {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		// invalid rpc
		if rf.currentTerm != args.Term {
			return
		}

		if reply.Term > rf.currentTerm {
			rf.updateTerm(args.Term)
			return
		}
		if !reply.VoteGranted {
			return
		}
		count := atomic.AddInt32(voteCount, 1)
		if rf.state == Candidate && int(count) >= len(rf.peers)/2+1 {
			rf.updateState(Leader)
			rf.resetElectionTime()
		}
	}()
}
