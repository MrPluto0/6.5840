package raft

type AppendEntriesArgs struct {
	Term         int        // leader’s term
	LeaderId     int        // so follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      LogEntries // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        // leader’s commitIndex
}

type AppendEntriesReply struct {
	Term         int  // currentTerm, for leader to update itself
	Success      bool // true if follower contained entry matching prevLogIndex and prevLogTerm
	PrevLogIndex int  // index of log entry preceding new ones actually to update Args.PrevLogIndex
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer func() {
		rf.mu.Unlock()
	}()

	reply.Success = false
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm {
		rf.updateTerm(args.Term)
	}

	reply.Term = args.Term
	rf.resetElectionTime()

	if len(args.Entries) > 0 {
		DPrintf("[S%d T%d]log replication %v %d", rf.me, rf.currentTerm, len(args.Entries), rf.logs.lastIndex())
	}

	// log replication
	if args.PrevLogIndex > rf.logs.lastIndex() ||
		(args.PrevLogIndex <= rf.logs.lastIndex() && rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm) {
		conflictIndex := min(args.PrevLogIndex, rf.logs.lastIndex())
		for i := conflictIndex - 1; i >= 0; i-- {
			if rf.logs[i].Term == args.PrevLogTerm || i == 0 {
				reply.PrevLogIndex = i
				break
			}
		}
		return
	}

	// If an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it.
	for i, entry := range args.Entries {
		logIndex := i + args.PrevLogIndex + 1
		// delete the existing entry and all that follow it
		if logIndex <= rf.logs.lastIndex() && entry.Term != rf.logs[logIndex].Term {
			rf.logs = rf.logs[:logIndex]
			rf.persist()
		}
		if logIndex > rf.logs.lastIndex() {
			rf.logs = append(rf.logs, args.Entries[i:]...)
			rf.persist()
			break
		}
	}

	if args.LeaderCommit > rf.commitIndex {
		// DPrintf("[S%d T%d]follower broadcast", rf.me, rf.currentTerm)
		rf.commitIndex = min(args.LeaderCommit, rf.logs.lastIndex())
		rf.applyCond.Broadcast()
	}

	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	rf.mu.Lock()
	DPrintf("[S%d T%d]AppendEntries %v: %v S%d T%d", rf.me, rf.currentTerm, ok, reply.Success, server, reply.Term)
	rf.mu.Unlock()

	return ok
}
