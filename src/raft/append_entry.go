package raft

import "time"

type AppendEntriesArgs struct {
	Term         int        // leader’s term
	LeaderId     int        // so follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      LogEntries // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        // leader’s commitIndex
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
	XTerm   int  // term in the conflicting entry (if any)
	XIndex  int  // index of first entry with that term (if any)
	XLen    int  // log length
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = false
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm {
		rf.updateTerm(args.Term)
	}

	reply.Term = args.Term
	reply.XLen = rf.getLastIndex() + 1
	reply.XIndex = args.PrevLogIndex
	reply.XTerm = args.PrevLogTerm
	rf.resetElectionTime()

	if len(args.Entries) > 0 {
		DPrintf("[S%d T%d]log replication %v %d", rf.me, rf.currentTerm, len(rf.logs), rf.getLastIndex())
	}

	// judge If conflict
	if args.PrevLogIndex > rf.getLastIndex() {
		return
	}

	if args.PrevLogIndex <= rf.getLastIndex() && rf.getLogTerm(args.PrevLogIndex) != args.PrevLogTerm {
		reply.XTerm = rf.getLogTerm(args.PrevLogIndex)
		reply.XIndex = args.PrevLogIndex
		for i := args.PrevLogIndex - 1; i >= rf.lastIncludedIndex; i-- {
			if rf.getLogTerm(i) != reply.Term {
				reply.XIndex = i + 1
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
		if logIndex <= rf.getLastIndex() && entry.Term != rf.getLogTerm(logIndex) {
			rf.logs = rf.getLogHead(logIndex)
			rf.persist()
		}
		if logIndex > rf.getLastIndex() {
			rf.logs = append(rf.logs, args.Entries[i:]...)
			rf.persist()
			break
		}
	}

	if args.LeaderCommit > rf.commitIndex {
		// DPrintf("[S%d T%d]follower broadcast", rf.me, rf.currentTerm)
		rf.commitIndex = min(args.LeaderCommit, rf.getLastIndex())
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

// heartBeats and append with logEntry
func (rf *Raft) startAppendEntries() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			// rf.resetElectionTime()
			continue
		}
		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: 0,
			PrevLogTerm:  0,
			Entries:      nil,
			LeaderCommit: rf.commitIndex,
		}
		if rf.nextIndex[i] > rf.lastIncludedIndex {
			args.PrevLogIndex = rf.nextIndex[i] - 1
			args.PrevLogTerm = rf.getLogTerm(rf.nextIndex[i] - 1)
			if rf.getLastIndex() >= rf.nextIndex[i] {
				args.Entries = rf.getLogTail(rf.nextIndex[i]) // truncate logs
			}
			go rf.handleAppendEntries(i, &args)
		} else {
			rf.startInstallSnapshot(i)
		}
	}
}

// handle call the rpc's response
func (rf *Raft) handleAppendEntries(server int, args *AppendEntriesArgs) {
	var reply AppendEntriesReply
	ok := rf.sendAppendEntries(server, args, &reply)
	if !ok {
		return
	}

	func() {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		// update ack time
		rf.lastAck[server] = time.Now()

		// invalid rpc
		if rf.currentTerm != args.Term {
			return
		}

		if reply.Success {
			// If successful: update nextIndex and matchIndex for follower
			rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
			rf.nextIndex[server] = rf.matchIndex[server] + 1
			// If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N,
			// and log[N].term == currentTerm, then set commitIndex = N
			for N := rf.getLastIndex(); N > rf.commitIndex; N-- {
				if rf.getLogTerm(N) != rf.currentTerm {
					continue
				}
				count := 1
				for i := 0; i < len(rf.peers); i++ {
					if i == rf.me {
						continue
					}
					if rf.matchIndex[i] >= N {
						count++
					}
					if count >= len(rf.peers)/2+1 {
						// DPrintf("[S%d T%d]leader broadcast", rf.me, rf.currentTerm)
						rf.commitIndex = N
						rf.applyCond.Broadcast()
						return
					}
				}
			}
		} else {
			// If fail and old term, update to newer term
			if reply.Term > rf.currentTerm {
				rf.updateTerm(reply.Term)
				return
			}

			if reply.Term != rf.currentTerm && rf.state != Leader {
				return
			}

			// If AppendEntries fails because of log inconsistency:
			// handle conflict, change nextIndex and retry

			// Case 1: follower's log is too short:
			if args.PrevLogIndex >= reply.XLen {
				rf.nextIndex[server] = reply.XLen
				return
			}

			// Case 2/3: leader has XTerm or not
			lastIndexForXTerm := -1
			for i := args.PrevLogIndex - 1; i >= rf.lastIncludedIndex; i-- {
				if rf.getLogTerm(i) == reply.XTerm {
					lastIndexForXTerm = i
					break
				}
				if rf.getLogTerm(i) < reply.XTerm {
					break
				}
			}
			if lastIndexForXTerm != -1 {
				rf.nextIndex[server] = lastIndexForXTerm
			} else {
				rf.nextIndex[server] = reply.XIndex
			}
		}
	}()
}

// judge how many active peer is still active
func (rf *Raft) quorumActive() bool {
	activeCount := 1
	for i, tracker := range rf.lastAck {
		if i != rf.me && time.Since(tracker) <= MaxElectionTimeout*time.Millisecond {
			activeCount++
		}
	}
	return activeCount >= len(rf.peers)/2+1
}
