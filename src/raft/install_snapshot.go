package raft

import "time"

type InstallSnapshotArgs struct {
	Term              int    // leader’s term
	LeaderId          int    // so follower can redirect clients
	LastIncludedIndex int    // the snapshot replaces all entries up through and including this index
	LastIncludedTerm  int    // term of lastIncludedIndex offset byte offset where chunk is positioned in the snapshot file
	Data              []byte // raw bytes of the snapshot chunk, starting at offset
	// Offset         int    // byte offset where chunk is positioned in the snapshot file (Don't implement)
}

type InstallSnapshotReply struct {
	Term int // currentTerm, for leader to update itself
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		return
	}

	// DPrintf("[S%d T%d]Install snapshot %v %v", rf.me, rf.currentTerm, args.LastIncludedIndex, args.LastIncludedTerm)

	if args.LastIncludedIndex <= rf.getLastIndex() && rf.getLogTerm(args.LastIncludedIndex) == args.LastIncludedTerm {
		rf.logs = rf.getLogTail(args.LastIncludedIndex)
		rf.logs[0].Command = nil
	} else {
		rf.logs = make(LogEntries, 0)
		rf.logs = append(rf.logs, LogEntry{Term: args.LastIncludedTerm, Command: nil})
	}

	if args.LastIncludedIndex > rf.commitIndex {
		rf.commitIndex = args.LastIncludedIndex
		rf.lastApplied = args.LastIncludedIndex
	}

	rf.snapshot = args.Data
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.persist()

	// notice service layer to update logs
	msg := ApplyMsg{
		CommandValid:  false,
		SnapshotValid: true,
		Snapshot:      rf.snapshot,
		SnapshotTerm:  rf.lastIncludedTerm,
		SnapshotIndex: rf.lastIncludedIndex,
	}

	rf.mu.Unlock()
	rf.applyCh <- msg
	rf.mu.Lock()
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)

	rf.mu.Lock()
	DPrintf("[S%d T%d]InstallSnapshot %v: S%d T%d", rf.me, rf.currentTerm, ok, server, reply.Term)
	rf.mu.Unlock()

	return ok
}

func (rf *Raft) startInstallSnapshot(server int) {
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Data:              rf.snapshot,
	}
	DPrintf("[S%d T%d]Install snapshot %v %v", rf.me, rf.currentTerm, args.LastIncludedIndex, args.LastIncludedTerm)

	go rf.handleInstallSnapshot(server, &args)
}

func (rf *Raft) handleInstallSnapshot(server int, args *InstallSnapshotArgs) {
	reply := &InstallSnapshotReply{}
	ok := rf.sendInstallSnapshot(server, args, reply)
	if !ok {
		return
	}

	func() {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		// update ack time
		rf.lastAck[server] = time.Now()

		if reply.Term > rf.currentTerm {
			rf.updateTerm(reply.Term)
			return
		}

		rf.matchIndex[server] = rf.lastIncludedIndex
		rf.nextIndex[server] = rf.lastIncludedIndex + 1
	}()
}
