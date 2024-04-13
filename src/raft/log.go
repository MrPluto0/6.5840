package raft

type LogEntry struct {
	Term    int
	Command interface{}
}

type LogEntries []LogEntry

func (rf *Raft) getLog(virtualIdx int) LogEntry {
	realIdx := virtualIdx - rf.lastIncludedIndex
	return rf.logs[realIdx]
}

func (rf *Raft) getLastLog() LogEntry {
	return rf.logs[len(rf.logs)-1]
}

func (rf *Raft) getLogTerm(virtualIdx int) int {
	return rf.getLog(virtualIdx).Term
}

func (rf *Raft) getLastLogTerm() int {
	return rf.getLastLog().Term
}

func (rf *Raft) getLastIndex() int {
	return rf.lastIncludedIndex + len(rf.logs) - 1
}

func (rf *Raft) getLogHead(virtualIdx int) LogEntries {
	realIdx := virtualIdx - rf.lastIncludedIndex
	return rf.logs[:realIdx]
}

func (rf *Raft) getLogTail(virtualIdx int) LogEntries {
	realIdx := virtualIdx - rf.lastIncludedIndex
	return rf.logs[realIdx:]
}
