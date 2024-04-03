package raft

type LogEntry struct {
	Term    int
	Command interface{}
}

type LogEntries []LogEntry

func (le LogEntries) getTerm(i int) int {
	if i < 0 {
		return -1
	} else {
		return le[i].Term
	}
}

func (le LogEntries) lastIndex() int {
	return len(le) - 1
}

func (le LogEntries) last() LogEntry {
	return le[le.lastIndex()]
}

func (le LogEntries) lastTerm() int {
	if le.lastIndex() > 0 {
		return le.last().Term
	} else {
		return -1
	}
}
