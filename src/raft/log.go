package raft

type LogEntry struct {
	Index   int // start from 0
	Term    int // start from 0
	Command string
}
