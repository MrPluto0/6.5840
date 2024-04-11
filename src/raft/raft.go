package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type ServerState string

const (
	Null      = -1
	Leader    = "Leader"
	Candidate = "Candidate"
	Follower  = "Follower"
)

const (
	MaxELectionTime = 1000
	MinElectionTime = 700
	HeartbeatTime   = 100
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh   chan ApplyMsg       // message sent to tester or service
	applyCond *sync.Cond

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Custom state byself
	state        ServerState
	electTimeout time.Duration
	electTime    time.Time
	heartbeat    time.Duration

	// Persistent state on all servers
	currentTerm int
	votedFor    int // candidateId that received vote in current term
	logs        LogEntries

	// Volatile state on all servers
	commitIndex int // index of highest log entry known to be committed
	lastApplied int // index of highest log entry applied to state machine

	// Volatile state on leaders
	nextIndex  []int // for each server, index of the next log entry to send to that server
	matchIndex []int // for each server, index of highest log entry known to be replicated on server
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Your code here (3A).
	term := rf.currentTerm
	isleader := rf.state == Leader

	return term, isleader
}

func (rf *Raft) updateState(state ServerState) {
	rf.currentTerm++
	rf.state = state
	if state == Follower {
		rf.votedFor = Null
	} else {
		rf.votedFor = rf.me
	}
	rf.persist()

	// Reinitialized after election
	if state == Leader {
		DPrintf("[S%d T%d]New Leader", rf.me, rf.currentTerm)
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = rf.logs.lastIndex() + 1
			rf.matchIndex[i] = 0
		}
	}
}

func (rf *Raft) updateTerm(term int) {
	rf.currentTerm = term
	rf.state = Follower
	rf.votedFor = Null
	rf.persist()
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	// DPrintf("persist %d %v", rf.me, rf.logs)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var voteFor int
	var logs LogEntries
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&voteFor) != nil ||
		d.Decode(&logs) != nil {
		panic("error when decoding persist data")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = voteFor
		rf.logs = logs
		// DPrintf("read persist %d %v", rf.me, rf.logs)
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Your code here (3B).
	index := rf.logs.lastIndex() + 1
	term := rf.currentTerm
	isLeader := rf.state == Leader

	if !isLeader {
		return index, term, isLeader
	}

	log := LogEntry{
		Term:    term,
		Command: command,
	}

	rf.logs = append(rf.logs, log)
	rf.persist()

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) startElection() {
	// update current status
	rf.updateState(Candidate)
	rf.resetElectionTime()

	// init the RequestVoteArgs and RequestVoteReply
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.logs.lastIndex(),
		LastLogTerm:  rf.logs.lastTerm(),
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

// heartBeats with logEntry
func (rf *Raft) sendHeartBeats() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			rf.resetElectionTime()
			continue
		}
		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: rf.nextIndex[i] - 1,
			PrevLogTerm:  rf.logs.getTerm(rf.nextIndex[i] - 1),
			Entries:      nil,
			LeaderCommit: rf.commitIndex,
		}
		// cut a part of logs
		if rf.logs.lastIndex() >= rf.nextIndex[i] {
			args.Entries = rf.logs[rf.nextIndex[i]:]
			// DPrintf("[S%d T%d]nextIndex[%d]=%d lastIndex=%d", rf.me, rf.currentTerm, i, rf.nextIndex[i], rf.logs.lastIndex())
		}
		go rf.handleHeartBeats(i, &args)
	}
}

// handle call the rpc's response
func (rf *Raft) handleHeartBeats(server int, args *AppendEntriesArgs) {
	var reply AppendEntriesReply
	ok := rf.sendAppendEntries(server, args, &reply)
	if !ok {
		return
	}

	func() {
		rf.mu.Lock()
		defer rf.mu.Unlock()

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
			for N := len(rf.logs) - 1; N > rf.commitIndex; N-- {
				if rf.logs.getTerm(N) != rf.currentTerm {
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
			return
		}

		// If fail and old term, update to newer term
		if reply.Term > rf.currentTerm {
			rf.updateTerm(reply.Term)
			return
		}

		// If AppendEntries fails because of log inconsistency: decrement nextIndex and retry
		if reply.Term == rf.currentTerm && rf.state == Leader {
			rf.nextIndex[server] = reply.PrevLogIndex + 1
			return
		}
	}()
}

func (rf *Raft) resetElectionTime() {
	now := time.Now()
	rf.electTimeout = time.Duration(MinElectionTime + (rand.Int63() % (MaxELectionTime - MinElectionTime)))
	rf.electTime = now.Add(rf.electTimeout * time.Millisecond)
}

func (rf *Raft) applyTicker() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for !rf.killed() {
		if rf.commitIndex > rf.lastApplied {
			msg := ApplyMsg{
				CommandValid: true,
				CommandIndex: rf.lastApplied + 1,
				Command:      rf.logs[rf.lastApplied+1].Command,
			}
			DPrintf("[S%d T%d]apply log: %d %v", rf.me, rf.currentTerm, msg.CommandIndex, msg.Command)
			rf.mu.Unlock()
			rf.applyCh <- msg // may wait for applyCh to get msg, so use Unlock()
			rf.mu.Lock()
			rf.lastApplied++
		} else {
			// Invoke Wait() function until lastApplied == commitIndex
			// sleep util others call applyCond.Broadcast()
			rf.applyCond.Wait()
		}
	}
}

func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here (3A)
		// Check if a leader election should be started.

		// Extend the process to reduce extra loops
		// Sync the process to avoid extra judges of lock
		rf.mu.Lock()
		if time.Now().After(rf.electTime) {
			rf.startElection()
		}
		if rf.state == Leader {
			rf.sendHeartBeats()
		}
		rf.mu.Unlock()

		// pause for a random amount of time between 50 and 350 milliseconds.
		// heartbeat << electionTime
		time.Sleep(rf.heartbeat)
		// ms := 50 + +(rand.Int63() % 300)
		// time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)

	// Your initialization code here (3A, 3B, 3C).
	rf.state = Follower
	rf.heartbeat = time.Duration(HeartbeatTime) * time.Millisecond
	rf.resetElectionTime()

	rf.currentTerm = 0
	rf.votedFor = Null
	rf.logs = make([]LogEntry, 0)
	rf.logs = append(rf.logs, LogEntry{Term: 0, Command: nil})
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	for i := 0; i < len(peers); i++ {
		rf.nextIndex[i] = rf.logs.lastIndex() + 1
		rf.matchIndex[i] = 0
	}

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.applyTicker()

	return rf
}
