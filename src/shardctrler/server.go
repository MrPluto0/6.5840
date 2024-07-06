package shardctrler

import (
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	dead     int32           // set by Kill()
	records  map[int64]int64 // record sequence of clients
	sessions map[int]chan Op // store session of current server

	configs []Config // indexed by config num
}

type Op struct {
	// Your data here.
	Id     int64 // also include in args
	Seq    int64 // also include in args
	Method string
	Args   interface{}

	// JoinArgs
	Servers map[int][]string

	// LeaveArgs
	GIDs []int

	// MoveArgs
	Shard int
	GID   int

	// QueryArgs
	Num int

	// QueryReply
	Config Config
}

// check if rpc is old
func (sc *ShardCtrler) checkDuplicate(client, seq int64) bool {
	lastSeq, ok := sc.records[client]
	if ok && lastSeq >= seq {
		return true
	}
	return false
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	op := Op{
		Id:      args.Id,
		Seq:     args.Seq,
		Method:  JOIN,
		Servers: args.Servers,
	}
	err, _ := sc.handleOp(op)
	reply.Err = err
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	op := Op{
		Id:     args.Id,
		Seq:    args.Seq,
		Method: LEAVE,
		GIDs:   args.GIDs,
	}
	err, _ := sc.handleOp(op)
	reply.Err = err
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	op := Op{
		Id:     args.Id,
		Seq:    args.Seq,
		Method: MOVE,
		Shard:  args.Shard,
		GID:    args.GID,
	}
	err, _ := sc.handleOp(op)
	reply.Err = err
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	op := Op{
		Id:     args.Id,
		Seq:    args.Seq,
		Method: QUERY,
		Num:    args.Num,
	}
	err, config := sc.handleOp(op)
	reply.Err = err
	if err == OK {
		reply.Config = config.(Config)
	}
}

func (sc *ShardCtrler) handleOp(op Op) (Err, interface{}) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	if sc.killed() {
		return ErrServerKilled, nil
	}

	_, isLeader := sc.rf.GetState()

	if !isLeader {
		return ErrWrongLeader, nil
	}

	if ok := sc.checkDuplicate(op.Id, op.Seq); ok {
		return OK, nil
	}

	// use index to indicate rpc, but not <clientId+seqId>
	index, _, _ := sc.rf.Start(op)
	ch := make(chan Op)
	sc.sessions[index] = ch
	defer func() {
		close(sc.sessions[index])
		delete(sc.sessions, index)
	}()

	sc.mu.Unlock()
	select {
	case <-time.After(1000 * time.Millisecond):
		sc.mu.Lock()
		return ErrTimeout, nil
	case replyOp := <-ch:
		sc.mu.Lock()
		// receive the newer leader's message, whose log's index is same as the index of old rpc
		if op.Id == replyOp.Id && op.Seq == replyOp.Seq {
			return OK, replyOp.Config
		} else {
			return ErrWrongLeader, nil
		}
	}
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&sc.dead, 1)
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func (sc *ShardCtrler) ticker() {
	for !sc.killed() {
		msg := <-sc.applyCh
		sc.mu.Lock()

		if !msg.CommandValid || msg.Command == nil {
			continue
		}

		op := msg.Command.(Op)
		index := msg.CommandIndex
		DPrintf("[Server %d] Rev Command Msg: %v \n", sc.me, op.Method)

		// check twice, because the message need time to receive.
		if ok := sc.checkDuplicate(op.Id, op.Seq); !ok {
			sc.executeOp(&op)
			sc.records[op.Id] = op.Seq
		}

		// check if the rpc is in sessions.
		_, isLeader := sc.rf.GetState()
		if _, ok := sc.sessions[index]; ok && isLeader {
			sc.sessions[index] <- op
			sc.records[op.Id] = op.Seq
		}

		sc.mu.Unlock()
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.records = make(map[int64]int64)
	sc.sessions = make(map[int]chan Op)

	go sc.ticker()

	return sc
}
