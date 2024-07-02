package kvraft

import (
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const (
	GET    = "Get"
	PUT    = "Put"
	APPEND = "Append"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Id     int64
	Seq    int64
	Method string
	Key    string
	Value  string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	persister    *raft.Persister

	// Your definitions here.
	kvs      map[string]string
	records  map[int64]int64 // record sequence of clients
	sessions map[int]chan Op // store session of current server
}

// check if rpc is old
func (kv *KVServer) checkDuplicate(client, seq int64) bool {
	lastSeq, ok := kv.records[client]
	if ok && lastSeq >= seq {
		return true
	}
	return false
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	DPrintf("[Server %d] Get kvs[%v]\n", kv.me, args.Key)

	_, isLeader := kv.rf.GetState()

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	if ok := kv.checkDuplicate(args.Id, args.Seq); ok {
		reply.Err = OK
		reply.Value = kv.kvs[args.Key]
		return
	}

	op := Op{
		Method: GET,
		Key:    args.Key,
		Id:     args.Id,
		Seq:    args.Seq,
	}

	// use index to indicate rpc, but not <clientId+seqId>
	index, _, _ := kv.rf.Start(op)
	ch := make(chan Op)
	kv.sessions[index] = ch

	kv.mu.Unlock()
	select {
	case <-time.After(1000 * time.Millisecond):
		kv.mu.Lock()
		reply.Err = ErrTimeout
	case replyOp := <-ch:
		kv.mu.Lock()
		// receive the newer leader's message, whose log's index is same as the index of old rpc
		if op.Id == replyOp.Id && op.Seq == replyOp.Seq {
			reply.Err = OK
			reply.Value = kv.kvs[op.Key]
		} else {
			reply.Err = ErrWrongLeader
		}
	}

	close(kv.sessions[index])
	delete(kv.sessions, index)
	DPrintf("[Server %d] Get kvs[%v] %s\n", kv.me, args.Key, reply.Err)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	DPrintf("[Server %d] %v kvs[%v] %v \n", kv.me, args.Op, args.Key, args.Value)

	_, isLeader := kv.rf.GetState()

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	if ok := kv.checkDuplicate(args.Id, args.Seq); ok {
		reply.Err = OK
		return
	}

	op := Op{
		Method: args.Op,
		Key:    args.Key,
		Id:     args.Id,
		Seq:    args.Seq,
		Value:  args.Value,
	}

	index, _, _ := kv.rf.Start(op)
	ch := make(chan Op)
	kv.sessions[index] = ch

	kv.mu.Unlock()
	select {
	case <-time.After(1000 * time.Millisecond):
		kv.mu.Lock()
		reply.Err = ErrTimeout
	case replyOp := <-ch:
		kv.mu.Lock()
		if op.Id == replyOp.Id && op.Seq == replyOp.Seq {
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}
	}

	close(kv.sessions[index])
	delete(kv.sessions, index)
	DPrintf("[Server %d] %v kvs[%v] %v %v\n", kv.me, args.Op, args.Key, args.Value, reply.Err)
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) ticker() {
	for !kv.killed() {
		msg := <-kv.applyCh
		kv.mu.Lock()

		// snapshot or command
		if msg.SnapshotValid {
			DPrintf("[Server %d] Rev Command Msg: %v\n", kv.me, msg.SnapshotIndex)
			kv.decodeSnapshot(msg.Snapshot)
		} else if msg.CommandValid {
			// logs[0].Command is nil after snapshot
			if msg.Command == nil {
				continue
			}
			op := msg.Command.(Op)
			index := msg.CommandIndex
			DPrintf("[Server %d] Rev Command Msg: %v kvs[%v] %v %v %v\n", kv.me, op.Method, op.Key, op.Value, op.Id, op.Seq)

			// check twice, because the message need time to receive.
			if ok := kv.checkDuplicate(op.Id, op.Seq); !ok {
				kv.records[op.Id] = op.Seq
				switch op.Method {
				case PUT:
					kv.kvs[op.Key] = op.Value
				case APPEND:
					kv.kvs[op.Key] += op.Value
				}
			}

			// check if the rpc is in sessions.
			if _, ok := kv.sessions[index]; ok {
				kv.records[op.Id] = op.Seq
				kv.sessions[index] <- op
			}

			// check if need to snapshot
			kv.encodeSnapshot(msg.CommandIndex)
		}

		kv.mu.Unlock()
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	kv.persister = persister
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.kvs = make(map[string]string)
	kv.records = make(map[int64]int64)
	kv.sessions = make(map[int]chan Op)

	snapshot := kv.persister.ReadSnapshot()
	kv.decodeSnapshot(snapshot)

	go kv.ticker()

	return kv
}
