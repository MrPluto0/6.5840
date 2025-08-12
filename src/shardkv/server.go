package shardkv

import (
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Id     int64
	Seq    int64
	Method string
	IsCtl  bool

	// Get/PutAppend
	Key   string
	Value string

	// AddShard/DelShard/UpConfig
	Shard  int
	Data   map[string]string
	Config shardctrler.Config
}

type Reply struct {
	Id  int64
	Seq int64
	// Value  string
	Err Err
}

type ShardKvs struct {
	kvs       map[string]string
	configNum int
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	sm         *shardctrler.Clerk
	persister  *raft.Persister
	config     shardctrler.Config
	lastConfig shardctrler.Config

	dead        int32               // set by Kill()
	shardKvs    []map[string]string // store shard of kvs by key
	shardCfgNum []int               // store shard of config num by key
	records     map[int64]int64     // record sequence of clients
	sessions    map[int]chan Reply  // store session of current server
}

// check if the rpc is duplicate
func (kv *ShardKV) checkDuplicate(op Op) bool {
	if op.IsCtl {
		return false
	}
	lastSeq, ok := kv.records[op.Id]
	if ok && lastSeq >= op.Seq {
		return true
	}
	return false
}

// check if the key is in the current config
func (kv *ShardKV) checkKeyShard(op Op) bool {
	if op.IsCtl {
		return true
	}
	shard := key2shard(op.Key)
	return kv.config.Shards[shard] == kv.gid
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		Method: GET,
		Key:    args.Key,
		Id:     args.Id,
		Seq:    args.Seq,
	}
	err := kv.handleOp(op)
	reply.Err = err
	if err == OK {
		kv.mu.Lock()
		shard := key2shard(op.Key)
		reply.Value = kv.shardKvs[shard][op.Key]
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		Method: args.Op,
		Key:    args.Key,
		Id:     args.Id,
		Seq:    args.Seq,
		Value:  args.Value,
	}
	err := kv.handleOp(op)
	reply.Err = err
}

func (kv *ShardKV) AddShard(args *AddShardArgs, reply *AddShardReply) {
	op := Op{
		Method: AddShard,
		Id:     args.Id,
		Seq:    args.Seq,
		Shard:  args.Shard,
		Data:   args.Data,
		IsCtl:  true,
	}

	err := kv.handleOp(op)
	reply.Err = err
}

func (kv *ShardKV) handleOp(op Op) Err {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.killed() {
		return ErrServerKilled
	}

	_, isLeader := kv.rf.GetState()

	if !isLeader {
		return ErrWrongLeader
	}

	if !kv.checkKeyShard(op) {
		return ErrWrongGroup
	}

	if kv.checkDuplicate(op) {
		return OK
	}

	// use index to indicate rpc, but not <clientId+seqId>
	index, _, _ := kv.rf.Start(op)
	ch := make(chan Reply)
	kv.sessions[index] = ch
	defer func() {
		close(kv.sessions[index])
		delete(kv.sessions, index)
	}()

	kv.mu.Unlock()
	select {
	case <-time.After(1000 * time.Millisecond):
		kv.mu.Lock()
		return ErrTimeout
	case reply := <-ch:
		kv.mu.Lock()
		// receive the newer leader's message, whose log's index is same as the index of old rpc
		if op.Id == reply.Id && op.Seq == reply.Seq {
			return reply.Err
		} else {
			return ErrWrongLeader
		}
	}
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *ShardKV) applyTicker() {
	for !kv.killed() {
		msg := <-kv.applyCh
		kv.mu.Lock()

		// snapshot or command
		if msg.SnapshotValid {
			kv.decodeSnapshot(msg.Snapshot)
			DPrintf("%v Rev Command %d: Snapshot %+v %v\n", kv.sayMe(), msg.SnapshotIndex, kv.shardKvs, kv.config.Shards)
		} else if msg.CommandValid {
			// logs[0].Command is nil after snapshot
			if msg.Command == nil {
				continue
			}
			op := msg.Command.(Op)
			index := msg.CommandIndex
			reply := Reply{op.Id, op.Seq, OK}

			// check twice, because the message need time to receive.
			if !kv.checkKeyShard(op) {
				reply.Err = ErrWrongGroup
			} else if !kv.checkDuplicate(op) {
				switch op.Method {
				case GET:
					reply.Err = kv.handleGetOp(op)
				case PUT:
					reply.Err = kv.handlePutOp(op)
				case APPEND:
					reply.Err = kv.handleAppendOp(op)
				case UpConfig:
					reply.Err = kv.handleUpConfigOp(op)
				case AddShard:
					reply.Err = kv.handleAddShardOp(op)
				case DelShard:
					reply.Err = kv.handleDelShardOp(op)
				}
				if reply.Err == OK {
					kv.records[op.Id] = op.Seq
				}
			}

			if op.Method == AddShard || op.Method == DelShard {
				kv.PrintKvs()
				DPrintf("%v Rev Command %d: %v shard %d reply:%v", kv.sayMe(), msg.CommandIndex, op.Method, op.Shard, reply.Err)
			} else if op.Method == UpConfig {
				kv.PrintKvs()
				DPrintf("%v Rev Command %d: %v config %v reply:%v", kv.sayMe(), msg.CommandIndex, op.Method, op.Config.Shards, reply.Err)
			} else {
				DPrintf("%v Rev Command %d: %v shardKvs[%v][%v] %v reply:%v", kv.sayMe(), msg.CommandIndex, op.Method, key2shard(op.Key), op.Key, op.Value, reply.Err)
			}

			// check if the rpc is in sessions.
			if _, ok := kv.sessions[index]; ok {
				// kv.records[op.Id] = op.Seq
				kv.sessions[index] <- reply
			}

			// check if need to snapshot
			kv.encodeSnapshot(msg.CommandIndex)
		}

		kv.mu.Unlock()
	}
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.persister = persister
	kv.sm = shardctrler.MakeClerk(kv.ctrlers)

	kv.shardKvs = make([]map[string]string, shardctrler.NShards)
	kv.shardCfgNum = make([]int, shardctrler.NShards)
	kv.records = make(map[int64]int64)
	kv.sessions = make(map[int]chan Reply)

	snapshot := kv.persister.ReadSnapshot()
	kv.decodeSnapshot(snapshot)

	go kv.applyTicker()

	go kv.configTicker()

	return kv
}
