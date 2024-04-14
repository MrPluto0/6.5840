package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Operation struct {
	Seq   int64
	Value string
}

type KVServer struct {
	mu sync.Mutex

	kvs     map[string]string
	records map[int64]Operation
	// Your definitions here.
}

func (kv *KVServer) checkDuplicate(client, seq int64) (string, bool) {
	op, ok := kv.records[client]
	if ok && op.Seq == seq {
		return op.Value, true
	}
	return "", false
}

// Get operation is idempotent, so we don't need to store the result
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	value, ok := kv.kvs[args.Key]
	if ok {
		reply.Value = value
	} else {
		reply.Value = ""
	}

	delete(kv.records, args.Id)
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if value, ok := kv.checkDuplicate(args.Id, args.Seq); ok {
		reply.Value = value
		return
	}

	kv.kvs[args.Key] = args.Value
	reply.Value = ""

	kv.records[args.Id] = Operation{args.Seq, reply.Value}
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if value, ok := kv.checkDuplicate(args.Id, args.Seq); ok {
		reply.Value = value
		return
	}

	value, ok := kv.kvs[args.Key]
	if ok {
		reply.Value = value
	} else {
		reply.Value = ""
	}

	kv.kvs[args.Key] = value + args.Value
	kv.records[args.Id] = Operation{args.Seq, reply.Value}
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.kvs = make(map[string]string)
	kv.records = make(map[int64]Operation)

	return kv
}
