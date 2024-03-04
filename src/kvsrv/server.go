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

type KVServer struct {
	mu sync.Mutex

	kvs     map[string]string
	records map[int64]string
	// Your definitions here.
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// check if duplicate rpc
	val, exist := kv.records[args.Id]
	if exist {
		reply.Value = val
		return
	}

	value, ok := kv.kvs[args.Key]
	if ok {
		reply.Value = value
	} else {
		reply.Value = ""
	}
	// store the value of rpc
	kv.records[args.Id] = value
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	val, exist := kv.records[args.Id]
	if exist {
		reply.Value = val
		return
	}

	kv.kvs[args.Key] = args.Value
	kv.records[args.Id] = args.Value
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	val, exist := kv.records[args.Id]
	if exist {
		reply.Value = val
		return
	}

	value, ok := kv.kvs[args.Key]
	if ok {
		reply.Value = value
	} else {
		reply.Value = ""
	}

	kv.records[args.Id] = value
	kv.kvs[args.Key] = value + args.Value
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.kvs = make(map[string]string)
	kv.records = make(map[int64]string)

	return kv
}
