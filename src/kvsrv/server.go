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

	kvs    map[string]string
	rpcIds map[int64]bool
	// Your definitions here.
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	_, exist := kv.rpcIds[args.Id]
	if exist {
		return
	} else {
		kv.rpcIds[args.Id] = true
	}

	value, ok := kv.kvs[args.Key]
	if ok {
		reply.Value = value
	} else {
		reply.Value = ""
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	_, exist := kv.rpcIds[args.Id]
	if exist {
		return
	} else {
		kv.rpcIds[args.Id] = true
	}

	kv.kvs[args.Key] = args.Value
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	_, exist := kv.rpcIds[args.Id]
	if exist {
		return
	} else {
		kv.rpcIds[args.Id] = true
	}

	value, ok := kv.kvs[args.Key]
	if ok {
		reply.Value = value
	} else {
		reply.Value = ""
	}

	kv.kvs[args.Key] = value + args.Value
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.kvs = make(map[string]string)
	kv.rpcIds = make(map[int64]bool)

	return kv
}
