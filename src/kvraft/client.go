package kvraft

import (
	"crypto/rand"
	"log"
	"math/big"
	"sync/atomic"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderId int
	id       int64        // client ID
	seq      atomic.Int64 // sequence ID
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.id = nrand()
	ck.leaderId = 0
	ck.seq.Store(0)

	return ck
}

func (ck *Clerk) Call(svcMethod string, args interface{}, reply interface{}) {
	ok := false
	i := ck.leaderId
	n := len(ck.servers)

	DPrintf("[Client]Call %v, Args%+v\n", svcMethod, args)

	for ; ; i = (i + 1) % n {
		// DPrintf("[Client]Server %d Call %v, Args%+v\n", i, svcMethod, args)
		ok = ck.servers[i].Call(svcMethod, args, reply)
		if !ok {
			continue
		}
		switch re := reply.(type) {
		case *GetReply:
			switch re.Err {
			case ErrWrongLeader:
				continue
			case ErrTimeout:
				i -= 1
				continue
			case OK:
				ck.leaderId = i
				return
			}
		case *PutAppendReply:
			switch re.Err {
			case ErrWrongLeader:
				continue
			case ErrTimeout:
				DPrintf("[Client]Call Timeout %v, Args%+v\n", svcMethod, args)
				i -= 1
				continue
			case OK:
				ck.leaderId = i
				return
			}
		default:
			log.Fatal("Unknown reply type")
		}
	}
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	var reply GetReply
	args := GetArgs{
		Id:  ck.id,
		Seq: ck.seq.Add(1),
		Key: key,
	}
	ck.Call("KVServer.Get", &args, &reply)
	if reply.Err == ErrNoKey {
		return ""
	}
	return reply.Value
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	var reply PutAppendReply
	args := PutAppendArgs{
		Id:    ck.id,
		Seq:   ck.seq.Add(1),
		Key:   key,
		Value: value,
		Op:    op,
	}

	ck.Call("KVServer.PutAppend", &args, &reply)
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
