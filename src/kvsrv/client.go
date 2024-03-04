package kvsrv

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	server *labrpc.ClientEnd
	// You will have to modify this struct.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.server = server
	// You'll have to add code here.
	return ck
}

func (ck *Clerk) Call(svcMethod string, args interface{}, reply interface{}, timeout time.Duration) {
	for {
		done := make(chan bool)
		go func() {
			ok := ck.server.Call(svcMethod, args, reply)
			done <- ok
		}()

		select {
		case <-time.After(timeout):
			// log.Printf("[timeout] retry to send rpc for %v.\n", args)
		case ok := <-done:
			if ok {
				// log.Printf("[success] %s %v %v\n", svcMethod, args, reply)
				return
			} else {
				// log.Printf("[fail] retry to send rpc for %v.\n", args)
			}
		}
	}
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	var reply GetReply
	args := GetArgs{
		Id:  nrand(),
		Key: key,
	}
	ck.Call("KVServer.Get", &args, &reply, 3*time.Second)

	// You will have to modify this function.
	return reply.Value
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) string {
	// You will have to modify this function.
	var reply PutAppendReply
	args := PutAppendArgs{
		Id:    nrand(),
		Key:   key,
		Value: value,
	}

	ck.Call("KVServer."+op, &args, &reply, 3*time.Second)
	return reply.Value
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}
