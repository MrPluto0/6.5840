package kvraft

import (
	"bytes"
	"log"

	"6.5840/labgob"
)

func (kv *KVServer) decodeSnapshot(snapshot []byte) {
	if len(snapshot) == 0 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	if d.Decode(&kv.kvs) != nil || d.Decode(&kv.records) != nil {
		log.Fatal("snapshot decode error")
	}
}

func (kv *KVServer) encodeSnapshot(index int) {
	size := kv.persister.RaftStateSize()
	if kv.maxraftstate == -1 && size <= kv.maxraftstate-100 {
		return
	}
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.kvs)
	e.Encode(kv.records)
	kv.rf.Snapshot(index, w.Bytes())
}
