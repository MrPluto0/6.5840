package shardkv

import (
	"bytes"
	"log"

	"6.5840/labgob"
)

func (kv *ShardKV) decodeSnapshot(snapshot []byte) {
	if len(snapshot) == 0 {
		DPrintf("[%s]decode snapshot null", kv.sayMe())
		return
	}

	DPrintf("decoding start kvs: %v", kv.shardKvs)

	temp := make([]map[string]string, len(kv.shardKvs))

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	if d.Decode(&temp) != nil ||
		d.Decode(&kv.shardCfgNum) != nil ||
		d.Decode(&kv.records) != nil ||
		d.Decode(&kv.lastConfig) != nil ||
		d.Decode(&kv.config) != nil {
		log.Fatal("snapshot decode error")
	}

	DPrintf("decoding end kvs: %v", temp)
	kv.shardKvs = temp
}

func (kv *ShardKV) encodeSnapshot(index int) {
	size := kv.persister.RaftStateSize()
	if kv.maxraftstate == -1 || size <= kv.maxraftstate-100 {
		return
	}
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.shardKvs)
	e.Encode(kv.shardCfgNum)
	e.Encode(kv.records)
	e.Encode(kv.lastConfig)
	e.Encode(kv.config)
	kv.rf.Snapshot(index, w.Bytes())
}
