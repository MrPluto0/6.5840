package shardkv

import (
	"time"
)

func (kv *ShardKV) configTicker() {
	for !kv.killed() {
		_, isLeader := kv.rf.GetState()
		if !isLeader {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		kv.mu.Lock()

		kv.PrintKvs()
		DPrintf("%v lastShard %v Shard %v", kv.sayMe(), kv.lastConfig.Shards, kv.config.Shards)

		// Migrate from GroupA's leader to GroupB's leader
		for shard, lastGid := range kv.lastConfig.Shards {
			gid := kv.config.Shards[shard]
			if lastGid == kv.gid && gid != kv.gid && kv.shardCfgNum[shard] < kv.config.Num {
				kv.startAddShard(shard, gid)
			}
		}

		// check if receive all shards
		if !kv.allReceived() {
			kv.mu.Unlock()
			time.Sleep(100 * time.Millisecond)
			continue
		}

		kv.mu.Unlock()

		config := kv.sm.Query(kv.config.Num + 1)

		if config.Num != kv.config.Num+1 {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		op := Op{
			Method: UpConfig,
			Config: config,
			IsCtl:  true,
			Id:     int64(kv.gid),
			Seq:    int64(kv.config.Num),
		}
		kv.handleOp(op)

		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) startAddShard(shard int, gid int) {
	args := AddShardArgs{
		Id:    int64(kv.gid),
		Seq:   int64(kv.config.Num),
		Shard: shard,
		Data:  cloneMap(kv.shardKvs[shard]),
	}

	DPrintf("%v Group %d add shard %d: %v, seq:%v", kv.sayMe(), gid, shard, args.Data, args.Seq)

	for {
		if servers, ok := kv.config.Groups[gid]; ok {
			// try each server for the shard.
			for si := 0; si < len(servers); si++ {
				srv := kv.make_end(servers[si])
				var reply AddShardReply
				ok := srv.Call("ShardKV.AddShard", &args, &reply)
				if ok && reply.Err == OK {
					kv.startDelShard(shard)
					return
				}
				if ok && (reply.Err == ErrWrongGroup) {
					break
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) startDelShard(shard int) {
	op := Op{
		Method: DelShard,
		Shard:  shard,
		IsCtl:  true,
		Id:     int64(kv.gid),
		Seq:    int64(kv.config.Num),
	}
	go kv.handleOp(op)
}

func (kv *ShardKV) allSent() bool {
	for shard, lastGid := range kv.lastConfig.Shards {
		gid := kv.config.Shards[shard]
		if lastGid == kv.gid && gid != kv.gid && kv.shardCfgNum[shard] < kv.config.Num {
			return false
		}
	}
	return true
}

func (kv *ShardKV) allReceived() bool {
	for shard, lastGid := range kv.lastConfig.Shards {
		gid := kv.config.Shards[shard]
		if gid == kv.gid && lastGid != kv.gid && kv.shardCfgNum[shard] < kv.config.Num {
			return false
		}
	}
	return true
}
