package shardkv

func (kv *ShardKV) handleGetOp(op Op) Err {
	shard := key2shard(op.Key)
	kvs := kv.shardKvs[shard]
	if kvs == nil {
		return ErrShardNotArrive
	}
	return OK
}

func (kv *ShardKV) handlePutOp(op Op) Err {
	shard := key2shard(op.Key)
	kvs := kv.shardKvs[shard]
	if kvs == nil {
		return ErrShardNotArrive
	}
	kvs[op.Key] = op.Value
	return OK
}

func (kv *ShardKV) handleAppendOp(op Op) Err {
	shard := key2shard(op.Key)
	kvs := kv.shardKvs[shard]
	if kvs == nil {
		return ErrShardNotArrive
	}
	kvs[op.Key] += op.Value
	return OK
}

func (kv *ShardKV) handleUpConfigOp(op Op) Err {
	if op.Config.Num <= kv.config.Num {
		return ErrConfigNum
	}

	for shard, gid := range op.Config.Shards {
		if gid == kv.gid && kv.config.Shards[shard] == 0 {
			kv.shardKvs[shard] = make(map[string]string)
			kv.shardCfgNum[shard] = op.Config.Num
		}
	}

	kv.lastConfig = kv.config
	kv.config = op.Config

	return OK
}

func (kv *ShardKV) handleAddShardOp(op Op) Err {
	if op.Seq < int64(kv.config.Num) {
		return ErrConfigNum
	}

	if kv.shardKvs[op.Shard] != nil {
		return OK
	}

	kv.shardKvs[op.Shard] = cloneMap(op.Data)
	kv.shardCfgNum[op.Shard] = int(op.Seq)

	return OK
}

func (kv *ShardKV) handleDelShardOp(op Op) Err {
	if op.Seq < int64(kv.config.Num) {
		return ErrConfigNum
	}

	kv.shardKvs[op.Shard] = nil
	kv.shardCfgNum[op.Shard] = int(op.Seq)

	return OK
}
