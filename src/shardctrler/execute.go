package shardctrler

import "sort"

func (sc *ShardCtrler) executeOp(op *Op) {
	switch op.Method {
	case "Join":
		DPrintf("Join Servers %v\n", op.Servers)
		sc.executeJoin(op.Servers)
	case "Leave":
		DPrintf("Leave Servers %v\n", op.GIDs)
		sc.executeLeave(op.GIDs)
	case "Move":
		DPrintf("Move Shard[%v]=%v\n", op.Shard, op.GID)
		sc.executeMove(op.Shard, op.GID)
	case "Query":
		cfg := sc.executeQuery(op.Num)
		op.Config = cfg
	}
	c := sc.getConfig(-1)
	DPrintf("[Server %d] Num: %d, Shards %v\n", sc.me, c.Num, c.Shards)
}

func (sc *ShardCtrler) executeJoin(groups map[int][]string) {
	config := sc.newConfig()

	for gid, servers := range groups {
		config.Groups[gid] = servers
	}

	sc.configs = append(sc.configs, config)
	sc.balance()
}

func (sc *ShardCtrler) executeLeave(GIDs []int) {
	config := sc.newConfig()

	for _, gid := range GIDs {
		delete(config.Groups, gid)
	}

	sc.configs = append(sc.configs, config)
	sc.balance()
}

func (sc *ShardCtrler) executeMove(shard, GID int) {
	config := sc.newConfig()
	config.Shards[shard] = GID
	sc.configs = append(sc.configs, config)
}

func (sc *ShardCtrler) executeQuery(num int) Config {
	return sc.getConfig(num)
}

func (sc *ShardCtrler) newConfig() Config {
	oldCfg := sc.configs[len(sc.configs)-1]
	newCfg := Config{
		Num:    oldCfg.Num + 1,
		Shards: oldCfg.Shards,
		Groups: make(map[int][]string),
	}
	for gid, servers := range oldCfg.Groups {
		newCfg.Groups[gid] = append([]string{}, servers...)
	}
	return newCfg
}

func (sc *ShardCtrler) getConfig(num int) Config {
	if num == -1 || num >= len(sc.configs) {
		return sc.configs[len(sc.configs)-1]
	}
	return sc.configs[num]
}

// balance every group's num
func (sc *ShardCtrler) balance() {
	config := sc.getConfig(-1)

	if len(config.Groups) == 0 {
		config.Shards = [10]int{}
		sc.configs[len(sc.configs)-1] = config
		return
	}

	totalShard := len(config.Shards)
	totalGroup := len(config.Groups)
	avgGroup := totalShard / totalGroup
	restGroup := totalShard % totalGroup

	gids := make([]int, 0)
	shardCount := make(map[int]int)
	moveShard := make([]int, 0)

	// calculate the needed shard for gid
	for gid := range config.Groups {
		gids = append(gids, gid)
	}

	// sort is necessary to confirm the Shard'order isn't random
	sort.Ints(gids)

	for _, gid := range gids {
		shardCount[gid] = avgGroup
		if restGroup > 0 {
			shardCount[gid]++
			restGroup--
		}
	}

	// allocate this shard for gid
	for shard, gid := range config.Shards {
		if shardCount[gid] > 0 {
			shardCount[gid]--
		} else {
			moveShard = append(moveShard, shard)
		}
	}

	// find other gid to allocate this shard
	for _, shard := range moveShard {
		for _, gid := range gids {
			if shardCount[gid] > 0 {
				config.Shards[shard] = gid
				shardCount[gid]--
				break
			}
		}
	}

	sc.configs[len(sc.configs)-1] = config
}
