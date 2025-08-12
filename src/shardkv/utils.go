package shardkv

import (
	"fmt"
	"log"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func (kv *ShardKV) sayMe() string {
	return fmt.Sprintf("[Group %d Server %d ConfigNum %d]", kv.gid, kv.me, kv.config.Num)
}

func (kv *ShardKV) PrintKvs() {
	fmt.Print("Print Shard Kvs: ")
	for _, m := range kv.shardKvs {
		if m == nil {
			fmt.Printf("map[nil] ")
		} else {
			fmt.Printf("%v ", m)
		}
	}
	fmt.Println("")
}

func cloneMap(m map[string]string) map[string]string {
	m2 := make(map[string]string)
	for k, v := range m {
		m2[k] = v
	}
	return m2
}
