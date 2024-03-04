package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type MapF func(string, string) []KeyValue
type ReduceF func(string, []string) string

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func writeLocalFile(name string, kva []KeyValue) string {
	f, err := os.CreateTemp("", "mr-tmp-*")
	if err != nil {
		log.Fatal("create intermediate file fail:", err)
	}
	defer f.Close()
	enc := json.NewEncoder(f)

	for _, kv := range kva {
		err = enc.Encode(&kv)
		if err != nil {
			log.Fatal("encode keyValue fail:", err)
		}
	}

	dir, _ := os.Getwd()
	absPath := filepath.Join(dir, name)
	os.Rename(f.Name(), absPath)

	return absPath
}

func readLocalFile(file string) []KeyValue {
	kva := []KeyValue{}
	f, err := os.Open(file)
	if err != nil {
		log.Fatalf("open file %s fail: %e\n", file, err)
	}
	dec := json.NewDecoder(f)
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		kva = append(kva, kv)
	}
	return kva
}

// main/mrworker.go calls this function.
func Worker(mapf MapF, reducef ReduceF) {

	// Your worker implementation here.
	for {
		task := CallAllocTask()
		if task == nil {
			break
		}
		if task.Type == 0 {
			continue
		}
		// log.Printf("[%d] task %d starts running.\n", task.Type, task.ID)

		switch task.Type {
		case MAP:
			runMapWorker(mapf, task)
		case REDUCE:
			runReduceWorker(reducef, task)
		}

		if ok := CallFinishTask(task); ok {
			// log.Printf("[%d] task %d finished.\n", task.Type, task.ID)
		} else {
			log.Fatal("worker call finish task fail\n")
		}
		time.Sleep(time.Second)
	}
}

func runMapWorker(mapf MapF, task *Task) {
	content, err := os.ReadFile(task.File)
	if err != nil {
		log.Fatal("read file fail:", err)
	}

	kva := mapf(task.File, string(content))
	buffer := make([][]KeyValue, task.NReduce)
	task.Intermediates = make([]string, task.NReduce)

	for _, kv := range kva {
		num := ihash(kv.Key) % task.NReduce
		buffer[num] = append(buffer[num], kv)
	}

	for i := 0; i < task.NReduce; i++ {
		name := fmt.Sprintf("mr-%d-%d", task.ID, i)
		absPath := writeLocalFile(name, buffer[i])
		task.Intermediates[i] = absPath
	}

	// 局部排序
	// sort.Sort(ByKey(kva))
}

func runReduceWorker(reducef ReduceF, task *Task) {
	files := task.Intermediates
	kva := []KeyValue{}

	for _, file := range files {
		kva2 := readLocalFile(file)
		kva = append(kva, kva2...)
	}

	sort.Sort(ByKey(kva))

	tmp, err := os.CreateTemp("", "mr-out-*")
	if err != nil {
		log.Fatal("create temp fail:", err)
	}

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		count := reducef(kva[i].Key, values)
		fmt.Fprintf(tmp, "%v %v\n", kva[i].Key, count)
		i = j
	}

	name := fmt.Sprintf("mr-out-%d", task.ID)
	dir, _ := os.Getwd()
	absPath := filepath.Join(dir, name)
	os.Rename(tmp.Name(), absPath)

	task.File = absPath
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallAllocTask() *Task {
	task := &Task{}
	ok := call("Coordinator.AllocTask", EmptyArgs{}, task)
	if ok {
		return task
	} else {
		return nil
	}
}

func CallFinishTask(task *Task) bool {
	// reply := EmptyArgs{}
	ok := call("Coordinator.FinishTask", *task, &EmptyArgs{})
	return ok
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Println(err.Error())
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	} else {
		log.Println(err.Error())
		return false
	}
}
