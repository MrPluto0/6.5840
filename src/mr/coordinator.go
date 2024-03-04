package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	MAP = iota + 1
	REDUCE
	END
)

const (
	READY = iota + 1
	RUNNING
	COMPLETE
)

const TIMEOUT = time.Second * 10

type CState int
type TState int

type Task struct {
	ID            int
	File          string
	Intermediates []string
	State         TState
	Type          int
	StartTime     time.Time
	NReduce       int
}

type Coordinator struct {
	Lock        sync.Mutex
	State       CState
	Files       []string
	MapTasks    []Task
	ReduceTasks []Task
	NMap        int
	NReduce     int
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) AllocTask(_ EmptyArgs, task *Task) error {
	c.Lock.Lock()
	defer c.Lock.Unlock()

	// search a task whose state is READY
	searchTask := func(tasks []Task) {
		for i, t := range tasks {
			if t.State == READY {
				t.State = RUNNING
				t.StartTime = time.Now()
				tasks[i] = t
				*task = t
				return
				// log.Printf("[%d] Alloc Task %d\n", task.Type, task.ID)
			}
		}
	}

	switch c.State {
	case MAP:
		searchTask(c.MapTasks)
	case REDUCE:
		searchTask(c.ReduceTasks)
	case END:
	}

	return nil
}

func (c *Coordinator) FinishTask(task Task, _ *EmptyArgs) error {
	c.Lock.Lock()
	defer c.Lock.Unlock()

	// check if tasks' state are COMPLETE
	checkTasksDone := func(tasks []Task) bool {
		tasksDone := true
		for _, t := range tasks {
			if t.State != COMPLETE {
				tasksDone = false
				break
			}
		}
		return tasksDone
	}

	task.State = COMPLETE

	switch task.Type {
	case MAP:
		c.MapTasks[task.ID] = task
		// transfer intermediates from mapTask to reduceTask
		for i := 0; i < c.NReduce; i++ {
			c.ReduceTasks[i].Intermediates[task.ID] = task.Intermediates[i]
		}
		if checkTasksDone(c.MapTasks) {
			c.State = REDUCE
		}
	case REDUCE:
		c.ReduceTasks[task.ID] = task
		if checkTasksDone(c.ReduceTasks) {
			c.State = END
		}
	case COMPLETE:
		log.Printf("[%d] Task %d has finished: ", task.Type, task.ID)
	}

	// log.Printf("[%d] Finish Task %d", task.Type, task.ID)

	return nil
}

// check if running task is timeout
func (c *Coordinator) checkTasksTimeout() {
	c.Lock.Lock()
	defer c.Lock.Unlock()

	var tasks []Task
	switch c.State {
	case MAP:
		tasks = c.MapTasks
	case REDUCE:
		tasks = c.ReduceTasks
	}

	for i, t := range tasks {
		now := time.Now()
		if t.State == RUNNING && now.After(t.StartTime.Add(TIMEOUT)) {
			tasks[i].State = READY
		}
	}
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (c *Coordinator) createMapTask(file string) {
	c.MapTasks = append(c.MapTasks, Task{
		ID:            len(c.MapTasks),
		File:          file,
		State:         READY,
		Type:          MAP,
		Intermediates: make([]string, c.NReduce),
		NReduce:       c.NReduce,
	})
}

func (c *Coordinator) createReduceTask() {
	c.ReduceTasks = append(c.ReduceTasks, Task{
		ID:            len(c.ReduceTasks),
		State:         READY,
		Type:          REDUCE,
		Intermediates: make([]string, c.NMap),
		NReduce:       c.NReduce,
	})
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.Lock.Lock()
	defer c.Lock.Unlock()
	return c.State == END
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.State = MAP
	c.NReduce = nReduce
	c.NMap = len(files)

	// Your code here.
	for i := 0; i < len(files); i++ {
		c.createMapTask(files[i])
	}

	for i := 0; i < nReduce; i++ {
		c.createReduceTask()
	}

	c.server()

	go func() {
		for !c.Done() {
			c.checkTasksTimeout()
			time.Sleep(time.Second * 2)
		}
	}()

	return &c
}
