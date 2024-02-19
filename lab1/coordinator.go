package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	WaitTaskType   = 1
	MapTaskType    = 2
	ReduceTaskType = 3
	ExitTaskType   = 4
)

const (
	ready      = 1
	inProgress = 2
	complete   = 3
)

type Coordinator struct {
	mu              sync.Mutex
	nReduce         int
	workers         []int
	mapTasks        []*task
	reduceTasks     []*task
	mapTasksDone    int
	reduceTasksDone int
}

// provide the worker with the number of reduce tasks and an id
func (c *Coordinator) RegisterHandler(args *RegisterRequest, reply *RegisterResponse) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	id := len(c.workers)
	c.workers = append(c.workers, -1)
	reply.AssignedId = id
	reply.NReduce = c.nReduce
	return nil
}

// assign a task to the worker
func (c *Coordinator) ReadyHandler(args *ReadyRequest, reply *ReadyReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	workerId := args.Id

	assignTask := func(taskType int, tasks []*task) {
		for taskNumber, task := range tasks {
			assumeCrash := (task.state == inProgress) && (time.Since(task.timeStarted) > 10*time.Second)
			if task.state == ready || assumeCrash {
				task.state = inProgress
				task.timeStarted = time.Now()
				c.workers[workerId] = taskNumber
				reply.TaskType = taskType
				reply.FileName = task.filename
				reply.TaskNumber = taskNumber
				return
			}
		}
		reply.TaskType = WaitTaskType
	}

	if c.mapTasksDone != len(c.mapTasks) {
		assignTask(MapTaskType, c.mapTasks)
		return nil
	} else if c.reduceTasksDone != len(c.reduceTasks) {
		assignTask(ReduceTaskType, c.reduceTasks)
		return nil
	}

	reply.TaskType = ExitTaskType
	return nil
}

func (c *Coordinator) DoneMapHandler(args *DoneMapRequest, reply *DoneMapReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	workerId := args.Id
	taskId := c.workers[workerId]
	c.workers[workerId] = -1

	if c.mapTasks[taskId].state == complete { // already done
		return nil
	}

	c.mapTasks[taskId].state = complete
	c.mapTasksDone++

	fmt.Printf("map task %d done\n", taskId)
	return nil
}

func (c *Coordinator) DoneReduceHandler(args *DoneReduceRequest, reply *DoneReduceReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	workerId := args.Id
	taskId := c.workers[workerId]
	c.workers[workerId] = -1

	if c.reduceTasks[taskId].state == complete { // already done
		return nil
	}

	c.reduceTasks[taskId].state = complete
	c.reduceTasksDone++

	fmt.Printf("reduce task %d done\n", taskId)
	return nil
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	ret := false

	// Your code here.
	ret = c.reduceTasksDone == len(c.reduceTasks)

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	mapTasks := make([]*task, len(files))
	for i, file := range files {
		mapTasks[i] = newTask(file)
	}

	reduceTasks := make([]*task, nReduce)
	for i := 0; i < nReduce; i++ {
		reduceTasks[i] = newTask("")
	}

	c := Coordinator{nReduce: nReduce, mapTasks: mapTasks, reduceTasks: reduceTasks, mu: sync.Mutex{}}

	c.server()
	return &c
}

type task struct {
	filename    string
	state       uint
	timeStarted time.Time
}

func newTask(filename string) *task {
	return &task{state: ready, filename: filename}
}
